import os
import json
import asyncio
import logging
import traceback
from decimal import Decimal
from dataclasses import dataclass, is_dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from aiohttp import ClientSession

load_dotenv()

TRANSLATE_API_KEY = os.environ["TRANSLATE_API_KEY"]

@dataclass
class Flow:
    purchased_usd: Decimal
    sold_usd: Decimal
    holding_usd: Decimal
    holding: Decimal
    lost_usd: Decimal

_symbol_cache = {}
@dataclass
class Token:
    address: str
    symbol: str

    def __post_init__(self):
        self.address = self.address.lower()
        self.symbol = self.symbol.upper()

        if self.address in _symbol_cache:
            assert _symbol_cache[self.address] == self.symbol, f"Symbol mismatch for {self.address}: {_symbol_cache[self.address]} != {self.symbol}"
        else:
            _symbol_cache[self.address] = self.symbol

    def __str__(self):
        return f"{self.symbol} {self.address}"

    @staticmethod
    def get_symbol(address: str) -> str:
        address = address.lower()
        if address not in _symbol_cache:
            raise
        return _symbol_cache[address]

@dataclass
class Balance:
    amount: Decimal
    usd_amount: Decimal | None

@dataclass
class Transfer:
    wallet: str
    chain: str
    block_number: int
    token: Token
    balance: Balance
    timestamp: int
    counterpart: str
    tx_type: str
    action: str

class JSONEnc(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if is_dataclass(o):
            return asdict(o) # type: ignore
        return super().default(o)

async def start_txs_job(session: ClientSession, wallet: str, chain: str) -> str:
    logger.info(f"STARTING JOB FOR {wallet} ON {chain}")
    url = f"https://translate.noves.fi/evm/{chain}/txs/job/start"
    headers = {
        "accept": "application/json",
        "apiKey": TRANSLATE_API_KEY
    }
    params = {
        "accountAddress": wallet,
        "v5Format": "false",
        "excludeSpam": "true"
    }

    retries = 0
    while retries < 3:
        try:
            async with session.post(url, headers=headers, params=params) as response:
                if response.status == 202:
                    return (await response.json())["nextPageUrl"]
                logger.warning(f"FAILED TO START JOB FOR {wallet} ON {chain}: {response.status}")
        except:
            logger.warning(f"FAILED TO START JOB FOR {wallet} ON {chain}: {traceback.format_exc()}")

        await asyncio.sleep(1)
        retries += 1

    raise Exception(f"FAILED TO START JOB FOR {wallet} ON {chain} AFTER 3 RETRIES")

def parse_transfers(tx: dict) -> list[Transfer]:
    wallet = tx["accountAddress"].lower()
    chain = tx["chain"]
    block_number = tx["rawTransactionData"]["blockNumber"]
    timestamp = tx["rawTransactionData"]["timestamp"]
    tx_type = tx["classificationData"]["type"]

    sent = tx["classificationData"].get("sent", [])
    received = tx["classificationData"].get("received", [])

    results = []
    for i, tr in enumerate(sent + received):
        if not tr.get("token"):
            continue
        side = -1 if i < len(sent) else 1
        counterside = "to" if side == -1 else "from"
        results.append(
            Transfer(
                wallet=wallet,
                chain=chain,
                block_number=block_number,
                token=Token(tr["token"]["address"], tr["token"].get("symbol", "")),
                balance=Balance(amount=side * Decimal(tr["amount"]), usd_amount=None),
                timestamp=timestamp,
                counterpart=(tr.get(counterside, {}).get("address") or "").lower(),
                tx_type=tx_type,
                action=tr["action"]
            )
        )
    return results

async def gather_job_results(session: ClientSession, url: str) -> list[dict]:
    logger.info(f"GATHERING JOB RESULTS FOR {url}")
    headers = {
        "accept": "application/json",
        "apiKey": TRANSLATE_API_KEY
    }

    results: list[dict] = []
    retries = 0
    while url:
        try:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    resp_json = await response.json()
                    results.extend(resp_json["items"])
                    url = resp_json["nextPageUrl"]
                    retries = 0
                    continue
                if response.status == 425:
                    logger.warning(f"JOB IS STILL RUNNING FOR {url}: {response.status}")
                    await asyncio.sleep(5)
                    continue

                logger.warning(f"FAILED TO FETCH PAGE {url}: {response.status}")
        except:
            logger.warning(f"FAILED TO FETCH PAGE {url}: {traceback.format_exc()}")

        if retries >= 5:
            raise Exception(f"FAILED TO FETCH PAGE {url} AFTER {retries} RETRIES")
        await asyncio.sleep(1)
        retries += 1

    return results

def fetch_cached_transfers(wallet: str, chain: str) -> list[dict] | None:
    # TODO: cache by block range
    if not os.path.exists(f"./cache/{wallet}/{chain}.json"):
        return None

    with open(f"./cache/{wallet}/{chain}.json", "r") as f:
        return json.load(f)

async def fetch_transfers(wallet: str, chain: str) -> list[Transfer]:
    results: list[Transfer] = []

    if not (txs := fetch_cached_transfers(wallet, chain)):
        async with ClientSession() as session:
            url = await start_txs_job(session, wallet, chain)
            txs = await gather_job_results(session, url)

        os.makedirs(f"./cache/{wallet}", exist_ok=True)
        with open(f"./cache/{wallet}/{chain}.json", "w") as f:
            json.dump(txs, f, indent=4)

    for tx in txs:
        results.extend(parse_transfers(tx))

    return results

PRICING_SEMAPHORE = asyncio.Semaphore(20)
async def fetch_usd_price(session: ClientSession, chain: str, token_address: str, block_number: int | None) -> Decimal | None:
    logger.info(f"FETCHING PRICE FOR {chain} {token_address} {block_number}")
    url = f"https://pricing.noves.fi/evm/{chain}/price/{token_address}"
    headers = {"apiKey": TRANSLATE_API_KEY}
    params = {"blockNumber": block_number} if block_number is not None else {}

    async with PRICING_SEMAPHORE:
        retries = 0
        while url:
            try:
                async with session.get(url, params=params, headers=headers) as response:
                    if response.status == 200:
                        resp_json = await response.json()
                        price = resp_json["price"]["amount"]
                        if resp_json["price"]["amount"]:
                            return Decimal(price)
                        return None
                    if response.status == 429:
                        logger.warning(f"RESPONSE 429 FOR PRICE {url} {params}: {response.status}, SLEEPING")
                        await asyncio.sleep(5)
                        continue

                    logger.warning(f"FAILED TO FETCH PRICE {url} {params}: {response.status}, RETRYING")
            except:
                logger.warning(f"FAILED TO FETCH PRICE {url} {params}: {traceback.format_exc()}\nRETRYING")

            if retries >= 5:
                logger.error(f"FAILED TO FETCH PRICE {url} {params} AFTER {retries} RETRIES")
                return None

            await asyncio.sleep(1)
            retries += 1
"""
curl -X 'GET' \
  'https://api.geckoterminal.com/api/v2/simple/networks/eth/token_price/0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2%2C0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' \
  -H 'accept: application/json'

RESPONSE:
{
  "data": {
    "id": "95e5eae1-7e85-44e7-812c-a61536c87e40",
    "type": "simple_token_price",
    "attributes": {
      "token_prices": {
        "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48": "0.999453368879243",
        "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2": "2602.07"
      }
    }
  }
}
"""
GECKO_SEMAPHORE = asyncio.Semaphore(1)
async def fetch_current_prices(session: ClientSession, chain: str, token_addresses: set[str]) -> dict[str, Decimal | None]:
    results: dict[str, Decimal | None] = {
        addr: None for addr in token_addresses
    }
    async with GECKO_SEMAPHORE:
        addrs = list(token_addresses)
        while addrs: # max 30 addresses per request
            formatted_addrs = ",".join(addrs[:5])
            addrs = addrs[5:]
            print(addrs)
            url = f"https://api.geckoterminal.com/api/v2/simple/networks/{chain}/token_price/{formatted_addrs}"
            retries = 0
            while True:
                try:
                    async with session.get(url) as response:
                        if response.status == 200:
                            resp_json = await response.json()
                            for tk_addr, price in resp_json["data"]["attributes"]["token_prices"].items():
                                results[tk_addr.lower()] = Decimal(price)
                            break
                        if response.status == 429:
                            logger.warning(f"RESPONSE 429 FOR PRICE {url}: {response.status}, SLEEPING")
                            await asyncio.sleep(5)
                            continue
                        logger.warning(f"FAILED TO FETCH PRICE {url}: {response.status}, RETRYING")
                        retries += 1
                except:
                    retries += 1
                    logger.warning(f"FAILED TO FETCH PRICE {url}: {traceback.format_exc()}\nRETRYING")
                if retries >= 5:
                    logger.error(f"FAILED TO FETCH PRICE {url} AFTER {retries} RETRIES")
                    break # fail to price these tokens

        return results

async def price_transfers(transfers: list[Transfer]):
    to_price: set[tuple[str, str, int]] = set()
    for tr in transfers:
        to_price.add((tr.chain, tr.token.address, tr.block_number))

    priced: dict[tuple[str, str, int], Decimal | None] = {}

    for chain, token_address, block in to_price.copy():
        cache_fpath = f"./cache/prices/{chain}/{token_address}/{block}"
        if os.path.exists(cache_fpath):
            with open(cache_fpath) as f:
                content = f.read().strip()
                if not content:
                    priced[(chain, token_address, block)] = None
                else:
                    priced[(chain, token_address, block)] = Decimal(content)
            to_price.remove((chain, token_address, block))

    tasks = {}
    async with ClientSession() as session:
        async with asyncio.TaskGroup() as tg:
            for chain, token_address, block in to_price:
                tasks[(chain, token_address, block)] = tg.create_task(
                    fetch_usd_price(session, chain, token_address, block)
                )

    for (chain, token_address, block), task in tasks.items():
        price = task.result()
        cache_fpath = f"./cache/prices/{chain}/{token_address}/{block}"
        os.makedirs(os.path.dirname(cache_fpath), exist_ok=True)
        with open(cache_fpath, "w") as f:
            if price is None:
                f.write("")
            else:
                f.write(str(price))
        priced[(chain, token_address, block)] = price

    for tr in transfers:
        tk_price = priced[(tr.chain, tr.token.address, tr.block_number)]
        if tk_price is not None:
            tr.balance.usd_amount = tk_price * tr.balance.amount

async def portfolio_snapshot(transfers: list[Transfer]) -> dict[str, dict[str, Balance]]:
    net_balances: dict[str, dict[str, Balance]] = {}
    for tr in transfers:
        if tr.chain not in net_balances:
            net_balances[tr.chain] = {}
        if tr.token.address not in net_balances[tr.chain]:
            net_balances[tr.chain][tr.token.address] = Balance(Decimal(0), None)
        net_balances[tr.chain][tr.token.address].amount += tr.balance.amount

    to_price: set[tuple[str, str]] = set()
    for tr in transfers:
        if not tr.balance.usd_amount: # skip unpriceables
            continue
        to_price.add((tr.chain, tr.token.address))

    tasks = {}
    async with ClientSession() as session:
        async with asyncio.TaskGroup() as tg:
            for chain, token_address in to_price:
                tasks[chain, token_address] = tg.create_task(
                    fetch_usd_price(session, chain, token_address, None)
                )

    priced: dict[tuple[str, str], Decimal] = {}
    for (chain, token_address), task in tasks.items():
        price = task.result()
        if price is not None:
            priced[(chain, token_address)] = price

    # add usd amounts to net balances
    for (chain, token_address), price in priced.items():
        net_balances[chain][token_address].usd_amount = price * net_balances[chain][token_address].amount

    return net_balances

async def calculate_token_flows(transfers: list[Transfer]) -> dict[str, dict[str, Flow]]:
    flows: dict[str, dict[str, Flow]] = {}
    for tr in transfers:
        if tr.balance.usd_amount is None:
            continue
        if tr.chain not in flows:
            flows[tr.chain] = {}
        if str(tr.token) not in flows[tr.chain]:
            flows[tr.chain][str(tr.token)] = Flow(Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0))
        flows[tr.chain][str(tr.token)].holding += tr.balance.amount
        if tr.balance.usd_amount > 0:
            flows[tr.chain][str(tr.token)].purchased_usd += tr.balance.usd_amount
        else:
            flows[tr.chain][str(tr.token)].sold_usd -= tr.balance.usd_amount

    # price the current holdings
    tasks = {}
    async with ClientSession() as session:
        async with asyncio.TaskGroup() as tg:
            for chain, tk_flows in flows.items():
                for token_str in tk_flows:
                    token_address = token_str.split(" ")[-1]
                    tasks[chain, token_str] = tg.create_task(
                        fetch_usd_price(session, chain, token_address, None)
                    )

    for (chain, token_str), task in tasks.items():
        price = task.result()
        if price is None:
            continue
        flows[chain][token_str].holding_usd = flows[chain][token_str].holding * price

    # calculate the lost usd
    for tk_flows in flows.values():
        for flow in tk_flows.values():
            flow.lost_usd = flow.purchased_usd - flow.sold_usd - flow.holding_usd

    return flows

def prune_transfers(transfers: list[Transfer], wallets: set[str]) -> list[Transfer]:
    # remove transfers where the counterpart is one of the wallets in the list
    result: list[Transfer] = []
    must_find: set[tuple[str, str, int, str, Decimal]] = set()
    for tr in transfers:
        if not tr.balance.amount:
            logger.debug(f"TRANSFER WITH NO AMOUNT: {tr}")
            continue
        if tr.counterpart not in wallets:
            result.append(tr)
            continue

        sender = tr.wallet if tr.balance.amount < 0 else tr.counterpart
        transferred = tr.balance.amount if sender == tr.wallet else -tr.balance.amount
        key = (sender, tr.chain, tr.block_number, tr.token.address, transferred)
        if key not in must_find:
            must_find.add(key)
            continue
        must_find.remove(key)

    assert not must_find, f"UNMATCHED TRANSFERS: {must_find}"

    return result

def merge_chains(token_flows: dict[str, dict[str, Flow]]) -> dict[str, Flow]:
    # XXX: asumes tokens with the same symbol to be the same token
    merged_flows: dict[str, Flow] = {}
    for tk_flows in token_flows.values():
        for token_str, flow in tk_flows.items():
            symbol = token_str.split(" ")[0]
            if symbol not in merged_flows:
                merged_flows[symbol] = Flow(Decimal(0), Decimal(0), Decimal(0), Decimal(0), Decimal(0))
            merged_flows[symbol].holding += flow.holding
            merged_flows[symbol].purchased_usd += flow.purchased_usd
            merged_flows[symbol].sold_usd += flow.sold_usd
            merged_flows[symbol].holding_usd += flow.holding_usd
            merged_flows[symbol].lost_usd += flow.lost_usd

    return merged_flows

async def main():
    wallet_chains = {
        os.environ["WALLET1"].lower(): ["eth", "polygon", "optimism", "avalanche", "bsc"],
        os.environ["WALLET2"].lower(): ["eth", "polygon"],
        os.environ["WALLET3"].lower(): ["eth", "polygon", "optimism", "avalanche", "base"]
    }
    tasks = []
    async with asyncio.TaskGroup() as tg:
        for wallet, chains in wallet_chains.items():
            for chain in chains:
                tasks.append(tg.create_task(fetch_transfers(wallet, chain)))

    transfers: list[Transfer] = []
    for task in tasks:
        transfers.extend(task.result())

    transfers = prune_transfers(transfers, set(wallet_chains.keys()))

    await price_transfers(transfers)

    """
    unique_tokens = set()
    for tr in transfers:
        if tr.balance.usd_amount is None:
            continue
        unique_tokens.add(f"{tr.chain} {str(tr.token)}")

    sorted_tokens = sorted(unique_tokens)
    for ut in sorted_tokens:
        print(ut)
    """

    token_flows = merge_chains(await calculate_token_flows(transfers))

    print(json.dumps(token_flows, indent=4, cls=JSONEnc))

    total_loss_usd = Decimal(0)
    for flow in token_flows.values():
        total_loss_usd += flow.lost_usd

    print()
    print(f"TOTAL LOSS USD: {total_loss_usd}")

    # sort tokens by loss
    top_losers = sorted(token_flows.items(), key=lambda x: x[1].lost_usd, reverse=True)
    print()
    print("TOP LOSERS:")
    for symbol, flow in top_losers:
        print(f"{symbol}:\t{flow.lost_usd}")

    # calculate total usd holdings
    total_usd = Decimal(0)
    for flow in token_flows.values():
        if flow.holding_usd is not None:
            total_usd += flow.holding_usd

    print()
    print(f"TOTAL USD (available to lose): {total_usd}")

    # portfolio = await portfolio_snapshot(transfers)
    # total = Decimal(0)
    # for chain, tk_balances in portfolio.items():
    #     for balance in tk_balances.values():
    #         if balance.usd_amount is not None:
    #             total += balance.usd_amount

    # # pruned_portfolio with only non-zero balances
    # pruned_portfolio = {
    #     chain: {
    #         token_address: balance
    #         for token_address, balance in tk_balances.items()
    #         if balance.amount > 0
    #     }
    #     for chain, tk_balances in portfolio.items()
    # }
    # print(json.dumps(pruned_portfolio, indent=4, cls=JSONEnc))
    # print(f"TOTAL: {total}")

if __name__ == "__main__":
    asyncio.run(main())
