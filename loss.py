import os
import json
import asyncio
import logging
import traceback
from decimal import Decimal
from dataclasses import dataclass, asdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
from aiohttp import ClientSession

load_dotenv()

TRANSLATE_API_KEY = os.environ["TRANSLATE_API_KEY"]

@dataclass
class Transfer:
    wallet: str
    chain: str
    block_number: int
    token_address: str
    token_symbol: str
    amount: Decimal
    timestamp: int
    usd_amount: Decimal | None
    counterpart: str
    tx_type: str
    action: str

class JSONEnc(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, Transfer):
            return asdict(o)
        return super().default(o)

async def start_txs_job(session: ClientSession, wallet: str, chain: str) -> str:
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
                token_address=tr["token"]["address"].lower(),
                token_symbol=tr["token"].get("symbol", ""),
                amount=side * Decimal(tr["amount"]),
                timestamp=timestamp,
                usd_amount=None,
                counterpart=(tr.get(counterside, {}).get("address") or "").lower(),
                tx_type=tx_type,
                action=tr["action"]
            )
        )
    return results

async def gather_job_results(session: ClientSession, url: str) -> list[dict]:
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

    logger.info(f"CACHE HIT FOR {wallet} ON {chain}")
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
        to_price.add((tr.chain, tr.token_address, tr.block_number))

    priced: dict[tuple[str, str, int], Decimal | None] = {}

    for chain, token_address, block in to_price.copy():
        cache_fpath = f"./cache/prices/{chain}/{token_address}/{block}"
        if os.path.exists(cache_fpath):
            logger.info(f"PRICE CACHE HIT FOR {chain} {token_address} {block}")
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
        tk_price = priced[(tr.chain, tr.token_address, tr.block_number)]
        if tk_price is None:
            tr.usd_amount = None
        else:
            tr.usd_amount = tk_price * tr.amount

async def portfolio_snapshot(transfers: list[Transfer]) -> dict[str, dict[str, Decimal]]:
    to_price: set[tuple[str, str]] = set()
    for tr in transfers:
        if not tr.usd_amount:
            continue
        to_price.add((tr.chain, tr.token_address))

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


    portfolio: dict[str, dict[str, Decimal]] = {}
    for tr in transfers:
        if priced.get((tr.chain, tr.token_address)) is None:
            # logger.warning(f"NO PRICE FOR {tr.chain} {tr.token_address}")
            continue
        if tr.chain not in portfolio:
            portfolio[tr.chain] = {}
        key = f"{tr.token_symbol.upper()} ({tr.token_address.lower()})"
        if key not in portfolio[tr.chain]:
            portfolio[tr.chain][key] = Decimal(0)
        portfolio[tr.chain][key] += tr.amount * priced[(tr.chain, tr.token_address)]
    return portfolio

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

    await price_transfers(transfers)

    portfolio = await portfolio_snapshot(transfers)
    total = Decimal(0)
    for chain, tk_usd_amounts in portfolio.items():
        for _, usd_amount in tk_usd_amounts.items():
            total += usd_amount

    print(f"TOTAL USD AMOUNT: {total}")

    print(json.dumps(portfolio, indent=4, cls=JSONEnc))

if __name__ == "__main__":
    asyncio.run(main())
