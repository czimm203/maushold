#!/usr/bin/env python

import aiohttp
import aiofiles
import asyncio
import logging
import sys

from pathlib import Path

logging.basicConfig(
    format="%(asctime)s %(levelname)s: %(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")

FAILED = []
MAX_RETRIES = 3
MAX_CONN = 1

async def fetch_data(url: str, client: aiohttp.ClientSession, sem: asyncio.Semaphore, attempts = 0) -> bytes | None:
    try:
        await sem.acquire()
        res = await client.request(method="GET", url=url)
        logger.info(res.status)
        logger.info("Fetching data from: %s", url)
        if res.status == 200:
            data = await res.content.read()
        else:
            logger.warn("Failed to fetch: %s", url)
            data = None
        return data
    # except:
    #     attempts += 1
    #     if attempts < MAX_RETRIES:
    #         logger.warn("Retrying :%s - %d", url, attempts)
    #         await fetch_data(url, client, sem, attempts)
    #     else:
    #         FAILED.append(url)
    #         logger.error("Exception raised: %s", url)
    finally:
        sem.release()
    

async def dump_data(path: Path, data: bytes):
    async with aiofiles.open(path, 'wb') as f:
        await f.write(data)
        logger.info("Wrote file: %s", path)

async def create_task(client: aiohttp.ClientSession, url: str, sem: asyncio.Semaphore, out_dir: Path) -> None:
    data = await fetch_data(url, client, sem)
    if data != None:
        filename = url.split('/')[-1]
        logger.info("Begin writing state file")
        await dump_data(out_dir / filename, data)

async def fetch_all():
    tasks = []
    data_dir = Path("./data")
    if not data_dir.exists():
        data_dir.mkdir()
        (data_dir / "blocks").mkdir()
        (data_dir / "block_groups").mkdir()
        (data_dir / "tracts").mkdir()

    async with aiohttp.ClientSession() as client:
        sem = asyncio.Semaphore(MAX_CONN)
        for i in range(79):
            url = f"https://www2.census.gov/geo/tiger/TIGER2022/TABBLOCK20/tl_2022_{i:02}_tabblock20.zip"
            tasks.append(create_task(client, url, sem, data_dir))

        for i in range(79):
            url = f"https://www2.census.gov/geo/tiger/TIGER2022/BG/tl_2022_{i:02}_bg.zip"
            tasks.append(create_task(client, url, sem, data_dir))

        for i in range(79):
            url = f"https://www2.census.gov/geo/tiger/TIGER2022/TRACT/tl_2022_{i:02}_tract.zip"
            tasks.append(create_task(client, url, sem, data_dir))
        
        tasks.append(create_task(client, "https://www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip", sem, data_dir))
        tasks.append(create_task(client, "https://www2.census.gov/geo/tiger/TIGER2022/STATE/tl_2022_us_state.zip", sem, data_dir))

        await asyncio.gather(*tasks)
        logger.info("All tasks finished")


if __name__ == '__main__':
    import requests

    asyncio.run(fetch_all())

    for url in FAILED:
        try:
            res = requests.get(url)
            if res.status_code == 200:
                with open(f"./data/{url.split('/')[-1]}", 'wb') as f:
                    f.write(res.content)
        except Exception as e:
            logger.error("ERROR: %s; %s", e, url)


    

