#!/usr/bin/env python

import aiohttp
import aiofiles
import asyncio
import logging
import sys

from pathlib import Path

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)
logger = logging.getLogger("areq")

async def fetch_data(url: str, client: aiohttp.ClientSession) -> bytes | None:
    res = await client.request(method="GET", url=url)
    logger.info("Fetching data from: %s", url)
    if res.status == 200:
        data = await res.content.read()
    else:
        logger.warn("Failed to fetch: %s", url)
        data = None
    return data

async def fetch_block_groups(num: int, client: aiohttp.ClientSession):
    return await fetch_data(f"https://www2.census.gov/geo/tiger/TIGER2022/BG/tl_2022_{num:02}_bg.zip", client)
    
async def fetch_tract(num: int, client: aiohttp.ClientSession):
    return await fetch_data(f"https://www2.census.gov/geo/tiger/TIGER2022/TRACT/tl_2022_{num:02}_tract.zip", client)
      
async def fetch_county(client: aiohttp.ClientSession):
    return await fetch_data(f"https://www2.census.gov/geo/tiger/TIGER2022/COUNTY/tl_2022_us_county.zip", client)

async def fetch_state(client: aiohttp.ClientSession):
    return await fetch_data(f"https://www2.census.gov/geo/tiger/TIGER2022/STATE/tl_2022_us_state.zip", client)

async def dump_data(path: Path, data: bytes):
    async with aiofiles.open(path, 'wb') as f:
        await f.write(data)
        logger.info("Wrote file: %s", path)

async def create_bg_task(num: int, client: aiohttp.ClientSession) -> None:
    data = await fetch_block_groups(num, client)
    if data != None:
        logger.info("Begin writing block group file: %d", num)
        await dump_data(Path(f"./data/block_groups/bg_{num:02}.zip"), data)

async def create_tract_task(num: int, client: aiohttp.ClientSession) -> None:
    data = await fetch_tract(num, client)
    if data != None:
        logger.info("Begin writing tract file: %d", num)
        await dump_data(Path(f"./data/tracts/tract_{num:02}.zip"), data)

async def create_county_task(client: aiohttp.ClientSession) -> None:
    data = await fetch_county(client)
    if data != None:
        logger.info("Begin writing county file")
        await dump_data(Path(f"./data/county.zip"), data)

async def create_state_task(client: aiohttp.ClientSession) -> None:
    data = await fetch_state(client)
    if data != None:
        logger.info("Begin writing state file")
        await dump_data(Path("./data/state.zip"), data)

async def fetch_all():
    tasks = []
    async with aiohttp.ClientSession() as client:
        for i in range(79):
            tasks.append(create_bg_task(i, client))

        for i in range(79):
            tasks.append(create_tract_task(i, client))
        
        tasks.append(create_state_task(client))
        tasks.append(create_county_task(client))

        await asyncio.gather(*tasks)
        logger.info("All tasks finished")

    


if __name__ == '__main__':
    asyncio.run(fetch_all())

