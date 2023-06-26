import json
import aiosqlite as sqlite3

from .models import CensusCategory, DbRow, PopQuery, GeoRefPopQuery


async def get_connection() -> sqlite3.Connection:
    conn = await sqlite3.connect("./data/census.db")
    conn.row_factory = sqlite3.Row
    return conn

async def get_ids(table: str, limit: int = 1_000_000, offset: int = 0) -> list[str]:
    conn = await get_connection()
    cur = await conn.cursor()
    await cur.execute(f"SELECT geo_id FROM {table} LIMIT ? OFFSET ?;", (limit, offset))
    res = await cur.fetchall()
    await cur.close()
    await cur.close()
    return [row["geo_id"] for row in res]

async def get_row_data(table: str, id: str, offset:int = 0) -> list[DbRow]:
    conn = await get_connection()
    cur = await conn.cursor()
    await cur.execute(f"SELECT * FROM {table} WHERE geo_id GLOB ? LIMIT 500 offset ?", (id, offset))
    res = await cur.fetchall()
    await cur.close()
    await conn.close()
    data = []
    for row in map(lambda x: dict(x), res):
        row['geometry'] = json.loads(row['geometry'])
        data.append(DbRow(**row)) #type: ignore
    return data

async def get_pop_data(table: str, id: str) -> list[PopQuery]:
    conn = await get_connection()
    # cur = await conn.cursor()
    cur = await conn.execute(f"SELECT geo_id, pop FROM {table} WHERE geo_id GLOB ?", (id,))
    res = await cur.fetchall()
    await cur.close()
    await conn.close()
    return [PopQuery.parse_obj(row) for row in res]

async def get_row_by_bbox(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float) -> list[GeoRefPopQuery]:
    match cat:
        case cat.state:
            index = 'v_states'
            table = 'states'
        case cat.county:
            index = 'v_counties'
            table = 'counties'
        case cat.tract:
            index = 'v_tracts'
            table = 'tracts'
        case cat.block_group:
            index = 'v_block_groups'
            table = 'block_groups'
        case cat.block:
            index = 'v_blocks'
            table = 'blocks'
        case _:
            return []
    conn = await get_connection()
    # cur = await conn.cursor()
    cur = await conn.execute(f"""SELECT x.geo_id, x.pop, x.lon, x.lat FROM (
                        SELECT {table}.geo_id AS geo_id,
                               {table}.pop AS pop,
                               {table}.clon AS lon,
                               {table}.clat AS lat,
                               {index}.minX AS minX,
                               {index}.minY AS minY,
                               {index}.maxX AS maxX,
                               {index}.maxY AS maxY
                        FROM {table}
                        INNER JOIN {index}
                        ON {table}.geo_id = {index}.geo_id
                    ) AS x
                WHERE x.minX >= ? AND x.minY >= ? AND x.maxX <= ? AND x.maxY <= ? AND x.pop != 0""", (minX, minY, maxX, maxY))
    res = await cur.fetchall()
    return [GeoRefPopQuery.parse_obj(row) for row in res]
