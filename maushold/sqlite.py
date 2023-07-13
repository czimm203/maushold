from __future__ import annotations
import json
import aiosqlite as sqlite3

from .db import DataBase
from .models import CensusCategory, DbRow, GeoJSON, PopQuery, GeoRefPopQuery
from contextlib import asynccontextmanager
from pathlib import Path
from shapely.geometry.base import shapely

async def get_connection() -> sqlite3.Connection:
    conn = await sqlite3.connect("file:./data/census.db?mode=ro&cache=shared&journal_mode=off&sync=off", uri=True)
    conn.row_factory = sqlite3.Row
    return conn

class Sqlite3Connector:
    def __init__(self, db_path: str | Path, **kwargs):
        self.path = db_path
        self.kwargs = kwargs
    
    @asynccontextmanager
    async def connection(self):
        try:
            conn = await sqlite3.connect(self.path, **self.kwargs)
            conn.row_factory = sqlite3.Row
            db = SqliteDB(conn)
            yield db
        finally:
            print("sqlite")
            await db.conn.close()

class SqliteDB(DataBase):
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    async def get_ids(self, cat: CensusCategory, limit: int = 10_000, offset: int = 0) -> list[str]:
        cur = await self.conn.cursor()
        await cur.execute(f"SELECT geo_id FROM {cat.to_table()} LIMIT ? OFFSET ?;", (limit, offset))
        res = await cur.fetchall()
        await cur.close()
        return [row["geo_id"] for row in res]

    async def get_row_data(self, cat: CensusCategory, id: str, offset:int = 0) -> list[DbRow]:
        cur = await self.conn.cursor()
        await cur.execute(f"SELECT * FROM {cat.to_table()} WHERE geo_id GLOB ? LIMIT 500 offset ?", (id, offset))
        res = await cur.fetchall()
        await cur.close()
        data = []
        for row in map(lambda x: dict(x), res):
            row['geometry'] = json.loads(row['geometry'])
            data.append(DbRow(**row)) #type: ignore
        return data

    async def get_pop_data(self, cat: CensusCategory, id: str) -> list[PopQuery]:
        cur = await self.conn.execute(f"SELECT geo_id, pop FROM {cat.to_table()} WHERE geo_id GLOB ?", (id,))
        res = await cur.fetchall()
        await cur.close()
        return [PopQuery.parse_obj(row) for row in res]

    async def get_row_by_polygon(self, cat: CensusCategory, geom: GeoJSON) -> list[GeoRefPopQuery]:
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
                index = 'v_blocks_clean'
                table = 'blocks_clean'
            case _:
                return []
        geometry: shapely.Polygon | shapely.MultiPolygon | shapely.GeometryCollection = geom.to_shapely()
        if geometry is None:
            return[GeoRefPopQuery(geo_id = "",pop = -1, lon = 0, lat = 0)]
        minX, minY, maxX, maxY = geometry.bounds #type: ignore

        match cat:
            case cat.block:
                cur = await self.conn.execute(f"""SELECT geo_id, pop, clon as lon, clat as lat
                                             FROM v_blocks_clean
                                             WHERE minX >= ? AND minY >= ? AND maxX <= ? AND maxY <= ? AND pop != 0""",
                                         (minX, minY, maxX, maxY))

            case _:
                cur = await self.conn.execute(f"""SELECT x.geo_id, x.pop, x.lon, x.lat FROM (
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
        query_res = await cur.fetchall()
        rows = [GeoRefPopQuery.parse_obj(row) for row in query_res]
        res = []
        for row in rows:
            if shapely.contains(geometry, shapely.Point(row.lon, row.lat)):
                res.append(row)
        return res
