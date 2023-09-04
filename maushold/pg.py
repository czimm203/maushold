from __future__ import annotations

from .db import DataBase
from .models import DbRow, PopQuery, CensusCategory, GeoRefPopQuery, GeoJSON
from contextlib import asynccontextmanager
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool
from psycopg.rows import class_row, dict_row
from psycopg.types import TypeInfo
from psycopg.types.shapely import register_shapely
from shapely.geometry import mapping



async def register_types(conn: AsyncConnection):
    info = await TypeInfo.fetch(conn, "geometry")
    if info is not None:
        register_shapely(info, conn)

class PgConnector:
    def __init__(self, *args, **kwargs) -> None:
        self.pool = AsyncConnectionPool(*args, **kwargs)

    @asynccontextmanager
    async def connection(self):
        try:
            conn = await self.pool.getconn()
            db = PgDB(conn)
            yield db
            await conn.commit()
        except:
            raise ValueError("oops")
        finally:
            if db is not None:
                await self.pool.putconn(db.conn)

        

class PgDB(DataBase):
    def __init__(self, connection: AsyncConnection):
        self.conn = connection

    async def get_ids(self, cat: CensusCategory, limit: int = 10_000, offset: int = 0) -> list[str]:
        cur = self.conn.cursor(row_factory=dict_row)
        table = cat.to_table()
        print(table)
        await cur.execute(f"""SELECT geo_id 
                        FROM {table}
                        ORDER BY geo_id ASC
                        LIMIT %s OFFSET %s;""", #type: ignore
                    (limit, offset))
        res = await cur.fetchall()
        await cur.close()
        return [row["geo_id"] for row in res]

    async def get_row_data(self, table: str, id: str, offset:int = 0) -> list[DbRow]:
        cur = self.conn.cursor()
        await cur.execute(f"SELECT * FROM {table} WHERE geo_id LIKE %s LIMIT 500 offset %s", (id.replace('*','%'), offset)) #type: ignore
        res = await cur.fetchall()
        await cur.close()
        data = []
        for row in map(lambda x: dict(x), res):
            row['geometry'] = mapping(row['geog'])
            data.append(DbRow(**row)) #type: ignore
        return data

    async def get_pop_data(self, cat: CensusCategory, id: str) -> list[PopQuery]:
        cur = self.conn.cursor(row_factory=class_row(PopQuery))
        await cur.execute(f"SELECT geo_id, pop FROM {cat.to_table()} WHERE geo_id LIKE %s", (id.replace('*', '%'),)) #type: ignore
        res = await cur.fetchall()
        await cur.close()
        return res

    async def get_row_by_geometry(self, cat: CensusCategory, geom: GeoJSON) -> list[GeoRefPopQuery]:
        await register_types(self.conn)
        cur = self.conn.cursor(row_factory=class_row(GeoRefPopQuery))
        geo = geom.to_shapely()
        if geo is None:
            return[GeoRefPopQuery(geo_id = "",pop = -1, lon = 0, lat = 0)]
        await cur.execute(f"""SELECT geo_id, pop,
                               clon AS lon,
                               clat AS lat
                        FROM {cat.to_table()}
                        WHERE ST_Intersects(geog, %s)""", #type: ignore
                    (geo,)) 
        res = await cur.fetchall()
        await cur.close()
        return res


