import json
import psycopg as pg
import shapely

from .models import DbRow, PopQuery, CensusCategory, GeoRefPopQuery
from psycopg import AsyncConnection, Connection
from psycopg.rows import class_row, dict_row
from psycopg.types import TypeInfo
from psycopg.types.shapely import register_shapely
from shapely.geometry import mapping

def get_connection(path: str) -> pg.connection.Connection:
    conn = pg.connect(path, row_factory=dict_row)
    return conn

async def register_types(conn: AsyncConnection):
    info = await TypeInfo.fetch(conn, "geometry")
    if info is not None:
        register_shapely(info, conn)

async def get_ids(conn: AsyncConnection, cat: CensusCategory, limit: int = 10_000, offset: int = 0) -> list[str]:
    cur = conn.cursor(row_factory=dict_row)
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

async def get_row_data(conn: AsyncConnection, table: str, id: str, offset:int = 0) -> list[DbRow]:
    cur = conn.cursor()
    await cur.execute(f"SELECT * FROM {table} WHERE geo_id LIKE %s LIMIT 500 offset %s", (id.replace('*','%'), offset)) #type: ignore
    res = await cur.fetchall()
    await cur.close()
    data = []
    for row in map(lambda x: dict(x), res):
        row['geometry'] = mapping(row['geog'])
        data.append(DbRow(**row)) #type: ignore
    return data

async def get_pop_data(conn: AsyncConnection, cat: CensusCategory, id: str) -> list[PopQuery]:
    cur = conn.cursor(row_factory=class_row(PopQuery))
    await cur.execute(f"SELECT geo_id, pop FROM {cat.to_table()} WHERE geo_id LIKE %s", (id.replace('*', '%'),)) #type: ignore
    res = await cur.fetchall()
    await cur.close()
    return res


async def get_row_by_polygon(conn: AsyncConnection, cat: CensusCategory, geom: shapely.Polygon) -> list[GeoRefPopQuery]:
    cur = conn.cursor(row_factory=class_row(GeoRefPopQuery))
    await cur.execute(f"""SELECT geo_id, pop,
                           clon AS lon,
                           clat AS lat
                    FROM {cat.to_table()}
                    WHERE ST_Intersects(geog, %s)""", #type: ignore
                (geom,)) 
    res = await cur.fetchall()
    await cur.close()
    return res

# async def get_async_connection(path: str) -> pg.connection.Connection:
#     conn = pg.a(path)
#     return conn

# if __name__ == '__main__':
#     conn = get_async_connection("user=cole password=michelle21 host=localhost dbname=census")
#     coords = [(-100,34),
#               (-97, 34),
#               (-97, 37),
#               (-100, 37),
#               (-100, 34)]
#     data = get_row_by_bbox(conn, CensusCategory.block, shapely.Polygon(coords))
#     print(data)

#     conn.close()
