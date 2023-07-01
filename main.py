from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pandas.core.window.rolling import RollingAndExpandingMixin
from shapely.geometry.base import shapely
# from maushold.db import get_pop_data, get_row_data, get_ids, get_row_by_bbox
from maushold.pg import get_pop_data, get_row_data, get_ids, get_row_by_bbox, register_types
from maushold.models import CensusCategory, Polygon
from psycopg_pool import AsyncConnectionPool

DSN = "user=cole password=michelle21 host=localhost dbname=census"
pool = AsyncConnectionPool(DSN)
app = FastAPI(title="Maushold", description="Simple API for getting georeferenced population data")

app = FastAPI()

origins = [
    "http://localhost.tiangolo.com",
    "https://localhost.tiangolo.com",
    "http://localhost",
    "http://localhost:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
@app.get("/")
async def root():
    return {"message": "Hi, feller"}

@app.get("/state")
async def get_state_ids():
    async with pool.connection() as conn:
        data = await get_ids(conn, "states")
    return data

@app.get("/state/{id}/pop")
async def get_state_pop(id):
    async with pool.connection() as conn:
        data = await get_pop_data(conn,"states",id)
    return data

@app.get("/state/{id}")
async def get_state(id):
    async with pool.connection() as conn:
        data = await get_row_data(conn, "states",id)
    return data

@app.get("/county")
async def get_county_ids():
    async with pool.connection() as conn:
        data = await get_ids(conn, "counties")
    return data

@app.get("/county/{id}/pop")
async def get_county_pop(id):
    async with pool.connection() as conn:
        data = await get_pop_data(conn, "counties",id)
    return data

@app.get("/county/{id}")
async def get_county(id):
    async with pool.connection() as conn:
        data = await get_row_data(conn, "counties",id)
    return data

@app.get("/tract")
async def get_tract_ids(limit = 1_000_000, offset = 0):
    async with pool.connection() as conn:
        data = await get_ids(conn, "tracts", limit, offset)
    return data

@app.get("/tract/{id}/pop")
async def get_tract_pop(id):
    async with pool.connection() as conn:
        data = await get_pop_data(conn, "tracts",id)
    return data

@app.get("/tract/{id}")
async def get_tract(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (conn, *) to fetch up to 500 rows matching the supplied id."""
    async with pool.connection() as conn:
        data = await get_row_data(conn, "tracts",id, offset)
    return data

@app.get("/block_group")
async def get_bg_ids(limit = 1_000_000, offset = 0):
    async with pool.connection() as conn:
        data = await get_ids(conn, "block_groups", limit, offset)
    return data

@app.get("/block_groups/{id}/pop")
async def get_block_group_pop(id):
    async with pool.connection() as conn:
        data = await get_pop_data(conn, "block_groups",id)
    return data

@app.get("/block_group/{id}")
async def get_block_group(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (conn, *) to fetch up to 500 rows matching the supplied id."""
    async with pool.connection() as conn:
        data = await get_row_data(conn, "block_groups",id, offset)
    return data

@app.get("/block")
async def get_block_ids(limit = 1_000_000, offset = 0):
    async with pool.connection() as conn:
        data = await get_ids(conn, "blocks", limit, offset)
    return data

@app.get("/block/{id}/pop")
async def get_block_pop(id):
    async with pool.connection() as conn:
        data = await get_pop_data(conn, "blocks",id)
    return data

@app.get("/block/{id}")
async def get_block(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (conn, *) to fetch up to 500 rows matching the supplied id."""
    async with pool.connection() as conn:
        data = await get_row_data(conn, "blocks", id, offset)
    return data

# @app.get("/bbox/{cat}")
# async def get(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float):
#     conn = get_connection(DSN)
#     data = get_row_by_bbox(conn, cat, float(minX), float(minY), float(maxX), float(maxY))
#     return data

# @app.get("/bbox/{cat}/pop")
# async def get_row_total(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float):
#     conn = get_connection(DSN)
#     data = get_row_by_bbox(conn, cat, float(minX), float(minY), float(maxX), float(maxY))
#     pop = 0
#     for row in data:
#         pop += row.pop
#     return {"pop":pop}

# @app.post("/polygon/{cat}")
# async def get_pop_by_polygon(cat: CensusCategory, geometry: Polygon):
#     conn = get_connection(DSN)
#     bbox = geometry.bounds()
#     data = get_row_by_bbox(conn, cat, bbox.minX, bbox.minY, bbox.maxX, bbox.maxY)
#     res = []
#     for row in data:
#         if geometry.contains_pt(row.lon, row.lat):
#             res.append(row)
#     return res

@app.post("/polygon/{cat}/pop")
async def get_pop_total_by_polygon(cat: CensusCategory, geometry: Polygon):
    poly = shapely.Polygon(geometry.coordinates[0])
    async with pool.connection() as conn:
        await register_types(conn)
        data = await get_row_by_bbox(conn, cat, poly)
    sum = 0
    for row in data:
        if geometry.contains_pt(row.lon, row.lat):
            sum += row.pop
    return sum
