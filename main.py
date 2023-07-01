from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pandas.core.window.rolling import RollingAndExpandingMixin
from shapely.geometry.base import shapely
# from maushold.db import get_pop_data, get_row_data, get_ids, get_row_by_bbox
from maushold.pg import get_pop_data, get_row_data, get_ids, get_row_by_polygon, register_types
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

@app.get("/{cat}")
async def get_state_ids(cat: CensusCategory, limit=10_000, offset=0):
    async with pool.connection() as conn:
        data = await get_ids(conn, cat, limit, offset)
    return data

@app.get("/{cat}/{id}/pop")
async def get_state_pop(cat: CensusCategory, id: str):
    async with pool.connection() as conn:
        data = await get_pop_data(conn, cat,id)
    return data

@app.get("/{cat}/{id}")
async def get_state(cat: CensusCategory, id: str):
    async with pool.connection() as conn:
        data = await get_row_data(conn, cat,id)
    return data

@app.get("/bbox/{cat}")
async def get(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float):
    poly = shapely.Polygon([(minX, minY), (maxX, minY), (maxX,maxY), (minX, maxY), (minX, minY)])
    async with pool.connection() as conn:
        await(register_types(conn))
        data = await get_row_by_polygon(conn, cat, poly)
    return data

@app.get("/bbox/{cat}/pop")
async def get_row_total(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float):
    poly = shapely.Polygon([(minX, minY), (maxX, minY), (maxX,maxY), (minX, maxY), (minX, minY)])
    async with pool.connection() as conn:
        await(register_types(conn))
        data = await get_row_by_polygon(conn, cat, poly)
    pop = 0
    for row in data:
        pop += row.pop
    return {"pop":pop}

@app.post("/polygon/{cat}")
async def get_pop_by_polygon(cat: CensusCategory, geometry: Polygon):
    poly = shapely.Polygon(geometry.coordinates[0])
    async with pool.connection() as conn:
        await(register_types(conn))
    bbox = geometry.bounds()
    data = await get_row_by_polygon(conn, cat, poly)
    res = []
    for row in data:
        if geometry.contains_pt(row.lon, row.lat):
            res.append(row)
    return res

@app.post("/polygon/{cat}/pop")
async def get_pop_total_by_polygon(cat: CensusCategory, geometry: Polygon):
    poly = shapely.Polygon(geometry.coordinates[0])
    async with pool.connection() as conn:
        await register_types(conn)
        data = await get_row_by_polygon(conn, cat, poly)
    sum = 0
    for row in data:
        if geometry.contains_pt(row.lon, row.lat):
            sum += row.pop
    return sum
