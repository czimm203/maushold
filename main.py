import json
import os
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from pydantic.errors import PydanticTypeError
from shapely import geometry
from shapely.geometry.base import shapely
from starlette.responses import HTMLResponse
from maushold.sqlite import SqliteDB, Sqlite3Connector 
from maushold.pg import PgConnector, PgDB
from maushold.models import CensusCategory, DbRow, GeoRefPopQuery, GeoJSON, PopQuery, PopTotal, Unit, Feature, GeometryCollection
from maushold.transform import make_buffered_geo

load_dotenv()

pg_user = os.getenv("PGUSER")
pg_pass = os.getenv("PGPASS")
pg_host = os.getenv("PGHOST")
pg_db = os.getenv("PGDB")
DSN = f"user={pg_user} password={pg_pass} host={pg_host} dbname={pg_db}"
pool = PgConnector(DSN, num_workers=32, timeout=120)

# sqlite_path = "file:./data/census.db?mode=ro&cache=shared&journal_mode=off&sync=off"
# pool = Sqlite3Connector(sqlite_path, uri=True)

app = FastAPI(title="Maushold", description="Simple API for getting georeferenced population data")

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

app.mount("/assets", StaticFiles(directory="./viewer/dist/assets", html=True), name="assets")

@app.get("/")
async def root():
    with open("./viewer/dist/index.html") as f:
        html = f.read()
    return HTMLResponse(html)

@app.get("/{cat}")
async def get_cat_ids(cat: CensusCategory, limit=10_000, offset=0) -> list[str]:
    async with pool.connection() as db:
        data = await db.get_ids(cat, limit, offset)
    return data

@app.get("/{cat}/{id}/pop")
async def get_cat_pop(cat: CensusCategory, id: str) -> list[PopQuery]:
    r"""
    Retrieve population by FIPS id. Values can be matched using the '*' operator. Use commas to separate multiple ids.

    Ex: /tract/20109\*,01001\*/pop
    """
    res = []
    async with pool.connection() as db:
        for code in id.split(","):
            data = await db.get_pop_data(cat, code)
            [res.append(item) for item in data]
    return res

@app.get("/{cat}/{id}")
async def get_cat_by_id(cat: CensusCategory, id: str) -> list[DbRow]:
    r"""
    Retrieve population by FIPS id. Values can be matched using the '*' operator. Use commas to separate multiple ids.

    Ex: /tract/20109\*,01001\*
    """
    async with pool.connection() as db:
        res = []
        for code in id.split(","):
            data = await db.get_row_data(cat, code)
            [res.append(item) for item in data]
    return res

@app.get("/bbox/{cat}")
async def get(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float) -> list[GeoRefPopQuery]:
    poly = shapely.Polygon([(minX, minY), (maxX, minY), (maxX,maxY), (minX, maxY), (minX, minY)])
    async with pool.connection() as db:
        data = await db.get_row_by_geometry(cat, poly)
    return data

@app.get("/bbox/{cat}/pop")
async def get_row_total(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float) -> PopTotal:
    poly = shapely.Polygon([(minX, minY), (maxX, minY), (maxX,maxY), (minX, maxY), (minX, minY)])
    async with pool.connection() as db:
        data = await db.get_row_by_geometry(cat, poly)
    pop = sum([row.pop for row in data if row.pop is not None])
    return PopTotal(pop=pop)

@app.post("/polygon/{cat}")
async def post_pop_by_polygon(cat: CensusCategory, geometry: GeoJSON, buffer: Optional[float]=None, unit: Optional[Unit]=None) -> list[GeoRefPopQuery]:
    async with pool.connection() as db:
        if unit is not None and buffer is not None:
            geometry = make_buffered_geo(geometry, buffer, unit)
        data = await db.get_row_by_geometry(cat, geometry)
    return data

@app.get("/polygon/{cat}")
async def get_pop_by_polygon(cat: CensusCategory, json_str: str, buffer: Optional[float]=None, unit: Optional[Unit]=None) -> list[GeoRefPopQuery]:
    async with pool.connection() as db:
        geojson = json.loads(json_str)
        try:
            geometry = GeoJSON(**geojson)
            if unit is not None and buffer is not None:
                geometry = make_buffered_geo(geometry, buffer, unit)
        except PydanticTypeError:
            raise HTTPException(status_code=422, detail="invalid geojson")
        data = await db.get_row_by_geometry(cat, geometry)
    return data

@app.post("/polygon/{cat}/pop")
async def get_pop_total_by_polygon(cat: CensusCategory, geometry: GeoJSON, buffer: Optional[float]=None, unit: Optional[Unit]=None) -> PopTotal:
    async with pool.connection() as db:
        if unit is not None and buffer is not None:
            geometry = make_buffered_geo(geometry, buffer, unit)
        data = await db.get_row_by_geometry(cat, geometry)
    pop = sum([row.pop for row in data if row.pop is not None])
    return PopTotal(pop=pop)

@app.get("/polygon/{cat}/pop")
async def post_pop_total_by_polygon(cat: CensusCategory, json_str: str) -> PopTotal:
    async with pool.connection() as db:
        geojson = json.loads(json_str)
        try:
            geometry = GeoJSON(**geojson)
        except PydanticTypeError:
            raise HTTPException(status_code=422, detail="invalid geojson")
        data = await db.get_row_by_geometry(cat, geometry)
        pop = sum([row.pop for row in data if row.pop is not None])
    return PopTotal(pop=pop)

@app.post("/polygon/{cat}/geometry")
async def post_geometry(cat: CensusCategory, geojson_str: str) -> GeometryCollection:
    async with pool.connection() as db:
        geojson = json.loads(geojson_str)
        try:
            geometry = GeoJSON(**geojson)
        except PydanticTypeError:
            raise HTTPException(status_code=422, detail="invalid geojson")
        data = await db.get_intersected_geometries(cat, geometry)
        feat = [Feature(geometry=geo) for geo in data]
        gc = GeometryCollection(features=feat)
    return gc
