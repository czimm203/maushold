from fastapi import FastAPI
from maushold.db import get_pop_data, get_row_data, get_ids, get_row_by_bbox
from maushold.models import CensusCategory, Polygon

app = FastAPI(title="Maushold", description="Simple API for getting georeferenced population data")

@app.get("/")
async def root():
    return {"message": "Hi, feller"}

@app.get("/state")
async def get_state_ids():
    data = await get_ids("states")
    return data

@app.get("/state/{id}/pop")
async def get_state_pop(id):
    data = await get_pop_data("states",id)
    return data

@app.get("/state/{id}")
async def get_state(id):
    data = await get_row_data("states",id)
    return data

@app.get("/county")
async def get_county_ids():
    data = await get_ids("counties")
    return data

@app.get("/county/{id}/pop")
async def get_county_pop(id):
    data = await get_pop_data("counties",id)
    return data

@app.get("/county/{id}")
async def get_county(id):
    data = await get_row_data("counties",id)
    return data

@app.get("/tract")
async def get_tract_ids(limit = 1_000_000, offset = 0):
    data = await get_ids("tracts", limit, offset)
    return data

@app.get("/tract/{id}/pop")
async def get_tract_pop(id):
    data = await get_pop_data("tracts",id)
    return data

@app.get("/tract/{id}")
async def get_tract(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (*) to fetch up to 500 rows matching the supplied id."""
    data = await get_row_data("tracts",id, offset)
    return data

@app.get("/block_group")
async def get_bg_ids(limit = 1_000_000, offset = 0):
    data = await get_ids("block_groups", limit, offset)
    return data

@app.get("/block_groups/{id}/pop")
async def get_block_group_pop(id):
    data = await get_pop_data("block_groups",id)
    return data

@app.get("/block_group/{id}")
async def get_block_group(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (*) to fetch up to 500 rows matching the supplied id."""
    data = await get_row_data("block_groups",id, offset)
    return data

@app.get("/block")
async def get_block_ids(limit = 1_000_000, offset = 0):
    data = await get_ids("blocks", limit, offset)
    return data

@app.get("/block/{id}/pop")
async def get_block_pop(id):
    data = await get_pop_data("blocks",id)
    return data

@app.get("/block/{id}")
async def get_block(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (*) to fetch up to 500 rows matching the supplied id."""
    data = await get_row_data("blocks", id, offset)
    return data

@app.get("/bbox/{cat}")
async def get(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float):
    data = await get_row_by_bbox(cat, float(minX), float(minY), float(maxX), float(maxY))
    return data

@app.get("/bbox/{cat}/total")
async def get_row_total(cat: CensusCategory, minX: float, minY: float, maxX: float, maxY: float):
    data = await get_row_by_bbox(cat, float(minX), float(minY), float(maxX), float(maxY))
    pop = 0
    for row in data:
        pop += row.pop
    return {"pop":pop}

@app.post("/polygon/{cat}")
async def get_pop_by_polygon(cat: CensusCategory, geometry: Polygon):
    bbox = geometry.bounds()
    data = await get_row_by_bbox(cat, bbox.minX, bbox.minY, bbox.maxX, bbox.maxY)
    res = []
    for row in data:
        if geometry.contains_pt(row.lon, row.lat):
            res.append(row)
    return res
