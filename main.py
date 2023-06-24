from fastapi import FastAPI
from maushold.db import get_pop_data, get_row_data, get_ids
from maushold.models import Polygon

app = FastAPI()

@app.get("/")
async def root():
    return {"message": "Hi, feller"}

@app.get("/state")
async def get_state_ids():
    data = get_ids("states")
    return data

@app.get("/state/pop/{id}")
async def get_state_pop(id):
    data = get_pop_data("states",id)
    return data

@app.get("/state/{id}")
async def get_state(id):
    data = get_row_data("states",id)
    return data

@app.get("/county")
async def get_county_ids():
    data = get_ids("counties")
    return data

@app.get("/county/pop/{id}")
async def get_county_pop(id):
    data = get_pop_data("counties",id)
    return data

@app.get("/county/{id}")
async def get_county(id):
    data = get_row_data("counties",id)
    return data

@app.get("/tract")
async def get_tract_ids(limit = 1_000_000, offset = 0):
    data = get_ids("tracts", limit, offset)
    return data

@app.get("/tract/pop/{id}")
async def get_tract_pop(id):
    data = get_pop_data("tracts",id)
    return data

@app.get("/tract/{id}")
async def get_tract(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (*) to fetch up to 500 rows matching the supplied id."""
    data = get_row_data("tracts",id, offset)
    return data

@app.get("/block_group")
async def get_bg_ids(limit = 1_000_000, offset = 0):
    data = get_ids("block_groups", limit, offset)
    return data

@app.get("/block_groups/pop/{id}")
async def get_block_group_pop(id):
    data = get_pop_data("block_groups",id)
    return data

@app.get("/block_group/{id}")
async def get_block_group(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (*) to fetch up to 500 rows matching the supplied id."""
    data = get_row_data("block_groups",id, offset)
    return data

@app.get("/block")
async def get_block_ids(limit = 1_000_000, offset = 0):
    data = get_ids("blocks", limit, offset)
    return data

@app.get("/block/pop/{id}")
async def get_block_pop(id):
    data = get_pop_data("blocks",id)
    return data

@app.get("/block/{id}")
async def get_block(id, offset=0):
    """Fetch rows matching the supplied id. Use the glob-opertator (*) to fetch up to 500 rows matching the supplied id."""
    data = get_row_data("blocks", id, offset)
    return data

@app.get("/pop")
async def get(geo):
    return({"msg": geo})
