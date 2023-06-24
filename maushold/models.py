from pydantic import BaseModel
from shapely import geometry
from typing import Any

class DbRow(BaseModel):
    geo_id: str
    clat: float
    clon: float
    minX: float
    minY: float
    maxX: float
    maxY: float
    geometry: dict
    area: float
    housing: int
    pop: int

class PopQuery(BaseModel):
    geo_id: str
    pop: int

class GeoJSONFeat(BaseModel):
    type: str
    geometry: list[float] | list[list[float]] | list[list[list[float]]] | list[list[list[list[float]]]]
    properties: dict[str, Any]

class Polygon(BaseModel):
    coordinates: list[tuple[float,float]]
