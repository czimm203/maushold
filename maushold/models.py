import shapely

from enum import Enum
from pydantic import BaseModel
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

class GeoRefPopQuery(BaseModel):
    geo_id: str
    pop: int
    lon: float
    lat: float

class GeoJSONFeat(BaseModel):
    type: str
    geometry: list[float] | list[list[float]] | list[list[list[float]]] | list[list[list[list[float]]]]
    properties: dict[str, Any]

class CensusCategory(str, Enum):
    state = 'state'
    county = 'county'
    tract = 'tract'
    block_group = 'block_group'
    block = 'block'

class BoundingBox(BaseModel):
    minX: float
    minY: float
    maxX: float
    maxY: float
        
class Polygon(BaseModel):
    type: str = 'Polygon'
    coordinates: list[list[tuple[float,float]]]

    def bounds(self) -> BoundingBox:
        minX, minY = 999, 999
        maxX, maxY = -999, -999
        for p in self.coordinates[0]:
            if p[0] > maxX:
                maxX = p[0]
            if p[0] < minX:
                minX = p[0]
            if p[1] > maxY:
                maxY = p[1]
            if p[1] < minY:
                minY = p[1]
        return BoundingBox(minX=minX, minY=minY, maxX=maxX, maxY=maxY)

    def contains_pt(self, x: float, y: float) -> bool:
        poly = shapely.Polygon(self.coordinates[0])
        return shapely.contains(poly, shapely.Point(x,y))
            
