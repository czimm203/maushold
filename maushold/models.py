import shapely

from enum import Enum
from pydantic import BaseModel
from shapely.geometry import shape
from typing import Any, Optional

class DbRow(BaseModel):
    geo_id: str
    clat: float
    clon: float
    minX: Optional[float]
    minY: Optional[float]
    maxX: Optional[float]
    maxY: Optional[float]
    geometry: dict
    area: float
    housing: int
    pop: int

class PopQuery(BaseModel):
    geo_id: str
    pop: int

class PopTotal(BaseModel):
    pop: int

class GeoRefPopQuery(BaseModel):
    geo_id: str 
    pop: int | None
    lon: float
    lat: float

class PolygonType(str, Enum):
    Polygon = "Polygon"
    MultiPolygon = "MultiPolygon"
    GeometryCollection = "GeometryCollection"

class GeoJSON(BaseModel):
    type: PolygonType
    coordinates: list[float] | list[list[float]] | list[list[list[float]]] | list[list[list[list[float]]]]
    properties: Optional[dict[str, Any]]

    def to_shapely(self):
        match self.type:
            case 'Polygon' | 'MultiPolygon':
                return shape(self.__dict__)
            case 'GeometryCollection':
                return shapely.GeometryCollection(self.coordinates)

class CensusCategory(str, Enum):
    state = 'state'
    county = 'county'
    tract = 'tract'
    block_group = 'block_group'
    block = 'block'

    def to_table(self) -> str:
        match self:
            case self.state:
                return "states"
            case self.county:
                return "counties"
            case self.tract:
                return "tracts"
            case self.block_group:
                return "block_groups"
            case self.block:
                return "blocks"
            case _:
                return ""

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
            
