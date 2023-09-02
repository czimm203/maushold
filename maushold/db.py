from abc import ABC, abstractmethod

from .models import CensusCategory, DbRow, GeoJSON, GeoRefPopQuery, PopQuery

class DataBase(ABC):
    @abstractmethod
    async def get_ids(self, cat: CensusCategory, limit: int, offset: int):
        pass

    @abstractmethod
    async def get_row_data(self , table: str, id: str, offset) -> list[DbRow]:
        pass

    @abstractmethod
    async def get_pop_data(self, cat: CensusCategory, id: str) -> list[PopQuery]:
        pass

    @abstractmethod
    async def get_row_by_geometry(self, cat: CensusCategory, geom: GeoJSON) -> list[GeoRefPopQuery]:
        pass
