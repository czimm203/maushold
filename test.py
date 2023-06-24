#! /usr/bin/env python3
import geopandas as gpd
import sqlite3
import gis

from pathlib import Path

p = Path("./data/blocks/tl_2022_20_tabblock20.zip")

df = gis.make_geo_dataframe(p)
