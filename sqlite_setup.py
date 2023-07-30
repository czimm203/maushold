#! /usr/bin/env python3
import geopandas as gpd
import json
import sqlite3
import shapely.wkb as wkb

from pathlib import Path
from shapely.geometry import mapping


def make_geo_dataframe(path: Path) -> gpd.GeoDataFrame:
    print("reading df: ", path)
    return gpd.read_file(path)

def filter_gdf(df: gpd.GeoDataFrame, is_block = False):
    for _, row in df.iterrows():
        area = row.geometry.area
        bounds = row.geometry.bounds
        if not is_block:
            yield (row["GEOID"], row["INTPTLAT"], row["INTPTLON"], 
                   bounds[0], bounds[1], bounds[2],
                   bounds[3], wkb.dumps(row.geometry), area)
        else:
            yield (row["GEOID20"], row["INTPTLAT20"], row["INTPTLON20"], 
                   bounds[0], bounds[1], bounds[2],
                   bounds[3], wkb.dumps(row.geometry), area, row["HOUSING20"], row["POP20"])


def create_table(db: sqlite3.Connection, table: str):
    insert_str = f"""CREATE TABLE IF NOT EXISTS {table}(
            geo_id TEXT PRIMARY KEY NOT NULL UNIQUE,
            clat REAL,
            clon REAL,
            minX REAL,
            minY REAL,
            maxX REAL,
            maxY REAL,
            geometry BLOB,
            area REAL,
            housing INTEGER,
            pop INTEGER
            );"""
    db.execute(insert_str)

def populate_table(db: sqlite3.Connection, table: str, df: gpd.GeoDataFrame, is_block = False):
    if not is_block:
        insert_str = f"INSERT INTO {table}(geo_id, clat, clon, minX, minY, maxX, maxY, geometry, area) VALUES(?,?,?,?,?,?,?,?,?)"
    else:
        insert_str = f"INSERT INTO {table}(geo_id, clat, clon, minX, minY, maxX, maxY, geometry, area, housing, pop) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
    db.executemany(insert_str, filter_gdf(df, is_block))
    db.commit()

def update_missing(db: sqlite3.Connection, df: gpd.GeoDataFrame):
    sql = lambda table: f"""
        UPDATE {table}
        SET pop = ? , housing = ?
        WHERE geo_id = ?;
    """
    df["county_id"] = df["STATEFP20"] + df["COUNTYFP20"]
    df["tract_id"] = df["county_id"] + df["TRACTCE20"]
    df["bg_id"] = df["tract_id"] + df["BLOCKCE20"].apply(lambda x: x[0])
    pops_county = df["POP20"].groupby(df["county_id"]).sum().to_dict()
    pops_tract = df["POP20"].groupby(df["tract_id"]).sum().to_dict()
    pops_bg = df["POP20"].groupby(df["bg_id"]).sum().to_dict()
    pops_state = df["POP20"].groupby(df["STATEFP20"]).sum().to_dict()
    housing_county = df["HOUSING20"].groupby(df["county_id"]).sum().to_dict()
    housing_tract = df["HOUSING20"].groupby(df["tract_id"]).sum().to_dict()
    housing_bg = df["HOUSING20"].groupby(df["bg_id"]).sum().to_dict()
    housing_state = df["HOUSING20"].groupby(df["STATEFP20"]).sum().to_dict()

    county_gen = ((pops_county[id], housing_county[id], id) for id in pops_county.keys())
    tract_gen = ((pops_tract[id], housing_tract[id], id) for id in pops_tract.keys())
    bg_gen = ((pops_bg[id], housing_bg[id], id) for id in pops_bg.keys())
    state_gen = ((pops_state[id], housing_state[id], id) for id in pops_state.keys())

    db.executemany(sql("block_groups"), bg_gen)
    db.executemany(sql("tracts"), tract_gen)
    db.executemany(sql("counties"), county_gen)
    db.executemany(sql("states"), state_gen)
    db.commit()

def create_id_index(conn: sqlite3.Connection, table: str):
    print("Indexing ", table)
    sql = f"""CREATE INDEX "{table}_id" ON "{table}" (
                "geo_id" ASC
            );
    """
    conn.execute(sql)
    conn.commit()

def create_rtree_index(db: sqlite3.Connection, table: str):
    cur = db.cursor()

    cur.execute(f"""CREATE VIRTUAL TABLE IF NOT EXISTS
                v_{table} USING rtree(
                    id PRIMARY KEY AUTOINCREMENT,
                    minX, maxX,
                    minY, maxY,
                    +geo_id, +pop,
                    +clon, +clat
                );""")
    cur.execute(f"""
                    INSERT INTO v_{table}
                    SELECT
                        rowid,
                        minX, maxX,
                        minY, maxY,
                        geo_id, pop,
                        clon, clat
                    FROM {table}
                   """)
    db.commit()


if __name__ == "__main__":
    data_dir = Path("./data")
    
    with sqlite3.connect("./data/census_test.db") as conn:
        create_table(conn, "states")
        create_table(conn, "blocks")
        create_table(conn, "block_groups")
        create_table(conn, "tracts")
        create_table(conn, "counties")

        for file in (data_dir / "block_groups").iterdir():
            print("Working on file: ", file)
            df = make_geo_dataframe(file)
            populate_table(conn, "block_groups", df)

        for file in (data_dir / "tracts").iterdir():
            print("Working on file: ", file)
            df = make_geo_dataframe(file)
            populate_table(conn, "tracts", df)

        print("Working on states")
        df = make_geo_dataframe(data_dir/"tl_2022_us_state.zip")
        populate_table(conn, "states", df)

        print("Working on counties")
        df = make_geo_dataframe(data_dir/"tl_2022_us_county.zip")
        populate_table(conn, "counties", df)

        create_id_index(conn, "block_groups")
        create_id_index(conn, "tracts")
        create_id_index(conn, "counties")
        create_id_index(conn, "states")

        for file in (data_dir / "blocks").iterdir():
            print("Working on file: ", file)
            df = make_geo_dataframe(file)
            populate_table(conn, "blocks", df, True)
            update_missing(conn, df)

        create_id_index(conn, "blocks")

        for table in ["blocks", "block_groups", "tracts", "counties", "states"]:
            print("Creating rtree for ", table)
            create_rtree_index(conn, table)
