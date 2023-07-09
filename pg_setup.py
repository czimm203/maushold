#! /usr/bin/env python3
import geopandas as gpd
import json
import psycopg2 as pg
import psycopg2.errors

from pathlib import Path
from shapely.geometry import mapping

def make_geo_dataframe(path: Path) -> gpd.GeoDataFrame:
    print("reading df: ", path)
    return gpd.read_file(path)

def filter_gdf(df: gpd.GeoDataFrame, is_block = False):
    for _, row in df.iterrows():
        area = row.geometry.area
        if not is_block:
            yield (row["GEOID"], row["INTPTLAT"], row["INTPTLON"], 
                   str(row.geometry), area)
        else:
            yield (row["GEOID20"], row["INTPTLAT20"], row["INTPTLON20"], 
                   str(row.geometry), area, row["HOUSING20"], row["POP20"])


def create_table(db, table: str):
    insert_str = f"""CREATE TABLE IF NOT EXISTS {table}(
            geo_id VARCHAR(32) NOT NULL UNIQUE,
            clat REAL,
            clon REAL,
            minX REAL,
            minY REAL,
            maxX REAL,
            maxY REAL,
            geog geometry,
            area REAL,
            housing INTEGER,
            pop INTEGER
            );"""
    db.cursor().execute(insert_str)

def populate_table(db, table: str, df: gpd.GeoDataFrame, is_block = False):
    if not is_block:
        insert_str = f"INSERT INTO {table}(geo_id, clat, clon, minX, minY, maxX, maxY, geog, area) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    else:
        insert_str = f"INSERT INTO {table}(geo_id, clat, clon, geog, area, housing, pop) VALUES(%s,%s,%s,%s,%s,%s,%s)"
    args = filter_gdf(df, is_block)
    try:
        db.cursor().executemany(insert_str, args)
        db.commit()
    except psycopg2.errors.NumericValueOutOfRange as e:
        print(e)
        db.rollback()

def update_missing(db, df: gpd.GeoDataFrame):
    sql = lambda table: f"""
        UPDATE {table}
        SET pop = %s , housing = %s
        WHERE geo_id = %s;
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

    db.cursor().executemany(sql("block_groups"), bg_gen)
    db.cursor().executemany(sql("tracts"), tract_gen)
    db.cursor().executemany(sql("counties"), county_gen)
    db.cursor().executemany(sql("states"), state_gen)
    db.commit()

def create_id_index(conn, table: str):
    print("Indexing ", table)
    sql = f"""CREATE INDEX "{table}_id" ON "{table}" (
                "geo_id" ASC
            );
    """
    conn.cursor().execute(sql)
    conn.commit()

def create_gist_index(conn, table: str):
    print("Indexing ", table)
    sql = f"""CREATE INDEX "{table}_goeg" ON "{table}" USING GIST(
                "geog"
            );
    """
    conn.cursor().execute(sql)
    conn.commit()

def create_rtree_index(db, table: str):
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
    import os
    from dotenv import load_dotenv
    data_dir = Path("./data")
    load_dotenv()
    pg_user = os.getenv("PGUSER")
    pg_pass = os.getenv("PGPASS")
    pg_host = os.getenv("PGHOST")
    pg_db = os.getenv("PGDB")
    DSN = f"user={pg_user} password={pg_pass} host={pg_host} dbname={pg_db}"
    
    try:
        conn = pg.connect(DSN)
        conn.cursor().execute("CREATE EXTENSION IF NOT EXISTS postgis;")

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
            # update_missing(conn, df)

        create_id_index(conn, "blocks")

        for table in ["blocks", "block_groups", "tracts", "counties", "states"]:
            print("Creating spatial index for ", table)
            create_gist_index(conn, table)
    finally:
        conn.commit()
        conn.close()
