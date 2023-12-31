#! /usr/bin/env python3

import psycopg as pg
import os
import subprocess
import tempfile
import zipfile

from dotenv import load_dotenv
from pathlib import Path

data_dir = Path("./data")
load_dotenv()
pg_user = os.getenv("PGUSER")
pg_pass = os.getenv("PGPASS")
pg_host = os.getenv("PGHOST")
pg_db = os.getenv("PGDB")

#load into db
#drop unneeded columns
#rename needed column

def load_file_to_db(file: str | Path, table: str, is_first: bool):
    with tempfile.TemporaryDirectory() as d:
        with zipfile.ZipFile(file) as z:
            z.extractall(d)

        p = Path(d+'/insert.sql')
        p.touch()
        shp = f'{d}/{str(file).split("/")[-1].replace(".zip", ".shp")}'
        cmd1 = ['shp2pgsql', '-D', shp, table]
        cmd2 = ['psql', '-h', pg_host, '-U', pg_user, 
                '-d', pg_db, '-f', str(p)]
        if not is_first:
            cmd1.insert(1,'-a')
        f = p.open('r+')
        subprocess.run(cmd1, stdout=f)
        f.close()
        subprocess.run(cmd2)

def get_table(file: str) -> str | None:
    if not file.endswith(".zip"):
        return None
    cats = {"state": "states",
            "county": "counties",
            "tract": "tracts",
            "bg": "block_groups",
            "tabblock20": "blocks"}
    for cat in cats:
        if file.find(cat) != -1:
            return cats[cat]

def get_all_files(directory: Path)->list[Path]:
    files = []
    for item in directory.iterdir():
        if item.is_file():
            files.append(item)
        elif item.is_dir():
            for nested in item.iterdir():
                files.append(nested)
    return files


if __name__ == "__main__":
    DSN = f"user={pg_user} password={pg_pass} host={pg_host} dbname={pg_db}"
    with pg.connect(DSN) as conn:
        conn.cursor().execute("CREATE EXTENSION IF NOT EXISTS postgis")

    first = {"states": True,
             "counties": True,
             "tracts": True,
             "block_groups": True,
             "blocks": True
            }
    
    files = get_all_files(data_dir)
    print(files)

    for file in files:
        print(file)
        table = get_table(file.name)
        if table == None:
            continue

        load_file_to_db(file, table, first[table])
        if first[table]:
            first[table] = False
    sql_stmts = ["ALTER TABLE blocks DROP COLUMN awater20;",
                 "ALTER TABLE blocks DROP COLUMN aland20;",
                 "ALTER TABLE blocks DROP COLUMN funcstat20;",
                 "ALTER TABLE blocks DROP COLUMN uatype20;",
                 "ALTER TABLE blocks DROP COLUMN uace20;",
                 "ALTER TABLE blocks DROP COLUMN mtfcc20;",
                 "ALTER TABLE blocks DROP COLUMN statefp20;",
                 "ALTER TABLE blocks DROP COLUMN countyfp20;",
                 "ALTER TABLE blocks DROP COLUMN tractce20;",
                 "ALTER TABLE blocks DROP COLUMN blockce20;",
                 "ALTER TABLE blocks RENAME COLUMN geoid20 TO geo_id;",
                 "ALTER TABLE blocks RENAME COLUMN intptlon20 TO clon;",
                 "ALTER TABLE blocks RENAME COLUMN intptlat20 TO clat;",
                 "ALTER TABLE blocks RENAME COLUMN geom TO geog;",
                 "ALTER TABLE blocks RENAME COLUMN pop20 TO pop;",

                 "ALTER TABLE block_groups DROP COLUMN awater;",
                 "ALTER TABLE block_groups DROP COLUMN aland;",
                 "ALTER TABLE block_groups DROP COLUMN funcstat;",
                 "ALTER TABLE block_groups DROP COLUMN mtfcc;",
                 "ALTER TABLE block_groups DROP COLUMN statefp;",
                 "ALTER TABLE block_groups DROP COLUMN countyfp;",
                 "ALTER TABLE block_groups DROP COLUMN tractce;",
                 "ALTER TABLE block_groups DROP COLUMN blkgrpce;",
                 "ALTER TABLE block_groups ADD COLUMN pop INTEGER;",
                 "ALTER TABLE block_groups RENAME COLUMN geoid TO geo_id;",
                 "ALTER TABLE block_groups RENAME COLUMN geom TO geog;",
                 "ALTER TABLE block_groups RENAME COLUMN intptlon TO clon;",
                 "ALTER TABLE block_groups RENAME COLUMN intptlat TO clat;",

                 "ALTER TABLE tracts DROP COLUMN awater;",
                 "ALTER TABLE tracts DROP COLUMN aland;",
                 "ALTER TABLE tracts DROP COLUMN funcstat;",
                 "ALTER TABLE tracts DROP COLUMN mtfcc;",
                 "ALTER TABLE tracts DROP COLUMN statefp;",
                 "ALTER TABLE tracts DROP COLUMN countyfp;",
                 "ALTER TABLE tracts DROP COLUMN tractce;",
                 "ALTER TABLE tracts ADD COLUMN pop INTEGER;",
                 "ALTER TABLE tracts RENAME COLUMN geoid TO geo_id;",
                 "ALTER TABLE tracts RENAME COLUMN geom TO geog;",
                 "ALTER TABLE tracts RENAME COLUMN intptlon TO clon;",
                 "ALTER TABLE tracts RENAME COLUMN intptlat TO clat;",

                 "ALTER TABLE counties DROP COLUMN awater;",
                 "ALTER TABLE counties DROP COLUMN aland;",
                 "ALTER TABLE counties DROP COLUMN funcstat;",
                 "ALTER TABLE counties DROP COLUMN mtfcc;",
                 "ALTER TABLE counties DROP COLUMN statefp;",
                 "ALTER TABLE counties DROP COLUMN countyfp;",
                 "ALTER TABLE counties ADD COLUMN pop INTEGER;",
                 "ALTER TABLE counties RENAME COLUMN geoid TO geo_id;",
                 "ALTER TABLE counties RENAME COLUMN geom TO geog;",
                 "ALTER TABLE counties RENAME COLUMN intptlon TO clon;",
                 "ALTER TABLE counties RENAME COLUMN intptlat TO clat;",

                 "ALTER TABLE states DROP COLUMN awater;",
                 "ALTER TABLE states DROP COLUMN aland;",
                 "ALTER TABLE states DROP COLUMN funcstat;",
                 "ALTER TABLE states DROP COLUMN mtfcc;",
                 "ALTER TABLE states DROP COLUMN statefp;",
                 "ALTER TABLE states ADD COLUMN pop INTEGER;",
                 "ALTER TABLE states RENAME COLUMN geoid TO geo_id;",
                 "ALTER TABLE states RENAME COLUMN geom TO geog;",
                 "ALTER TABLE states RENAME COLUMN intptlon TO clon;",
                 "ALTER TABLE states RENAME COLUMN intptlat TO clat;",
                ]
    with pg.connect(DSN) as conn:
        for stmt in sql_stmts:
            print(stmt)
            conn.execute(stmt.encode())

        for table in first:
            print("Making GIST index for ", table)
            sql = f'CREATE INDEX "{table}_goeg" ON "{table}" USING GIST("geog");'.encode()
            conn.execute(sql)
            print("Making geoid index for ", table)
            sql = f'CREATE INDEX "{table}_id" ON "{table}" ("geo_id" ASC);'.encode()
            conn.execute(sql)
            update = f"""UPDATE {table} SET pop = bar.pop FROM (SELECT geo_id, SUM(pop) as pop FROM (
                            SELECT states.geo_id AS geo_id,
                            blocks.pop as pop 
                            FROM states
                            JOIN blocks
                            ON blocks.geo_id ^@ states.geo_id) as foo
                            GROUP BY geo_id) AS bar
                        WHERE {table}.geo_id=bar.geo_id;
                    """
            if table != 'blocks':
                conn.execute(update.encode())
