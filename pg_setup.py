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
        cmd1 = ['shp2pgsql', '-s', '4269', '-D', shp, table]
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
            "counties": "counties",
            "tract": "tracts",
            "bg": "block_groups",
            "tabblock20": "blocks"}
    for cat in cats:
        if file.find(cat) != -1:
            return cats[cat]

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

    for file in data_dir.iterdir():
        print(file)
        table = get_table(file.name)
        if table == None:
            continue

        load_file_to_db(file, table, first[table])
        if first[table]:
            first[table] = False
