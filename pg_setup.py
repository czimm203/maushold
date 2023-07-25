#! /usr/bin/env python3

import psycopg as pg
import os
import subprocess
import sys
import tempfile
import zipfile

from dotenv import load_dotenv
from io import FileIO
from pathlib import Path

data_dir = Path("./data")
load_dotenv()
pg_user = os.getenv("PGUSER")
pg_pass = os.getenv("PGPASS")
pg_host = os.getenv("PGHOST")
pg_db = os.getenv("PGDB")

#unzip files
#load into db
#drop unneeded columns
#rename needed column

def load_file_to_db(file: str | Path, table: str, is_first: bool = False):
    with tempfile.TemporaryDirectory() as d:
        with zipfile.ZipFile(file) as z:
            z.extractall(d)

        p = Path(d+'/insert.sql')
        p.touch()
        shp = f'{d}/{str(file).split("/")[-1].replace(".zip", ".shp")}'
        cmd1 = ['shp2pgsql', '-s', '4269', '-I', '-D', shp, table]
        cmd2 = ['psql', '-h', pg_host, '-U', pg_user, 
                '-d', pg_db, '-f', str(p)]
        if is_first:
            cmd1.append('-a')
        cmd = cmd1+cmd2
        f = p.open('r+')
        subprocess.run(cmd1, stdout=f)
        f.close()
        subprocess.run(cmd2)

if __name__ == "__main__":
    DSN = f"user={pg_user} password={pg_pass} host={pg_host} dbname={pg_db}"
    load_file_to_db('./data/tl_2022_us_state.zip', 'test2')
