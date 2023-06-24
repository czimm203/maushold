import json
import sqlite3

from .models import DbRow, PopQuery

def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect("./data/census.db")
    conn.row_factory = sqlite3.Row
    return conn

def get_ids(table: str, limit: int = 1_000_000, offset: int = 0) -> list[str]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT geo_id FROM {table} LIMIT ? OFFSET ?;", (limit, offset))
    res = cur.fetchall()
    cur.close()
    cur.close()
    return [row["geo_id"] for row in res]

def get_row_data(table: str, id: str, offset:int = 0) -> list[DbRow]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table} WHERE geo_id GLOB ? LIMIT 500 offset ?", (id, offset))
    res = cur.fetchall()
    cur.close()
    conn.close()
    data = []
    for row in map(lambda x: dict(x), res):
        row['geometry'] = json.loads(row['geometry'])
        data.append(DbRow(**row)) #type: ignore
    return data

def get_pop_data(table: str, id: str) -> list[PopQuery]:
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT geo_id, pop FROM {table} WHERE geo_id GLOB ?", (id,))
    res = cur.fetchall()
    cur.close()
    conn.close()
    return [PopQuery(**row) for row in res]
