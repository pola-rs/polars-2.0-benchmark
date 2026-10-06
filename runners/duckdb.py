import sys
from pathlib import Path

import duckdb


def register(tables, opts):
    con = duckdb.connect()
    if opts["threads"] is not None:
        con.execute(f"SET threads = {opts['threads']}")
    for name, path in tables.items():
        if Path(path).is_dir():
            path = f"{path}/*.parquet"
        con.execute(f"CREATE VIEW {name} AS SELECT * FROM read_parquet('{path}')")
    return con


def run(ctx, sql, opts):
    return ctx.sql(sql).to_arrow_table()


def info(opts):
    con = register({}, opts)
    version, source_id, _ = con.execute("PRAGMA version").fetchone()
    threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    return {
        "engine_version": version.removeprefix("v"),
        "engine_build": source_id,
        "engine_binary": sys.modules[type(con).__module__].__file__,
        "threads_used": int(threads),
    }
