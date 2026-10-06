import os

import datafusion._internal
from datafusion import SessionConfig, SessionContext


def context(opts):
    config = SessionConfig()
    if opts["threads"] is not None:
        config = config.with_target_partitions(opts["threads"])
    return SessionContext(config)


def register(tables, opts):
    ctx = context(opts)
    for name, path in tables.items():
        ctx.register_parquet(name, path)
    return ctx


def run(ctx, sql, opts):
    return ctx.sql(sql).to_arrow_table()


def info(opts):
    # E.g. "Apache DataFusion 54.0.0, aarch64 on macos", from the Rust core.
    build = context(opts).sql("SELECT version() AS v").to_pylist()[0]["v"]
    threads = opts["threads"] if opts["threads"] is not None else os.cpu_count()
    return {
        "engine_version": build.removeprefix("Apache DataFusion ").split(",")[0],
        "engine_build": build,
        "engine_binary": datafusion._internal.__file__,
        "threads_used": threads,
    }
