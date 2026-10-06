import polars as pl
import polars._plr as plr


def register(tables, opts):
    return pl.SQLContext({name: pl.scan_parquet(path) for name, path in tables.items()})


def run(ctx, sql, opts):
    return ctx.execute(sql).collect(engine=opts["polars_engine"]).to_arrow()


def info(opts):
    # Versions as reported by the loaded native runtime, not package metadata.
    return {
        "engine_version": plr.__version__,
        "engine_build": getattr(plr, "_BUILD_COMMIT", None),
        "engine_binary": plr.__file__,
        "polars_build_info": pl.build_info(),
        "threads_used": pl.thread_pool_size(),
    }
