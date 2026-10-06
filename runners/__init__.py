"""Engine-specific query runners.

Each engine is a module runners.<engine> with three functions:
    register(tables, opts) -> ctx       creates an engine context with the tables registered
    run(ctx, sql, opts) -> pyarrow.Table  runs the query and returns its result
    info(opts) -> dict                  engine metadata, at least engine_version,
                                        engine_build, engine_binary and threads_used,
                                        as reported by the loaded native library
where `tables` maps table names to parquet file/directory paths. Only run() is timed.
"""

ENGINES = ["polars", "duckdb", "datafusion"]
