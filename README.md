# Polars 2.0 benchmark

This repository contains the TPC-H and TPC-DS derived benchmarks for Polars,
DuckDB and DataFusion used in the 2.0 release blog post benchmark for Polars.
The raw data we observed can be found in `results/`.

## Setup

```sh
uv sync
```

`tpcgen-cli` is installed from git and built from source, so this needs a Rust
toolchain and takes a few minutes the first time.

## Generating data

```sh
uv run setup.py tpch 10,100
uv run setup.py tpcds 10,100
```

This writes Parquet files to `data/tpch-<scale_factor>` or `data/tpcds-<scale_factor>`
for each given scale factor.

## Reproducing benchmarks

The `bench.py` script supports more options but the commands to reproduce the results for one machine at SF=100 are:

```sh
uv run bench.py tpch 100 --engine polars --iters 5 --timeout 60 --tag polars-tpch-100-machineid
uv run bench.py tpcds 100 --engine polars --iters 5 --timeout 60 --tag polars-tpcds-100-machineid
uv run bench.py tpch 100 --engine duckdb --iters 5 --timeout 60 --tag duckdb-tpch-100-machineid
uv run bench.py tpcds 100 --engine duckdb --iters 5 --timeout 60 --tag duckdb-tpcds-100-machineid
uv run --with "duckdb==2.0.0.dev2610011535" bench.py tpch 100 --engine duckdb --iters 5 --timeout 60 --tag duckdb2-tpch-100-machineid
uv run --with "duckdb==2.0.0.dev2610011535" bench.py tpcds 100 --engine duckdb --iters 5 --timeout 60 --tag duckdb2-tpcds-100-machineid
uv run bench.py tpch 100 --engine datafusion --iters 5 --timeout 60 --tag datafusion-tpch-100-machineid
uv run bench.py tpcds 100 --engine datafusion --iters 5 --timeout 60 --tag datafusion-tpcds-100-machineid
```

In addition, on the 192 core machine we ran Polars fixed to 32 threads:

```sh
uv run bench.py tpch 100 --engine polars --iters 5 --timeout 60 --threads 32 --tag polars-t32-tpch-100-machineid
uv run bench.py tpcds 100 --engine polars --iters 5 --timeout 60 --threads 32 --tag polars-t32-tpcds-100-machineid
```

Each command was preceded with a `sync; echo 3 | sudo tee /proc/sys/vm/drop_caches` to avoid cross-engine or
cross-benchmark file cache pollution. You may also need to wrap the commands in
`systemd-run --user --scope -p OOMPolicy=continue` to avoid the OOM killer from stopping the entire bench script.

Note that the benchmarks in `results/` were ran with a build of polars compiled from source just before the official
release (commit c630040), so for the Polars commands `uv run bench.py` was replaced with `../polars-build/.venv/bin/python bench.py`.

## TPC-H / TPC-DS Benchmark notice

Note that these benchmarks are derived from TPC-H and TPC-DS Benchmarks and as
such any results obtained are not comparable to published TPC-H and TPC-DS
Benchmark results, as the results obtained do not comply with the TPC-H and
TPC-DS Benchmarks.
