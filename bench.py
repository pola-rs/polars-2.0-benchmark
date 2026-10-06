import argparse
import hashlib
import importlib
import itertools
import json
import multiprocessing
import os
import platform
import queue
import resource
import secrets
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from runners import ENGINES

ROOT = Path(__file__).parent
QUERY_DIR = ROOT / "queries/sql"


def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, check=True).stdout.strip()
    except Exception:
        return None


def environment_info():
    cpu = mem = None
    if sys.platform == "darwin":
        cpu = sh(["sysctl", "-n", "machdep.cpu.brand_string"])
        mem = int(sh(["sysctl", "-n", "hw.memsize"]) or 0) or None
    else:
        try:
            for line in Path("/proc/cpuinfo").read_text().splitlines():
                if line.startswith("model name"):
                    cpu = line.split(":", 1)[1].strip()
                    break
            for line in Path("/proc/meminfo").read_text().splitlines():
                if line.startswith("MemTotal"):
                    mem = int(line.split()[1]) * 1024
                    break
        except OSError:
            pass
    lock = ROOT / "uv.lock"
    return {
        "hostname": socket.gethostname(),
        "os": platform.platform(),
        "kernel": platform.release(),
        "cpu_model": cpu,
        "cpu_count": os.cpu_count(),
        "total_memory_bytes": mem,
        "python_version": platform.python_version(),
        "argv": sys.argv,
        "uv_lock_sha256": hashlib.sha256(lock.read_bytes()).hexdigest() if lock.exists() else None,
    }


def drop_caches_cmd():
    if sys.platform == "darwin":
        return ["sudo", "-n", "sh", "-c", "sync && purge"]
    return ["sudo", "-n", "sh", "-c", "sync && echo 3 > /proc/sys/vm/drop_caches"]


def drop_caches():
    r = subprocess.run(drop_caches_cmd(), capture_output=True, text=True)
    if r.returncode != 0:
        sys.exit(
            "error: --cold needs to clear the filesystem cache, which failed:\n"
            f"  {' '.join(drop_caches_cmd())}\n  {r.stderr.strip()}\n"
            "Make sure this runs under sudo without a password prompt (e.g. run `sudo -v` first)."
        )


def find_tables(data_dir):
    tables = {}
    for p in sorted(data_dir.iterdir()):
        if p.is_file() and p.suffix == ".parquet":
            tables[p.stem] = str(p.resolve())
        elif p.is_dir() and any(p.glob("*.parquet")):
            tables[p.name] = str(p.resolve())
    return tables


def parse_queries(benchmark, spec):
    qdir = QUERY_DIR / benchmark
    if spec is None:
        return sorted(p.stem for p in qdir.glob("q*.sql"))
    queries = []
    for s in spec.split(","):
        s = s.strip().lower().removeprefix("q")
        if not s.isdigit():
            sys.exit(f"error: invalid query '{s}'")
        q = f"q{int(s):02d}"
        if not (qdir / f"{q}.sql").exists():
            sys.exit(f"error: query {q} does not exist in {qdir}")
        queries.append(q)
    return queries


def fmt_bytes(n):
    for unit in ["B", "KiB", "MiB", "GiB", "TiB"]:
        if n < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.2f} {unit}"
        n /= 1024


def max_rss_bytes():
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return rss if sys.platform == "darwin" else rss * 1024


def table_hash(tbl):
    import pyarrow as pa

    tbl = tbl.replace_schema_metadata(None).combine_chunks()
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, tbl.schema) as writer:
        writer.write_table(tbl)
    return hashlib.sha256(sink.getvalue()).hexdigest()[:16]


# Run untimed after setup and before the first measured run, so one-time engine
# initialisation (thread pools, lazy statics, ...) isn't timed, also in cold mode.
PRIME_SQL = "SELECT a, count(*) AS n, sum(b) AS s FROM (VALUES (1, 2.0), (2, 3.0), (1, 4.0)) AS t(a, b) GROUP BY a"

# Everything else, including engine panics (pyo3 PanicException is a
# BaseException), is recorded as an error.
PASSTHROUGH = (KeyboardInterrupt, SystemExit, GeneratorExit)


def error_text(e):
    return f"{type(e).__name__}: {e}"


def measurements(engine, sql, tables, opts, warmup, iters):
    """Sets up the engine, then calls its run() warmup + iters times.

    Yields {"started": True, "warmup": ...} right before each run and a result after
    it. Timing starts at the run itself, after imports and registering the tables.
    The first error ends the sequence with an error result.
    """
    try:
        runner = importlib.import_module(f"runners.{engine}")
        info = runner.info(opts)
        ctx = runner.register(tables, opts)
        runner.run(ctx, PRIME_SQL, opts)
    except PASSTHROUGH:
        raise
    except BaseException as e:
        yield {"warmup": warmup > 0, "status": "error", "error": f"setup failed: {error_text(e)}"}
        return

    for i in range(warmup + iters):
        res = {**info, "warmup": i < warmup, "timestamp_start": datetime.now(timezone.utc).isoformat()}
        yield {"started": True, "warmup": res["warmup"]}
        try:
            r0 = resource.getrusage(resource.RUSAGE_SELF)
            t0 = time.perf_counter()
            tbl = runner.run(ctx, sql, opts)
            t1 = time.perf_counter()
            r1 = resource.getrusage(resource.RUSAGE_SELF)
            rows, digest = tbl.num_rows, table_hash(tbl)
            del tbl
        except PASSTHROUGH:
            raise
        except BaseException as e:
            yield {**res, "status": "error", "error": error_text(e)}
            return
        yield {
            **res,
            "status": "ok",
            "error": None,
            "wall_time_s": t1 - t0,
            "user_cpu_s": r1.ru_utime - r0.ru_utime,
            "sys_cpu_s": r1.ru_stime - r0.ru_stime,
            "max_rss_bytes": max_rss_bytes(),
            "result_rows": rows,
            "result_hash": digest,
        }


def measure(engine, sql, tables, opts, warmup, iters, results):
    """Process target: forwards measurements() to the parent."""
    for res in measurements(engine, sql, tables, opts, warmup, iters):
        results.put(res)


def process_results(proc, results, timeout, warmup):
    """Yields the messages of a measure() process, ending with an error result if it
    crashes or a run exceeds the timeout (counted from the start of that run)."""
    deadline = None
    while True:
        wait = 0.1 if deadline is None else min(0.1, max(0.0, deadline - time.monotonic()))
        try:
            msg = results.get(timeout=wait)
        except queue.Empty:
            if proc.is_alive():
                if deadline is not None and time.monotonic() >= deadline:
                    proc.kill()
                    yield {"warmup": warmup, "status": "timeout", "error": f"exceeded {timeout}s"}
                    return
                continue
            try:
                msg = results.get(timeout=1)
            except queue.Empty:
                yield {"warmup": warmup, "status": "error", "error": f"process exited with code {proc.exitcode}"}
                return
        if msg.get("started"):
            warmup = msg["warmup"]
            deadline = None if timeout is None else time.monotonic() + timeout
        else:
            deadline = None
        yield msg


def measured_runs(msgs, iters):
    """Yields (label, result) per finished run, where label is the iteration number,
    or "warmup" for a failed warmup. Stops after the last iteration or a failure."""
    i = 1
    for res in msgs:
        if res.pop("started", False):
            continue
        if res.get("warmup"):
            if res["status"] == "ok":
                continue
            yield "warmup", res
            return
        yield i, res
        if res["status"] != "ok" or i == iters:
            return
        i += 1


def run_query(engine, sql, tables, opts, warmup, iters, args):
    """Runs one query, yields (label, result) per finished run."""
    if args.isolation == "none":
        yield from measured_runs(measurements(engine, sql, tables, opts, warmup, iters), iters)
        return

    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    proc = ctx.Process(target=measure, args=(engine, sql, tables, opts, warmup, iters, results))
    proc.start()
    try:
        yield from measured_runs(process_results(proc, results, args.timeout, warmup > 0), iters)
    except BaseException:
        proc.kill()
        raise
    finally:
        proc.join(timeout=5)
        if proc.is_alive():
            proc.kill()
            proc.join()


def query_runs(engine, sql, tables, opts, args):
    """Yields (label, result) for every finished run of one query."""
    if not args.cold:
        yield from run_query(engine, sql, tables, opts, 1, args.iters, args)
        return
    # Cold: clear the fs cache before every run (each in a fresh process unless
    # --isolation none), stopping at the first failure like hot mode.
    for i in range(1, args.iters + 1):
        drop_caches()
        for _, res in run_query(engine, sql, tables, opts, 0, 1, args):
            yield i, res
        if res["status"] != "ok":
            return


def write_record(out_dir, stem, record):
    """Writes a result file, never overwriting an existing one."""
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S%f")
    for n in itertools.count():
        path = out_dir / f"{stem}-{timestamp}{f'-{n}' if n else ''}.json"
        try:
            with open(path, "x") as f:
                f.write(json.dumps(record, indent=2) + "\n")
            return
        except FileExistsError:
            continue


def positive(type_):
    def parse(s):
        v = type_(s)
        if not v > 0:
            raise argparse.ArgumentTypeError(f"must be positive, got {s}")
        return v

    parse.__name__ = f"positive {type_.__name__}"
    return parse


def main():
    parser = argparse.ArgumentParser(description="Run TPC benchmark queries.")
    subparsers = parser.add_subparsers(dest="benchmark", required=True)
    for benchmark in ["tpch", "tpcds"]:
        sub = subparsers.add_parser(benchmark, help=f"run {benchmark} queries")
        sub.add_argument("scale_factor")
        sub.add_argument("--engine", choices=ENGINES, default="polars")
        sub.add_argument("--iters", type=positive(int), default=1)
        sub.add_argument("--query", help="comma-separated query numbers, e.g. 1,5,14 (default: all)")
        mode = sub.add_mutually_exclusive_group()
        mode.add_argument("--hot", dest="cold", action="store_false", help="one warmup iteration per query (default)")
        mode.add_argument("--cold", dest="cold", action="store_true", help="no warmup, clear fs cache before each run")
        sub.set_defaults(cold=False)
        sub.add_argument("--threads", type=positive(int), help="number of threads (default: engine default)")
        sub.add_argument("--timeout", type=positive(float), help="timeout per run in seconds")
        sub.add_argument(
            "--isolation", choices=["process", "none"], default="process",
            help="process: each query in a fresh process (default); none: run in this process, e.g. for profiling",
        )
        sub.add_argument("--polars-engine", choices=["streaming", "in-memory", "auto"], default="streaming")
        sub.add_argument("--data-dir", type=Path, default=ROOT / "data")
        sub.add_argument("--tag", help="name for this run (default: random)")
        sub.add_argument("--out", type=Path, default=ROOT / "results")
    args = parser.parse_args()

    data_dir = args.data_dir / f"{args.benchmark}-{args.scale_factor}"
    if not data_dir.is_dir():
        sys.exit(f"error: {data_dir} does not exist, generate it with `uv run setup.py {args.benchmark} {args.scale_factor}`")
    tables = find_tables(data_dir)
    queries = parse_queries(args.benchmark, args.query)

    if args.tag is None:
        args.tag = datetime.now().strftime("%Y%m%d-%H%M%S-") + secrets.token_hex(2)
        print(f"tag: {args.tag}", file=sys.stderr)
    out_dir = args.out / args.tag
    out_dir.mkdir(parents=True, exist_ok=True)

    if args.isolation == "none" and args.timeout is not None:
        print("warning: --timeout is ignored with --isolation none", file=sys.stderr)
        args.timeout = None

    if args.cold:
        drop_caches()  # Fail early if we can't.

    # Engines are only imported in the process that runs the queries, after this.
    if args.engine == "polars" and args.threads is not None:
        os.environ["POLARS_MAX_THREADS"] = str(args.threads)

    common = {
        "tag": args.tag,
        "benchmark": args.benchmark,
        "scale_factor": args.scale_factor,
        "engine": args.engine,
        "polars_engine": args.polars_engine if args.engine == "polars" else None,
        "mode": "cold" if args.cold else "hot",
        "isolation": args.isolation,
        "threads_requested": args.threads,
        "timeout_s": args.timeout,
        "environment": environment_info(),
    }
    opts = {"threads": args.threads, "polars_engine": args.polars_engine}

    for q in queries:
        sql = (QUERY_DIR / args.benchmark / f"{q}.sql").read_text().strip().rstrip(";")
        for label, res in query_runs(args.engine, sql, tables, opts, args):
            warmup = label == "warmup"
            record = {**common, "query": q, "iteration": None if warmup else label, **res, "warmup": warmup}
            run_name = "warmup" if warmup else f"i{label}"
            write_record(out_dir, f"{args.benchmark}-{args.scale_factor}-{args.engine}-{q}-{run_name}", record)

            if res["status"] == "ok":
                summary = f"{res['wall_time_s']:.3f}s  {fmt_bytes(res['max_rss_bytes'])}"
            else:
                summary = f"{res['status']}: {res['error'].splitlines()[0][:100]}"
            progress = "warmup" if warmup else f"{label}/{args.iters}"
            print(f"{q} {progress}  {summary}", file=sys.stderr, flush=True)

if __name__ == "__main__":
    main()
