#!/usr/bin/env python3
"""Extract compact per-file metadata from intermediate shards and final repos.jsonl.

Outputs (no file content):
  ~/dataflow_prep_20261111/funnel/post_quality.jsonl
  ~/dataflow_prep_20261111/funnel/final_files.jsonl
  ~/dataflow_prep_20261111/funnel/shard_repo_stats.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

OUT = Path(os.path.expanduser("~/dataflow_prep_20261111/funnel"))
SHARDS = Path("/mnt/data4/cleaning_data/dataflow_runs/full-rules-20261111/intermediate/shards")
FINAL = Path("/mnt/data4/cleaning_data/dataflow_runs/full-rules-20261111/repos.jsonl")
LARGE_BYTES = 400_000_000  # shards bigger than this are processed with limited concurrency


def rec_row(rec: dict) -> dict:
    return {
        "repo_path": rec.get("repo_path") or "",
        "file_path": rec.get("file_path") or "",
        "sha1": rec.get("sha1") or "",
        "size_bytes": int(rec.get("size_bytes") or 0),
        "language": rec.get("language") or "",
        "is_vendor": bool(rec.get("is_vendor")),
        "drop_reason": rec.get("drop_reason") or "",
    }


def extract_shard(path: str) -> dict:
    with open(path) as f:
        obj = json.load(f)
    inp = obj.get("input") or {}
    res = obj.get("result") or {}
    rows = [rec_row(r) for r in (res.get("records") or [])]
    return {
        "shard": os.path.basename(path),
        "tar": inp.get("path") or res.get("_tar"),
        "compressed_bytes": inp.get("size_bytes"),
        "repo_path": res.get("repo_path"),
        "failed": res.get("failed"),
        "activity": res.get("activity"),
        "dup_within": res.get("dup_within"),
        "stats": res.get("stats") or [],
        "n_records": len(rows),
        "record_bytes": sum(r["size_bytes"] for r in rows),
        "rows": rows,
    }


def write_parts(kind: str, idx: int, rows: list, stats_line: dict | None, parts_dir: Path):
    parts_dir.mkdir(parents=True, exist_ok=True)
    p = parts_dir / f"{kind}_{idx:05d}.jsonl"
    with p.open("w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n")
    if stats_line is not None:
        sp = parts_dir / f"stats_{idx:05d}.jsonl"
        with sp.open("w") as f:
            f.write(json.dumps(stats_line, ensure_ascii=False, separators=(",", ":")) + "\n")


def process_shard(args):
    idx, path = args
    t0 = time.time()
    data = extract_shard(path)
    rows = data.pop("rows")
    stats_line = data
    return idx, rows, stats_line, time.time() - t0, os.path.getsize(path)


def concat_parts(parts_dir: Path, pattern: str, dest: Path):
    files = sorted(parts_dir.glob(pattern))
    with dest.open("w") as out:
        for p in files:
            with p.open() as f:
                for line in f:
                    out.write(line)
    return dest, dest.stat().st_size


def extract_shards(workers: int):
    shards = sorted(str(p) for p in SHARDS.glob("*.json"))
    print(f"shards={len(shards)} workers={workers}", flush=True)
    sizes = [(os.path.getsize(p), p) for p in shards]
    sizes.sort(reverse=True)
    large = [(i, p) for i, (sz, p) in enumerate(sizes) if sz >= LARGE_BYTES]
    small = [(i, p) for i, (sz, p) in enumerate(sizes) if sz < LARGE_BYTES]
    parts_dir = OUT / "meta_parts"
    if parts_dir.exists():
        for p in parts_dir.glob("*"):
            p.unlink()
    parts_dir.mkdir(parents=True, exist_ok=True)

    t0 = time.time()
    done = 0
    nfiles = 0
    nbytes = 0

    def handle(idx, rows, stats_line, elapsed, sz):
        nonlocal done, nfiles, nbytes
        write_parts("pq", idx, rows, stats_line, parts_dir)
        done += 1
        nfiles += len(rows)
        nbytes += stats_line.get("record_bytes") or 0
        if done % 100 == 0 or done == len(shards):
            print(
                f"shard progress {done}/{len(shards)} files={nfiles} "
                f"bytes={nbytes} last_sec={elapsed:.1f} shard_mb={sz/1e6:.1f} "
                f"elapsed={time.time()-t0:.1f}s",
                flush=True,
            )

    # large first, few at a time
    large_workers = min(4, workers, max(1, len(large)))
    if large:
        print(f"processing {len(large)} large shards with {large_workers} workers", flush=True)
        with ProcessPoolExecutor(max_workers=large_workers) as ex:
            futs = [ex.submit(process_shard, item) for item in large]
            for fut in as_completed(futs):
                handle(*fut.result())
    if small:
        print(f"processing {len(small)} small shards with {workers} workers", flush=True)
        with ProcessPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(process_shard, item) for item in small]
            for fut in as_completed(futs):
                handle(*fut.result())

    dest_pq, sz_pq = concat_parts(parts_dir, "pq_*.jsonl", OUT / "post_quality.jsonl")
    dest_st, sz_st = concat_parts(parts_dir, "stats_*.jsonl", OUT / "shard_repo_stats.jsonl")
    print(f"wrote {dest_pq} ({sz_pq} bytes), {dest_st} ({sz_st} bytes), files={nfiles}", flush=True)


def extract_final():
    dest = OUT / "final_files.jsonl"
    t0 = time.time()
    n_repos = 0
    n_files = 0
    nbytes = 0
    with open(FINAL) as f, dest.open("w") as out:
        for line in f:
            obj = json.loads(line)
            n_repos += 1
            repo = obj.get("repo_path") or ""
            for rec in obj.get("files") or []:
                row = rec_row(rec)
                if not row["repo_path"]:
                    row["repo_path"] = repo
                out.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
                n_files += 1
                nbytes += row["size_bytes"]
            if n_repos % 200 == 0:
                print(f"final repos {n_repos} files={n_files} elapsed={time.time()-t0:.1f}s", flush=True)
    print(
        f"wrote {dest} repos={n_repos} files={n_files} bytes={nbytes} "
        f"elapsed={time.time()-t0:.1f}s",
        flush=True,
    )


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--shards-only", action="store_true")
    ap.add_argument("--final-only", action="store_true")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    if not args.final_only:
        extract_shards(args.workers)
    if not args.shards_only:
        extract_final()


if __name__ == "__main__":
    main()
