#!/usr/bin/env python
"""Build training datasets from dataflow's repos.jsonl output.

Subcommands (all stream line-by-line, safe for multi-GB corpora):

    stats       corpus quality stats (repos/files/langs/years/size percentiles)
    filter      subset selection (activity year / languages / min files / vendor)
    flatten     one file per line (file-level JSONL)
    split       repo-level train/val split (no repo leaks across splits)
    to-parquet  official stack-v3-train style parquet (sha1 -> content_id)

Examples:

    python scripts/build_dataset.py stats runs/pilot10/repos.jsonl
    python scripts/build_dataset.py filter runs/full/repos.jsonl \
        --year 2026 --min-files 3 -o active2026.jsonl
    python scripts/build_dataset.py to-parquet active2026.jsonl -o active2026.parquet
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from collections import Counter
from pathlib import Path


def _iter(path: Path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


# ---------------------------------------------------------------- stats

def cmd_stats(args) -> None:
    n_repos = n_files = n_bytes = 0
    langs: Counter = Counter()
    years: Counter = Counter()
    sizes: list[int] = []
    empty_repos = 0
    for repo in _iter(args.input):
        n_repos += 1
        files = repo.get("files", [])
        if not files:
            empty_repos += 1
        n_files += len(files)
        y = repo.get("last_activity_year")
        years[y if y is not None else "unknown"] += 1
        for f in files:
            langs[f.get("language") or "unknown"] += 1
            n_bytes += len(f.get("content", "").encode("utf-8", "replace"))
            if n_files % 50 == 0 and len(sizes) < 200_000:  # reservoir-ish sample
                sizes.append(f.get("size_bytes", 0))

    def pct(p):
        if not sizes:
            return 0
        s = sorted(sizes)
        return s[min(len(s) - 1, int(len(s) * p))]

    print(f"repos:            {n_repos:,}  (empty: {empty_repos})")
    print(f"files:            {n_files:,}")
    print(f"content (UTF-8):  {n_bytes/1e9:.2f} GB")
    if n_files:
        print(f"avg files/repo:   {n_files/n_repos:.1f}")
    if sizes:
        print(f"file size p50/p90/p99: {pct(0.5):,} / {pct(0.9):,} / {pct(0.99):,} B")
    print("\ntop languages:")
    for lang, c in langs.most_common(args.top):
        print(f"  {lang:<24} {c:>9,}  {c/max(n_files,1)*100:5.1f}%")
    print("\nlast_activity_year:")
    for y, c in sorted(years.items(), key=lambda kv: str(kv[0])):
        print(f"  {str(y):<24} {c:>9,}  {c/max(n_repos,1)*100:5.1f}%")


# ---------------------------------------------------------------- filter

def cmd_filter(args) -> None:
    langs = {l.strip() for l in args.langs.split(",")} if args.langs else None
    years = {int(y) for y in args.years.split(",")} if args.years else None
    n_in = n_out = 0
    with open(args.output, "w", encoding="utf-8") as out:
        for repo in _iter(args.input):
            n_in += 1
            y = repo.get("last_activity_year")
            if years is not None and y not in years:
                continue
            files = repo.get("files", [])
            if args.drop_unknown_lang:
                files = [f for f in files if f.get("language")]
            if args.no_vendor:
                files = [f for f in files if not f.get("is_vendor")]
            if args.langs:
                files = [f for f in files if f.get("language") in langs]
            if len(files) < args.min_files:
                continue
            repo["files"] = files
            repo["num_files"] = len(files)
            out.write(json.dumps(repo, ensure_ascii=False) + "\n")
            n_out += 1
    print(f"kept {n_out:,}/{n_in:,} repos -> {args.output}")


# ---------------------------------------------------------------- flatten

def cmd_flatten(args) -> None:
    n = 0
    with open(args.output, "w", encoding="utf-8") as out:
        for repo in _iter(args.input):
            base = {"repo_path": repo["repo_path"],
                    "last_activity_year": repo.get("last_activity_year"),
                    "github_metadata": repo.get("github_metadata", {})}
            for f in repo.get("files", []):
                rec = {**base, **f}
                out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                n += 1
    print(f"wrote {n:,} file rows -> {args.output}")


# ---------------------------------------------------------------- split

def cmd_split(args) -> None:
    rnd = random.Random(args.seed)
    paths = [l.strip() for l in sys.stdin if l.strip()] if args.repo_list else None
    val = set(paths) if paths else None
    train_p = Path(f"{args.prefix}.train.jsonl")
    val_p = Path(f"{args.prefix}.val.jsonl")
    n_tr = n_va = 0
    with open(train_p, "w", encoding="utf-8") as tr, \
            open(val_p, "w", encoding="utf-8") as va:
        for repo in _iter(args.input):
            rp = repo["repo_path"]
            to_val = (rp in val) if val is not None \
                else (rnd.random() < args.val_ratio)
            (va if to_val else tr).write(
                json.dumps(repo, ensure_ascii=False) + "\n")
            n_va += to_val
            n_tr += not to_val
    print(f"train {n_tr:,} -> {train_p}\nval   {n_va:,} -> {val_p}")


# ---------------------------------------------------------------- to-parquet

def cmd_to_parquet(args) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    file_type = pa.struct([
        ("content_id", pa.string()),          # dataflow: sha1
        ("content", pa.string()),
        ("size_bytes", pa.int32()),
        ("file_path", pa.string()),
        ("language", pa.string()),
        ("is_vendor", pa.bool_()),
        ("license_type", pa.string()),
        ("last_activity_year", pa.int16()),   # dataflow extension
    ])
    schema = pa.schema([
        ("repo_path", pa.string()),
        ("repo_id", pa.int64()),              # placeholder until crawler injects
        ("commit_id", pa.string()),
        ("github_metadata", pa.struct([       # official 10-field shape
            ("branch", pa.string()), ("commit_count", pa.int32()),
            ("repo_created_at", pa.string()), ("is_fork", pa.bool_()),
            ("is_org_owned", pa.bool_()), ("forked_from", pa.string()),
            ("stars", pa.int32()), ("forks", pa.int32()),
            ("issues", pa.int32()), ("pull_requests", pa.int32())])),
        ("num_files", pa.int64()),
        ("files", pa.list_(file_type)),
        ("last_activity_year", pa.int16()),
    ])
    gm_defaults = {"string": "", "int32": 0, "bool": False}

    def gm_row(gm: dict) -> dict:
        gm = gm or {}
        out = {}
        for child in schema.field("github_metadata").type:
            out[child.name] = gm.get(
                child.name, gm_defaults[str(child.type)])
        return out

    n = 0
    batch: list[dict] = []

    def flush(writer):
        if batch:
            writer.write_table(pa.Table.from_pylist(batch, schema=schema))
            batch.clear()

    with pq.ParquetWriter(args.output, schema,
                          compression="snappy") as writer:
        for repo in _iter(args.input):
            batch.append({
                "repo_path": repo["repo_path"],
                "repo_id": n,
                "commit_id": repo.get("commit_id", ""),
                "github_metadata": gm_row(repo.get("github_metadata")),
                "num_files": repo.get("num_files", len(repo.get("files", []))),
                "files": [{
                    "content_id": f["sha1"],
                    "content": f["content"],
                    "size_bytes": f["size_bytes"],
                    "file_path": f["file_path"],
                    "language": f.get("language") or "",
                    "is_vendor": bool(f.get("is_vendor")),
                    "license_type": f.get("license_type") or "",
                    "last_activity_year": repo.get("last_activity_year") or 0,
                } for f in repo.get("files", [])],
                "last_activity_year": repo.get("last_activity_year") or 0,
            })
            n += 1
            if len(batch) >= 1000:
                flush(writer)
        flush(writer)
    print(f"wrote {n:,} repos -> {args.output}")


# ---------------------------------------------------------------- cli

def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("stats", help="corpus statistics")
    s.add_argument("input", type=Path)
    s.add_argument("--top", type=int, default=15)
    s.set_defaults(fn=cmd_stats)

    s = sub.add_parser("filter", help="subset selection")
    s.add_argument("input", type=Path)
    s.add_argument("-o", "--output", type=Path, required=True)
    s.add_argument("--years", help="comma list, e.g. 2026 or 2024,2025,2026")
    s.add_argument("--langs", help="comma list, e.g. Python,TypeScript")
    s.add_argument("--min-files", type=int, default=1)
    s.add_argument("--drop-unknown-lang", action="store_true")
    s.add_argument("--no-vendor", action="store_true")
    s.set_defaults(fn=cmd_filter)

    s = sub.add_parser("flatten", help="one file per line")
    s.add_argument("input", type=Path)
    s.add_argument("-o", "--output", type=Path, required=True)
    s.set_defaults(fn=cmd_flatten)

    s = sub.add_parser("split", help="repo-level train/val split")
    s.add_argument("input", type=Path)
    s.add_argument("-o", "--prefix", type=Path, required=True,
                   help="writes <prefix>.train.jsonl / <prefix>.val.jsonl")
    s.add_argument("--val-ratio", type=float, default=0.005)
    s.add_argument("--seed", type=int, default=42)
    s.add_argument("--repo-list", action="store_true",
                   help="read val repo_path list from stdin instead of random")
    s.set_defaults(fn=cmd_split)

    s = sub.add_parser("to-parquet", help="official-style parquet export")
    s.add_argument("input", type=Path)
    s.add_argument("-o", "--output", type=Path, required=True)
    s.set_defaults(fn=cmd_to_parquet)

    args = p.parse_args(argv)
    args.fn(args)


if __name__ == "__main__":
    main()
