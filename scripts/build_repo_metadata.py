#!/usr/bin/env python
"""Normalise repository metadata for dataflow's representative selection.

Writes a TSV with header ``repo_path stars forks created_at license`` that
``dedup.repo_metadata_path`` can point at. Fields that no source provides are
left empty - never guessed.

Known sources (2026-11-11):
  * /mnt/data4/cleaning_data/MANIFEST.tsv (swhkit volume of repos/):
    columns repo_id url stars swh_rev bytes sha256 artifact -> stars only.
  * github_downloads/: only <owner>/<repo>_HEAD.tar.gz, no metadata at all.

    python scripts/build_repo_metadata.py /mnt/data4/cleaning_data/MANIFEST.tsv \
        -o repo_metadata.tsv --coverage-dir /mnt/data4/cleaning_data/github_downloads
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

FIELDS = ("stars", "forks", "created_at", "license")


def _rows(path: Path):
    with path.open(encoding="utf-8") as stream:
        if path.suffix in (".jsonl", ".ndjson"):
            for line in stream:
                if line.strip():
                    yield json.loads(line)
            return
        header = stream.readline().rstrip("\n").split("\t")
        for line in stream:
            if line.strip():
                yield dict(zip(header, line.rstrip("\n").split("\t")))


def _repo_path(row) -> str | None:
    if row.get("repo_path"):
        return row["repo_path"]
    url = row.get("url")
    if url:
        return "/".join(str(url).rstrip("/").removesuffix(".git").split("/")[-2:])
    return None


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("sources", nargs="+", type=Path, help="TSV/JSONL sources; later ones fill gaps only")
    p.add_argument("-o", "--output", type=Path, required=True)
    p.add_argument("--coverage-dir", type=Path,
                   help="github_downloads-style dir; prints how many of its repos got each field")
    args = p.parse_args(argv)

    merged: dict[str, dict] = {}
    for source in args.sources:
        for row in _rows(source):
            key = _repo_path(row)
            if not key:
                continue
            slot = merged.setdefault(key.lower(), {"repo_path": key})
            for field in FIELDS:
                value = row.get(field)
                if value not in (None, "") and slot.get(field) in (None, ""):
                    slot[field] = value
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8") as out:
        out.write("repo_path\t" + "\t".join(FIELDS) + "\n")
        for key in sorted(merged):
            row = merged[key]
            out.write(row["repo_path"] + "\t" + "\t".join(str(row.get(f) or "") for f in FIELDS) + "\n")
    print(f"wrote {len(merged)} repos -> {args.output}", file=sys.stderr)

    if args.coverage_dir:
        repos = set()
        for tar in args.coverage_dir.glob("*/*.tar.gz"):
            name = tar.name.removesuffix(".tar.gz").removesuffix("_HEAD")
            repos.add(f"{tar.parent.name}/{name}".lower())
        hits = [merged[r] for r in repos if r in merged]
        cov = {"repos_in_dir": len(repos), "with_any_row": len(hits)}
        for field in FIELDS:
            cov[f"with_{field}"] = sum(1 for h in hits if h.get(field) not in (None, ""))
        print(json.dumps(cov, indent=2))


if __name__ == "__main__":
    main()
