"""Step 8 - Group surviving files by repository and export.

Stack v3 output shape:

    Each row is a repository: repo_path, repo_id, commit_id, github_metadata,
    num_files, files[] {content_id, content, size_bytes, file_path, language,
    is_vendor, license_type, ...}.

JSONL output is always available; Parquet when the optional `parquet` extra
is installed (one row per repo, files[] as a list-struct column).
"""

from __future__ import annotations

import json
from pathlib import Path

from .models import RepoRecord


def group_by_repo(repo_path: str, records: list, dropped_repo: bool = False,
                  drop_reason: str = "", commit_id: str = "",
                  github_metadata: dict | None = None) -> RepoRecord:
    rr = RepoRecord(
        repo_path=repo_path,
        commit_id=commit_id,
        num_files=len(records),
        files=[r.to_json() for r in records],
        github_metadata=github_metadata or {},
        dropped=dropped_repo,
        drop_reason=drop_reason,
    )
    return rr


def write_jsonl(repos: list[RepoRecord], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        for rr in repos:
            f.write(json.dumps(rr.__dict__, ensure_ascii=False) + "\n")


def write_parquet(repos: list[RepoRecord], out_path: Path) -> None:
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        ("repo_path", pa.string()),
        ("commit_id", pa.string()),
        ("num_files", pa.int64()),
        ("dropped", pa.bool_()),
        ("drop_reason", pa.string()),
        ("github_metadata", pa.string()),   # serialized JSON
        ("files", pa.string()),             # serialized JSON array
    ])
    rows = [
        (rr.repo_path, rr.commit_id, rr.num_files, rr.dropped, rr.drop_reason,
         json.dumps(rr.github_metadata), json.dumps(rr.files))
        for rr in repos
    ]
    table = pa.Table.from_arrays(
        [pa.array(col, type=field.type) for field, col in zip(schema, zip(*rows))],
        schema=schema,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_path)
