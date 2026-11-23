"""Step 7 - Global near-duplicate deduplication (MinHash LSH + connected components).

Stack v3 official parameters:

    MinHash signatures (256 permutations, 5-grams, min. 5 tokens) are indexed
    with LSH; candidate pairs with estimated Jaccard similarity >= 0.7 are
    grouped into duplicate clusters via connected components. Deduplication
    runs across all languages at once (language-agnostic) and candidate pairs
    are verified against their Jaccard estimate to eliminate false positives.

dataflow goes one step further: the production path (execution.py) verifies
every LSH candidate pair with the TRUE Jaccard of the shingle sets
(:func:`shingle_set` / :func:`true_jaccard`), not the MinHash estimate.
The in-memory :func:`dedup_files` below is a small-scale helper only.

Representative selection per cluster (in order): highest stars, highest
forks, permissive license, earliest repository creation.

Requires the optional ``datasketch`` dependency; without it we fall back to
exact SHA-1 dedup only (already applied at unpack time via content addressing).
"""

from __future__ import annotations

import hashlib
from collections import defaultdict

from .models import FileRecord, StageStats

try:
    from datasketch import MinHash, MinHashLSH
    _HAS_DATASKETCH = True
except ImportError:  # pragma: no cover
    _HAS_DATASKETCH = False


def _tokenize(text: str) -> list[bytes]:
    """Character 5-grams over whitespace-normalized text, hashed to u64."""
    norm = " ".join(text.split())
    tok = []
    for i in range(0, max(0, len(norm) - 4)):
        tok.append(int.from_bytes(
            hashlib.blake2b(norm[i:i + 5].encode("utf-8", "replace"), digest_size=8).digest(),
            "little"))
    return tok


def minhash_signature(text: str, permutations: int) -> "MinHash | None":
    if not _HAS_DATASKETCH:
        return None
    toks = _tokenize(text)
    if len(toks) < 5:  # Stack v3: minimum 5 tokens
        return None
    # Explicit scheme: the global pass decodes signatures as uint32 / affine32.
    mh = MinHash(num_perm=permutations, scheme="affine32")
    mh.update_batch([t.to_bytes(8, "little") for t in toks])
    return mh


def shingle_set(text: str):
    """Sorted unique uint64 array of the exact shingles MinHash is built from.

    Same tokenisation as :func:`minhash_signature` (whitespace-normalised
    character 5-grams, blake2b-64), so ``true_jaccard`` measures the very set
    whose Jaccard the signature estimates.
    """
    import numpy as _np
    toks = _tokenize(text)
    return _np.unique(_np.fromiter(toks, dtype=_np.uint64, count=len(toks)))


def true_jaccard(a, b) -> float:
    """Exact Jaccard of two sorted-unique shingle arrays."""
    import numpy as _np
    if len(a) == 0 and len(b) == 0:
        return 1.0
    inter = _np.intersect1d(a, b, assume_unique=True).size
    return inter / (len(a) + len(b) - inter)


# ---------------------------------------------------------------------------
# Representative selection (Stack v3 order)
# ---------------------------------------------------------------------------

REPRESENTATIVE_POLICY = (
    "stars_desc>forks_desc>permissive_license>earliest_created_at"
    ">first_in_sorted_input_path_then_sha1"
)

_PERMISSIVE_WORDS = {"permissive"}


def _is_permissive(value) -> bool:
    if not value:
        return False
    from .licenses import PERMISSIVE_SPDX
    text = str(value).strip()
    return text.lower() in _PERMISSIVE_WORDS or text in PERMISSIVE_SPDX


def _int_or_none(value):
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def load_repo_metadata(path) -> dict:
    """Load ``{repo_path_lower: {stars, forks, created_at, license}}``.

    Accepts ``.jsonl`` (one object per line) or TSV with a header row. The
    repo key column is ``repo_path`` (``owner/name``); a GitHub ``url`` column
    is accepted instead. Missing / empty fields stay ``None`` - nothing is
    invented. Keys are lower-cased because GitHub paths are case-insensitive.
    """
    from pathlib import Path as _Path
    import json as _json
    path = _Path(path)
    rows = []
    with path.open(encoding="utf-8") as stream:
        if path.suffix in (".jsonl", ".ndjson", ".json"):
            rows = [_json.loads(line) for line in stream if line.strip()]
        else:
            header = stream.readline().rstrip("\n").split("\t")
            for line in stream:
                if line.strip():
                    rows.append(dict(zip(header, line.rstrip("\n").split("\t"))))
    meta = {}
    for row in rows:
        key = row.get("repo_path")
        if not key and row.get("url"):
            key = "/".join(str(row["url"]).rstrip("/").split("/")[-2:])
        if not key:
            continue
        meta[key.lower()] = {
            "stars": _int_or_none(row.get("stars")),
            "forks": _int_or_none(row.get("forks")),
            "created_at": (row.get("created_at") or None),
            "license": (row.get("license") or None),
        }
    return meta


def repo_priority_key(meta: dict | None) -> tuple:
    """Sort key, smaller = better representative.

    Order: highest stars, highest forks, permissive license, earliest repo
    creation. Missing values sort last (stars/forks as -1, created_at as
    +inf); remaining ties fall back to input order, applied by the caller.
    """
    meta = meta or {}
    stars = meta.get("stars")
    forks = meta.get("forks")
    created = meta.get("created_at")
    return (
        -(stars if stars is not None else -1),
        -(forks if forks is not None else -1),
        0 if _is_permissive(meta.get("license")) else 1,
        (0, str(created)) if created else (1, ""),
    )


class UnionFind:
    def __init__(self):
        self.parent: dict[int, int] = {}

    def find(self, x: int) -> int:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: int, b: int) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def dedup_files(
    records: list[FileRecord],
    cfg: dict,
    repo_meta: dict | None = None,
) -> tuple[list[FileRecord], StageStats]:
    """Cluster near-duplicates and keep one representative per cluster.

    repo_meta: optional {repo_path: {"stars": int, "forks": int,
    "created_at": str}} used for representative selection.
    """
    stats = StageStats(stage="dedup")
    repo_meta = repo_meta or {}

    if not _HAS_DATASKETCH:
        # Fallback: exact dedup by sha1 (content addressing already collapsed
        # identical blobs, this is a no-op pass-through).
        for rec in records:
            stats.seen += 1
            stats.kept += 1
        return records, stats

    perms = int(cfg.get("minhash_permutations", 256))
    threshold = float(cfg.get("lsh_jaccard_threshold", 0.7))
    verify = bool(cfg.get("verify_jaccard", True))

    sigs: list = []
    idx_map: dict[str, int] = {}
    lsh = MinHashLSH(threshold=threshold, num_perm=perms)

    for rec in records:
        stats.seen += 1
        if rec.sha1 in idx_map:
            stats.drop("exact_duplicate")
            continue
        mh = minhash_signature(rec.content, perms)
        if mh is None:
            # too small to minhash -> keep (Stack v3 keeps short files)
            idx_map[rec.sha1] = len(sigs)
            sigs.append(None)
            continue
        idx_map[rec.sha1] = len(sigs)
        sigs.append(mh)
        lsh.insert(rec.sha1, mh)

    uf = UnionFind()
    order = [r.sha1 for r in records if r.sha1 in idx_map]
    seen_pair: set = set()
    for rec in records:
        i = idx_map.get(rec.sha1)
        if i is None or sigs[i] is None:
            continue
        for other in lsh.query(sigs[i]):
            if other == rec.sha1:
                continue
            pair = (min(rec.sha1, other), max(rec.sha1, other))
            if pair in seen_pair:
                continue
            seen_pair.add(pair)
            if verify:
                j = sigs[i].jaccard(sigs[idx_map[other]])
                if j < threshold:
                    continue  # LSH false positive eliminated
            uf.union(idx_map[rec.sha1], idx_map[other])

    cluster_of: dict[int, int] = {}
    best: dict[int, str] = {}
    sha1_to_repo = {r.sha1: r.repo_path for r in records}
    for sha1, i in idx_map.items():
        root = uf.find(i)
        cluster_of[i] = root
        if root not in best:
            best[root] = sha1
            continue
        cur = best[root]
        if _better(sha1, cur, repo_meta, sha1_to_repo):
            best[root] = sha1

    keep_sha = set(best.values())
    kept: list[FileRecord] = []
    seen_out: set = set()
    for rec in records:
        if rec.sha1 in seen_out:
            continue  # exact duplicate of an already-emitted file
        i = idx_map.get(rec.sha1)
        if i is None:
            continue
        is_rep = rec.sha1 in keep_sha
        is_short = sigs[i] is None  # too small to minhash; kept as-is (v3 keeps short files)
        if is_rep or is_short:
            seen_out.add(rec.sha1)
            kept.append(rec)
            stats.kept += 1
        else:
            stats.drop("near_duplicate")

    return kept, stats


def _meta_of(sha1: str, repo_meta: dict, sha1_to_repo: dict) -> dict:
    rp = sha1_to_repo.get(sha1, "")
    return repo_meta.get(rp, {})


def _better(a: str, b: str, repo_meta: dict, sha1_to_repo: dict) -> bool:
    """Stack v3 representative order: stars -> forks -> permissive -> earliest."""
    ra, rb = _meta_of(a, repo_meta, sha1_to_repo), _meta_of(b, repo_meta, sha1_to_repo)
    return repo_priority_key(ra) < repo_priority_key(rb)  # tie: keep existing
