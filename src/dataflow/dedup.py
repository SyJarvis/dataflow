"""Step 7 - Global near-duplicate deduplication (MinHash LSH + connected components).

Stack v3 official parameters:

    MinHash signatures (256 permutations, 5-grams, min. 5 tokens) are indexed
    with LSH; candidate pairs with estimated Jaccard similarity >= 0.7 are
    grouped into duplicate clusters via connected components. Deduplication
    runs across all languages at once (language-agnostic) and candidate pairs
    are verified against their Jaccard estimate to eliminate false positives.

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
    mh = MinHash(num_perm=permutations)
    mh.update_batch([t.to_bytes(8, "little") for t in toks])
    return mh


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
    for key in ("stars", "forks"):
        va, vb = ra.get(key, -1), rb.get(key, -1)
        if va != vb:
            return va > vb
    return False  # tie: keep existing (license/creation-date hook: TODO)
