"""Corpus-level size controls applied after global dedup (Stack v3 step 5 tail).

* Per-language byte budget (``quality.byte_budget``): for each capped language
  the surviving files are visited in a seeded pseudo-random order
  (``blake2b(seed, "budget", sha1)``, ties by input order) and kept until the
  next file would exceed the cap; that file and every later one are dropped
  (reason ``byte_budget``). The result is a uniform, reproducible subsample
  that does not depend on worker timing or metadata.
* Large-repo downsampling (``quality.max_repo_bytes_before_downsample``): a
  repository whose post-dedup retained bytes exceed the threshold keeps each
  file independently with probability ``downsample_keep_ratio``, decided by
  ``blake2b(seed, "downsample", sha1)`` (reason ``repo_downsample``). Same
  content + same seed => same decision on every run.

Downsampling runs first, then the byte budget over what is left.
"""

from __future__ import annotations

import hashlib


def unit_hash(seed: int, purpose: str, sha1: str) -> float:
    """Deterministic value in [0, 1) for (seed, purpose, file content hash)."""
    digest = hashlib.blake2b(f"{seed}:{purpose}:{sha1}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64


def keep_in_downsample(seed: int, sha1: str, keep_ratio: float) -> bool:
    return unit_hash(seed, "downsample", sha1) < keep_ratio


def apply_byte_budget(candidates: list, cap: int) -> tuple[list, int]:
    """candidates: [(rank, *ident, size)]; returns (dropped idents, kept bytes)."""
    candidates.sort()
    total, dropped, over = 0, [], False
    for item in candidates:
        size = item[-1]
        if over or total + size > cap:
            over = True
            dropped.append(item[1:-1])
        else:
            total += size
    return dropped, total
