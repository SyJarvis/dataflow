"""Repository activity-year annotation (content-evidence based).

Why: GitHub snapshot tarballs overwrite every file's mtime with the packaging
time, so "last activity year" cannot be read from the tar. Instead we scan
license/readme/changelog/version-style text files for year mentions
(copyright lines, release notes, "last updated" markers) and take the max
year per repository as ``last_activity_year``.

This is a cheap, best-effort signal (a repo with no year evidence anywhere
gets ``None``). The authoritative source is GitHub's ``pushed_at`` metadata,
which the crawler should inject into ``github_metadata`` when available.

Evidence thresholds follow the pilot analysis: ~70% of repos carry a 2026
marker in their LICENSE/README/CHANGELOG.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

_YEAR_RE = re.compile(r"\b(20[0-3]\d)\b")
_EVIDENCE_NAME_RE = re.compile(
    r"(?i)(license|licence|copying|readme|changelog|changes|news|history|"
    r"version|release|notice|authors|contributors)")
_MAX_SCAN_BYTES = 20_000
_MAX_FILE_BYTES = 200_000


def file_years(path: str, content: str) -> set[int]:
    """Year mentions found in an evidence file."""
    if len(content) > _MAX_FILE_BYTES:
        return set()
    name = path.rsplit("/", 1)[-1]
    if not _EVIDENCE_NAME_RE.search(name):
        return set()
    head = content[:_MAX_SCAN_BYTES]
    return {int(y) for y in _YEAR_RE.findall(head)}


def repo_activity_year(files: Iterable) -> tuple[int | None, int]:
    """Max evidence year across a repo's files.

    files: iterable of FileRecord-like objects with .file_path and .content.
    Returns (last_activity_year or None, n_evidence_files).
    """
    best: int | None = None
    n_ev = 0
    for rec in files:
        ys = file_years(rec.file_path, rec.content)
        if ys:
            n_ev += 1
            m = max(ys)
            if best is None or m > best:
                best = m
    return best, n_ev
