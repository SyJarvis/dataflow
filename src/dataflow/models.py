"""Shared data structures for the dataflow pipeline.

Each stage consumes and emits these records; intermediate artifacts are JSONL
so runs are inspectable and resumable (inspired by DataForge's runs/ layout).
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Optional


@dataclass
class FileRecord:
    """One file inside one repository snapshot (post-unpack)."""

    repo_path: str            # "owner/name"
    file_path: str            # path relative to repo root
    size_bytes: int           # original byte size
    sha1: str                 # content address (exact dedup happens implicitly)
    content: str = ""         # decoded text (empty if stored in content store)
    language: str = ""        # go-enry language, "" if undetected
    is_vendor: bool = False   # vendored/third-party per language detector
    license_type: str = ""    # permissive | no_license | non_permissive (filled by step 3)
    drop_reason: str = ""     # set by whichever stage dropped this file

    def to_json(self) -> dict:
        return asdict(self)

    @classmethod
    def from_json(cls, d: dict) -> "FileRecord":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


@dataclass
class RepoRecord:
    """One repository row in the final grouped output (Stack v3 style)."""

    repo_path: str
    commit_id: str = ""
    num_files: int = 0
    files: list[dict] = field(default_factory=list)       # FileRecord.to_json() dicts
    github_metadata: dict = field(default_factory=dict)   # stars/forks/... if available
    dropped: bool = False
    drop_reason: str = ""
    last_activity_year: Optional[int] = None              # content-evidence based (activity.py)
    evidence_files: int = 0


@dataclass
class StageStats:
    """Per-stage counters, aggregated into runs/<name>/report.json."""

    stage: str
    seen: int = 0
    kept: int = 0
    dropped: int = 0
    reasons: dict = field(default_factory=dict)

    def drop(self, reason: str) -> None:
        self.dropped += 1
        self.reasons[reason] = self.reasons.get(reason, 0) + 1
