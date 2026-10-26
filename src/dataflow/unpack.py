"""Step 1 - Unpack repository snapshot tarballs with download-time drop rules.

The Stack v3 rules (official, from the dataset card "Data collection"):

    * Files larger than 5 MB are dropped.
    * Binary files (files containing a null byte) are dropped.
    * Symlinks and non-regular files are dropped.
    * Repositories are capped at 1,000,000 files.

Input  : ``<user>/<repo>_HEAD.tar.gz`` snapshots (github_downloads layout).
Output : a stream of FileRecord with sha1 content addressing already applied,
         so identical blobs naturally collapse to one content-store entry.
"""

from __future__ import annotations

import hashlib
import io
import tarfile
from collections.abc import Iterator
from pathlib import Path

from .models import FileRecord, StageStats


def repo_path_from_tarball(tar_path: Path) -> str:
    """``.../a1exlism/claude-code-cmd-fix-thinking-error_HEAD.tar.gz`` ->
    ``a1exlism/claude-code-cmd-fix-thinking-error``."""
    name = tar_path.name
    for suffix in ("_HEAD.tar.gz", ".tar.gz", ".tgz", "_HEAD.tar", ".tar"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
            break
    owner = tar_path.parent.name
    return f"{owner}/{name}"


def iter_tarball_files(
    tar_path: Path,
    *,
    max_file_bytes: int,
    max_repo_files: int,
    drop_binary: bool,
    drop_symlinks: bool,
) -> tuple[Iterator[FileRecord], StageStats]:
    """Stream files out of one tarball, applying the drop rules.

    Returns (file iterator, stats). The whole repo is dropped (empty iterator)
    if the file count exceeds ``max_repo_files``.
    """
    stats = StageStats(stage="unpack")

    def gen():
        with tarfile.open(tar_path, "r:*") as tf:
            members = []
            while True:
                m = tf.next()
                if m is None:
                    break
                members.append(m)
                if len(members) > max_repo_files:
                    stats.drop("repo_exceeds_max_files")
                    return
            for m in members:
                stats.seen += 1
                if not m.isfile():
                    if drop_symlinks or m.issym():
                        stats.drop("symlink_or_nonregular")
                    continue
                if m.size > max_file_bytes:
                    stats.drop("oversize_file")
                    continue
                f = tf.extractfile(m)
                if f is None:
                    stats.drop("nonregular")
                    continue
                data = f.read()
                if drop_binary and b"\x00" in data[:65536]:
                    stats.drop("binary_null_byte")
                    continue
                # strip leading "./" and any single top-level dir wrapper
                inner = _normalize_member_name(m.name)
                if inner is None:
                    stats.drop("empty_name")
                    continue
                yield FileRecord(
                    repo_path=repo_path_from_tarball(tar_path),
                    file_path=inner,
                    size_bytes=m.size,
                    sha1=hashlib.sha1(data).hexdigest(),
                    content="",
                )
                stats.kept += 1
                data = None  # free

    return gen(), stats


_MEMBER_STRIP = ("./", "/")


def _normalize_member_name(name: str) -> str | None:
    for p in _MEMBER_STRIP:
        while name.startswith(p):
            name = name[len(p):]
    return name or None
