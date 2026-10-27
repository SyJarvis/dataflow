"""Durable local/NFS output primitives; inputs are never modified."""

from contextlib import contextmanager
import errno
import fcntl
import json
import os
from pathlib import Path
import tempfile


def identity(path: Path) -> dict:
    st = path.stat()
    return {"path": str(path.resolve()), "size_bytes": st.st_size,
            "mtime_ns": st.st_mtime_ns}


@contextmanager
def run_lock(directory: Path):
    directory.mkdir(parents=True, exist_ok=True)
    with (directory / ".lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ValueError(f"another process owns this run: {directory}") from exc
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


@contextmanager
def atomic_text(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".tmp-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            yield stream
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        dir_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            try:
                os.fsync(dir_fd)
            except OSError as exc:
                if exc.errno not in (errno.EINVAL, errno.ENOTSUP):
                    raise
        finally:
            os.close(dir_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_json(path: Path, value):
    with atomic_text(path) as stream:
        json.dump(value, stream, ensure_ascii=False, sort_keys=True, indent=1)
        stream.write("\n")


def atomic_jsonl(path: Path, rows):
    with atomic_text(path) as stream:
        for row in rows:
            stream.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
