"""Read-only archive acceptance: header inventory, gzip CRC, and tar members.

Limits are separate statuses, never reported as corrupt or successfully valid.
Only status=valid is accepted by a run configured with input_manifest.
"""

from collections import Counter
import gzip
import hashlib
import json
import multiprocessing as mp
import os
from pathlib import Path
import signal
import tarfile
import time
import zlib

from .storage import atomic_json, atomic_jsonl, identity, run_lock


CHUNK = 1024 * 1024


class ExpandedLimitError(Exception):
    pass


class HashingInput:
    def __init__(self, stream):
        self.stream = stream
        self.digest = hashlib.sha256()

    def read(self, size=-1):
        block = self.stream.read(size)
        self.digest.update(block)
        return block


class BoundedInput:
    def __init__(self, stream, limit):
        self.stream, self.limit, self.count = stream, limit, 0

    def read(self, size=-1):
        block = self.stream.read(size)
        self.count += len(block)
        if self.limit and self.count > self.limit:
            raise ExpandedLimitError(f"expanded data exceeds {self.limit} bytes")
        return block


def _timeout(_signum, _frame):
    raise TimeoutError("archive validation time limit exceeded")


def inspect_header(path: Path):
    row = {"path": str(path.resolve()), "size_bytes": None, "mtime_ns": None}
    try:
        row.update(identity(path))
        with path.open("rb") as source:
            head = source.read(64)
        if identity(path) != {k: row[k] for k in ("path", "size_bytes", "mtime_ns")}:
            row.update(status="input_changed", error="source changed during header inspection")
        elif head.startswith(b"\x1f\x8b"):
            row["status"] = "gzip_candidate"
        elif row["size_bytes"] == 14 and head == b"404: Not Found":
            row["status"] = "http_404"
        else:
            row["status"] = "not_gzip"
    except OSError as exc:
        row.update(status="read_error", error=f"{type(exc).__name__}: {exc}")
    return row


def validate_archive(args):
    row, timeout_sec, expanded_limit = args
    row = dict(row)
    expected = {key: row[key] for key in ("path", "size_bytes", "mtime_ns")}
    path = Path(row["path"])
    start = time.monotonic()
    prior = signal.signal(signal.SIGALRM, _timeout)
    signal.setitimer(signal.ITIMER_REAL, timeout_sec)
    bounded = None
    try:
        if identity(path) != expected:
            row.update(status="input_changed", error="source changed since header inventory")
            return row
        members = regular = declared_bytes = 0
        with path.open("rb") as raw:
            hashed = HashingInput(raw)
            with gzip.GzipFile(fileobj=hashed, mode="rb") as decoded:
                bounded = BoundedInput(decoded, expanded_limit)
                with tarfile.open(fileobj=bounded, mode="r|") as archive:
                    for member in archive:
                        members += 1
                        if member.isfile():
                            regular += 1
                            declared_bytes += member.size
                            with archive.extractfile(member) as content:
                                while content.read(CHUNK):
                                    pass
                # Tar readers stop at tar end markers, before the gzip trailer.
                # Draining the underlying decoder is required for CRC/ISIZE.
                while bounded.read(CHUNK):
                    pass
            while hashed.read(CHUNK):
                pass
        if identity(path) != expected:
            row.update(status="input_changed", error="source changed during validation")
        else:
            row.update(status="valid", sha256=hashed.digest.hexdigest(),
                       tar_members=members, regular_files=regular,
                       declared_file_bytes=declared_bytes)
    except TimeoutError as exc:
        row.update(status="timeout", error=str(exc))
    except ExpandedLimitError as exc:
        row.update(status="expanded_limit", error=str(exc))
    except (gzip.BadGzipFile, EOFError, zlib.error) as exc:
        row.update(status="invalid_gzip", error=f"{type(exc).__name__}: {exc}")
    except tarfile.TarError as exc:
        row.update(status="invalid_tar", error=f"{type(exc).__name__}: {exc}")
    except OSError as exc:
        row.update(status="read_error", error=f"{type(exc).__name__}: {exc}")
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, prior)
        row["elapsed_sec"] = round(time.monotonic() - start, 3)
        if bounded is not None:
            row["expanded_bytes_read"] = bounded.count
    return row


def _recover_results(path):
    results = {}
    if not path.exists():
        return results
    with path.open("r+b") as stream:
        while True:
            offset = stream.tell()
            line = stream.readline()
            if not line:
                break
            if not line.endswith(b"\n"):
                # Only an incomplete final record may be discarded on resume.
                stream.truncate(offset)
                stream.flush()
                os.fsync(stream.fileno())
                break
            row = json.loads(line)
            if row["path"] in results:
                raise ValueError(f"duplicate validation result: {row['path']}")
            results[row["path"]] = row
    return results


def validate_inputs(input_dir, output_dir, *, workers=4, timeout_sec=1800,
                    max_expanded_bytes=256 * 1024**3, resume=False, headers_only=False):
    from .runner import discover_tarballs

    root, output = Path(input_dir).resolve(), Path(output_dir).resolve()
    if not root.is_dir():
        raise ValueError(f"input directory does not exist: {root}")
    if output == root or output.is_relative_to(root):
        raise ValueError("validation output must be outside the source directory")
    if workers < 1 or timeout_sec <= 0 or max_expanded_bytes < 1:
        raise ValueError("workers, timeout and expanded-byte limit must be positive")
    settings = {"schema_version": 1, "input_dir": str(root),
                "timeout_sec": timeout_sec, "max_expanded_bytes": max_expanded_bytes,
                "validator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    with run_lock(output):
        settings_path, headers_path = output / "settings.json", output / "headers.jsonl"
        if not resume and any(p.name != ".lock" for p in output.iterdir()):
            raise ValueError("validation run exists; use --resume or a new output directory")
        if resume:
            if not settings_path.exists() or not headers_path.exists():
                raise ValueError("no complete header checkpoint to resume")
            if json.loads(settings_path.read_text()) != settings:
                raise ValueError("validation settings/code changed; use a new output directory")
            headers = [json.loads(line) for line in headers_path.read_text().splitlines()]
        else:
            headers = [inspect_header(path) for path in discover_tarballs(root)]
            if not headers:
                raise ValueError("no archives discovered")
            atomic_json(settings_path, settings)
            atomic_jsonl(headers_path, headers)
        results_path = output / "results.jsonl"
        results = _recover_results(results_path)
        candidates = {r["path"]: r for r in headers if r["status"] == "gzip_candidate"}
        if not results.keys() <= candidates.keys():
            raise ValueError("results contain paths outside the frozen header inventory")
        for path, row in results.items():
            if any(row[k] != candidates[path][k] for k in ("size_bytes", "mtime_ns")):
                raise ValueError(f"validation checkpoint identity mismatch: {path}")
        for row in headers:
            if row["size_bytes"] is not None:
                if identity(Path(row["path"])) != {k: row[k] for k in ("path", "size_bytes", "mtime_ns")}:
                    raise ValueError(f"input changed since inventory; start a new run: {row['path']}")

        def snapshot(complete):
            rows = [results.get(row["path"], row) for row in headers]
            summary = {**settings, "complete": complete, "workers": workers,
                       "archives": len(rows), "compressed_bytes": sum(r["size_bytes"] or 0 for r in rows),
                       "status_counts": dict(sorted(Counter(r["status"] for r in rows).items())),
                       "candidates_completed": len(results), "candidates_total": len(candidates)}
            atomic_json(output / "summary.json", summary)
            return rows, summary

        rows, summary = snapshot(False)
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        if headers_only:
            atomic_jsonl(output / "inventory.jsonl", rows)
            return summary
        # Small archives first; the >=1GiB queue is deliberately serialized below.
        pending = sorted((row for row in candidates.values() if row["path"] not in results),
                         key=lambda row: (row["size_bytes"], row["path"]))
        small = [row for row in pending if row["size_bytes"] < 1024**3]
        large = [row for row in pending if row["size_bytes"] >= 1024**3]
        with results_path.open("a", encoding="utf-8") as journal:
            for queue, concurrency in ((small, workers), (large, 1)):
                if not queue:
                    continue
                print(f"[validate] queue={len(queue)} workers={concurrency}", flush=True)
                with mp.get_context("fork").Pool(concurrency) as pool:
                    for row in pool.imap_unordered(validate_archive,
                            ((r, timeout_sec, max_expanded_bytes) for r in queue), chunksize=1):
                        journal.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
                        journal.flush()
                        os.fsync(journal.fileno())
                        results[row["path"]] = row
                        if len(results) % 50 == 0:
                            _, summary = snapshot(False)
                            print(f"[validate] {len(results)}/{len(candidates)} {summary['status_counts']}", flush=True)
        rows, summary = snapshot(False)
        atomic_jsonl(output / "inventory.jsonl", rows)
        rows, summary = snapshot(True)
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True), flush=True)
        return summary
