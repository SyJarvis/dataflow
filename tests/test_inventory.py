import gzip
import io
import json
import os
from pathlib import Path
import tarfile

import pytest

from dataflow.inventory import inspect_header, validate_archive, validate_inputs
from dataflow.storage import run_lock


def archive(root, name="tiny", data=b"print('hello')\n"):
    path = root / "owner" / f"{name}_HEAD.tar.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as stream:
        info = tarfile.TarInfo("snapshot/a.py")
        info.size = len(data)
        stream.addfile(info, io.BytesIO(data))
    return path


def check(path, limit=1024**2, timeout=10):
    return validate_archive((inspect_header(path), timeout, limit))


def test_small_gzip_is_valid_and_sha256_covers_compressed_input(tmp_path):
    import hashlib
    path = archive(tmp_path)
    assert path.stat().st_size < 1024
    row = check(path)
    assert row["status"] == "valid"
    assert row["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert row["regular_files"] == row["tar_members"] == 1
    assert row["declared_file_bytes"] == len(b"print('hello')\n")


def test_404_is_classified_by_body_not_size(tmp_path):
    path = archive(tmp_path)
    path.write_bytes(b"404: Not Found")
    assert inspect_header(path)["status"] == "http_404"
    path.write_bytes(b"some text here")
    assert inspect_header(path)["status"] == "not_gzip"


@pytest.mark.parametrize("damage", ["crc", "truncate", "deflate"])
def test_gzip_damage_is_not_accepted_even_after_tar_end(tmp_path, damage):
    path = archive(tmp_path)
    body = bytearray(path.read_bytes())
    if damage == "crc":
        body[-8] ^= 0xFF
    elif damage == "truncate":
        body = body[:-5]
    else:
        body[len(body) // 2] ^= 0xFF
    path.write_bytes(body)
    assert check(path)["status"] in {"invalid_gzip", "invalid_tar"}


def test_valid_gzip_with_invalid_tar_is_rejected(tmp_path):
    path = archive(tmp_path)
    path.write_bytes(gzip.compress(b"this is not a tar archive"))
    assert check(path)["status"] == "invalid_tar"


def test_expansion_limit_is_separate_from_corruption(tmp_path):
    path = archive(tmp_path, data=b"a" * 20000)
    assert check(path, limit=1024)["status"] == "expanded_limit"


def test_time_limit_is_separate_from_corruption(tmp_path, monkeypatch):
    from dataflow import inventory
    path = archive(tmp_path)
    def timeout(*args, **kwargs):
        raise TimeoutError("synthetic slow source")
    monkeypatch.setattr(inventory.HashingInput, "read", timeout)
    assert check(path)["status"] == "timeout"


def test_changed_input_is_not_validated_using_stale_header(tmp_path):
    path = archive(tmp_path)
    header = inspect_header(path)
    path.write_bytes(b"changed")
    assert validate_archive((header, 10, 1024**2))["status"] == "input_changed"


def test_inventory_resume_recovers_torn_tail_without_duplicate_results(tmp_path):
    root, output = tmp_path / "inputs", tmp_path / "validation"
    archive(root)
    bad = root / "owner" / "bad_HEAD.tar.gz"
    bad.write_bytes(b"404: Not Found")
    first = validate_inputs(root, output, workers=1)
    assert first["status_counts"] == {"http_404": 1, "valid": 1}
    assert first["complete"] is True
    before = (output / "inventory.jsonl").read_bytes()
    journal = output / "results.jsonl"
    committed = journal.read_bytes()
    with journal.open("ab") as stream:
        stream.write(b'{"partial":')
    resumed = validate_inputs(root, output, workers=2, resume=True)
    assert resumed["candidates_completed"] == 1
    assert journal.read_bytes() == committed
    assert (output / "inventory.jsonl").read_bytes() == before
    with pytest.raises(ValueError, match="exists"):
        validate_inputs(root, output)
    with pytest.raises(ValueError, match="settings/code changed"):
        validate_inputs(root, output, timeout_sec=1, resume=True)


def test_headers_only_are_candidates_until_full_validation(tmp_path):
    root, output = tmp_path / "inputs", tmp_path / "validation"
    archive(root)
    summary = validate_inputs(root, output, headers_only=True)
    assert summary["complete"] is False
    assert summary["status_counts"] == {"gzip_candidate": 1}
    summary = validate_inputs(root, output, resume=True, workers=1)
    assert summary["status_counts"] == {"valid": 1}


def test_run_lock_prevents_two_writers(tmp_path):
    with run_lock(tmp_path):
        with pytest.raises(ValueError, match="another process"):
            with run_lock(tmp_path):
                pytest.fail("must not acquire an already-owned lock")
