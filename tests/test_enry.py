"""EnryDetector: go-enry CLI integration with builtin fallback."""

import json
import os
import stat

import pytest

from dataflow.languages import EnryDetector, detect_language


def _write_fake_enry(path, body):
    """Create an executable fake enry CLI script."""
    script = f"#!/usr/bin/env python3\n{body}\n"
    path.write_text(script, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    return str(path)


# --- CLI path: parsing, vendored flag, basename preservation ---------------

def test_cli_json_is_parsed(tmp_path):
    """Valid `-json` output is used directly (vendored flag honoured)."""
    enry = _write_fake_enry(
        tmp_path / "enry",
        'print(\'{"language":"Python","vendored":true}\')',
    )
    det = EnryDetector(enry)
    # builtin fallback would return ("Python", False); True proves CLI path
    assert det.detect("src/app/test.py", "def f(): pass\n") == ("Python", True)


def test_cli_receives_original_basename(tmp_path):
    """The temp file keeps the original basename (enry needs the extension)."""
    record = tmp_path / "args.json"
    enry = _write_fake_enry(
        tmp_path / "enry",
        f"import json,sys\n"
        f"json.dump(sys.argv[1:], open({str(record)!r}, 'w'))\n"
        'print(\'{"language":"Python","vendored":false}\')',
    )
    det = EnryDetector(enry)
    assert det.detect("a/b/c/main.rs", "fn main() {}") == ("Python", False)
    args = json.loads(record.read_text())
    # recorded argv[1:] = ["-json", <path>]
    assert len(args) == 2 and args[0] == "-json"
    assert args[1].endswith("main.rs")
    # full relative path structure is preserved for vendor detection
    assert os.path.join("a", "b", "c", "main.rs") in args[1]


def test_cli_tmp_file_is_cleaned_up(tmp_path):
    """TemporaryDirectory is removed after the CLI call, success or failure."""
    record = tmp_path / "seen_paths.txt"
    enry = _write_fake_enry(
        tmp_path / "enry",
        f"import sys\n"
        f"open({str(record)!r}, 'a').write(sys.argv[2] + '\\n')\n"
        'print(\'{"language":"Python","vendored":false}\')',
    )
    det = EnryDetector(enry)
    det.detect("x/y/test.py", "pass")
    det.detect("x/y/other.py", "pass")
    seen = record.read_text().splitlines()
    assert len(seen) == 2, "fake enry must have run successfully twice"
    for p in seen:
        assert not os.path.exists(p), f"residual temp file: {p}"
        assert not os.path.exists(os.path.dirname(p))


# --- Fallback paths --------------------------------------------------------

def test_empty_bin_uses_builtin_fallback():
    det = EnryDetector("")
    assert det.bin is None
    assert det.detect("src/main.go", "package main\n") == ("Go", False)
    assert det.detect("lib/a.js", "var x = 1;\n") == ("JavaScript", False)


def test_cli_exit_failure_falls_back(tmp_path):
    """Non-zero exit / empty stdout degrades to the builtin table."""
    enry = _write_fake_enry(tmp_path / "enry", "import sys\nsys.exit(1)")
    det = EnryDetector(enry)
    assert det.detect("test.py", "def f(): pass\n") == detect_language("test.py", "def f(): pass\n")


def test_cli_invalid_json_falls_back(tmp_path):
    enry = _write_fake_enry(tmp_path / "enry", "print('not json')")
    det = EnryDetector(enry)
    assert det.detect("test.py", "def f(): pass\n") == ("Python", False)


def test_cli_empty_language_falls_back(tmp_path):
    """enry returns `{}` for unknown files; fall back instead of ('', ...)."""
    enry = _write_fake_enry(tmp_path / "enry", "print('{}')")
    det = EnryDetector(enry)
    assert det.detect("Dockerfile", "FROM scratch\n") == ("Dockerfile", False)


def test_missing_bin_falls_back(tmp_path):
    det = EnryDetector(str(tmp_path / "does-not-exist"))
    assert det.detect("test.py", "def f(): pass\n") == ("Python", False)


@pytest.mark.parametrize("name", ["test.py", "lib/mod.rs"])
def test_content_truncated_to_64kb(tmp_path, name):
    """Only the first 64KB of content is written to the temp file."""
    record = tmp_path / "sizes.txt"
    enry = _write_fake_enry(
        tmp_path / "enry",
        f"import os, sys\n"
        f"open({str(record)!r}, 'a').write(str(os.path.getsize(sys.argv[2])) + '\\n')\n"
        'print(\'{"language":"Python","vendored":false}\')',
    )
    det = EnryDetector(enry)
    det.detect(name, "x" * (200 * 1024))
    assert record.read_text().splitlines() == [str(64 * 1024)]
