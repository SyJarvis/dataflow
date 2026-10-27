"""Parallel runner regressions using only tiny, synthetic repository archives."""

import copy
import io
import json
import multiprocessing as mp
import sys
import tarfile
from pathlib import Path

import pytest
import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from dataflow import runner


@pytest.fixture
def cfg():
    default = Path(__file__).resolve().parents[1] / "configs" / "default.yaml"
    config = copy.deepcopy(yaml.safe_load(default.read_text(encoding="utf-8")))
    # Isolate unpacking from optional dependencies and unrelated cleaning rules.
    config["run"]["steps"].update(
        quality=False, notebooks=False, dedup=False, licenses=False, pii=False
    )
    config["languages"]["go_enry_bin"] = ""
    config["unpack"].update(max_repo_files=100, max_file_bytes=32)
    return config


def _member(name, data=b"", kind=tarfile.REGTYPE, linkname=""):
    info = tarfile.TarInfo(name)
    info.type = kind
    info.linkname = linkname
    info.size = len(data) if info.isfile() else 0
    return info, data


def _tar(tmp_path, members, name="repo"):
    path = tmp_path / "owner" / f"{name}_HEAD.tar.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        for info, data in members:
            archive.addfile(info, io.BytesIO(data) if info.isfile() else None)
    return path


def _unpack_stats(result):
    stats = [s for s in result["stats"] if s["stage"] == "unpack"]
    assert len(stats) == 1
    return stats[0]


def _jsonl(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def test_worker_keeps_repository_at_exact_member_cap(tmp_path, cfg):
    cfg["unpack"]["max_repo_files"] = 2
    tar = _tar(tmp_path, [
        _member("snapshot/first.py", b"print('first')\n"),
        # The per-file size boundary is inclusive too.
        _member("snapshot/second.txt", b"x" * 32),
    ])

    result = runner._clean_one_tar((str(tar), cfg))

    assert result["failed"] is None
    assert not result.get("drop_reason")
    assert result["repo_path"] == "owner/repo"
    assert [f["file_path"] for f in result["records"]] == ["first.py", "second.txt"]
    assert [f["content"] for f in result["records"]] == ["print('first')\n", "x" * 32]
    assert _unpack_stats(result) == {
        "stage": "unpack", "seen": 2, "kept": 2, "dropped": 0, "reasons": {}
    }


@pytest.mark.parametrize("middle", [
    _member("snapshot/second.py", b"print('second')\n"),
    _member("snapshot/directory", kind=tarfile.DIRTYPE),
    _member("snapshot/symbolic", kind=tarfile.SYMTYPE, linkname="first.py"),
    _member("snapshot/hard", kind=tarfile.LNKTYPE, linkname="snapshot/first.py"),
    _member("snapshot/pipe", kind=tarfile.FIFOTYPE),
    _member("snapshot/oversize.txt", b"x" * 33),
    _member("snapshot/binary.txt", b"binary\x00text"),
    _member("./", b"empty path"),
], ids=["regular", "directory", "symlink", "hardlink", "fifo", "oversize", "binary", "empty-name"])
def test_worker_drops_whole_repo_above_cap_even_with_filtered_members(
    tmp_path, cfg, monkeypatch, middle
):
    cfg["unpack"]["max_repo_files"] = 2
    tar = _tar(tmp_path, [
        _member("snapshot/first.py", b"print('first')\n"),
        middle,
        _member("snapshot/last.py", b"print('last')\n"),
    ])

    def unexpected_language_stage(*args, **kwargs):
        pytest.fail("an over-cap repository must not reach downstream stages")

    monkeypatch.setattr(runner, "EnryDetector", unexpected_language_stage)
    result = runner._clean_one_tar((str(tar), cfg))

    assert result["failed"] is None
    assert result["drop_reason"] == "repo_exceeds_max_files"
    assert result["records"] == []
    assert result["sigs"] == []
    assert result["activity"] is None
    stats = _unpack_stats(result)
    assert stats["seen"] == 3
    assert stats["kept"] == 0
    assert stats["reasons"]["repo_exceeds_max_files"] == 1
    assert [s["stage"] for s in result["stats"]] == ["unpack"]


@pytest.mark.parametrize("drop_symlinks", [True, False])
def test_worker_counts_and_never_extracts_any_nonregular_member(
    tmp_path, cfg, monkeypatch, drop_symlinks
):
    cfg["unpack"]["drop_symlinks"] = drop_symlinks
    tar = _tar(tmp_path, [
        _member("snapshot/real.py", b"print('real')\n"),
        _member("snapshot/directory", kind=tarfile.DIRTYPE),
        _member("snapshot/symbolic", kind=tarfile.SYMTYPE, linkname="real.py"),
        _member("snapshot/hard", kind=tarfile.LNKTYPE, linkname="snapshot/real.py"),
        _member("snapshot/pipe", kind=tarfile.FIFOTYPE),
    ])
    extracted = []
    extractfile = tarfile.TarFile.extractfile

    def regular_only(archive, member, *args, **kwargs):
        assert member.isfile(), f"must not extract nonregular member: {member.name}"
        extracted.append(member.name)
        return extractfile(archive, member, *args, **kwargs)

    monkeypatch.setattr(tarfile.TarFile, "extractfile", regular_only)
    result = runner._clean_one_tar((str(tar), cfg))

    assert result["failed"] is None
    assert extracted == ["snapshot/real.py"]
    assert [f["file_path"] for f in result["records"]] == ["real.py"]
    assert _unpack_stats(result) == {
        "stage": "unpack", "seen": 5, "kept": 1, "dropped": 4,
        "reasons": {"symlink_or_nonregular": 4},
    }


@pytest.mark.parametrize("drop_binary", [True, False])
def test_worker_preserves_unpack_filter_statistics_and_binary_switch(
    tmp_path, cfg, drop_binary
):
    cfg["unpack"]["drop_binary"] = drop_binary
    # At the repository cap, even filtered members count toward the boundary.
    cfg["unpack"]["max_repo_files"] = 5
    tar = _tar(tmp_path, [
        _member("snapshot/kept.py", b"print('kept')\n"),
        _member("snapshot/oversize.txt", b"x" * 33),
        _member("snapshot/binary.txt", b"binary\x00text"),
        _member("./", b"empty path"),
        _member("snapshot/directory", kind=tarfile.DIRTYPE),
    ])

    result = runner._clean_one_tar((str(tar), cfg))

    assert result["failed"] is None
    assert not result.get("drop_reason")
    reasons = {"oversize_file": 1, "empty_name": 1, "symlink_or_nonregular": 1}
    paths = ["kept.py"]
    if drop_binary:
        reasons["binary_null_byte"] = 1
    else:
        paths.append("binary.txt")
        assert result["records"][1]["content"] == "binary\x00text"
    assert [f["file_path"] for f in result["records"]] == paths
    assert _unpack_stats(result) == {
        "stage": "unpack", "seen": 5, "kept": len(paths),
        "dropped": sum(reasons.values()), "reasons": reasons,
    }
    languages = next(s for s in result["stats"] if s["stage"] == "languages")
    assert languages["seen"] == languages["kept"] == len(paths)


@pytest.mark.skipif("fork" not in mp.get_all_start_methods(), reason="runner requires fork")
def test_pipeline_aggregates_repo_drop_without_output_and_resumes_once(tmp_path, cfg):
    input_dir = tmp_path / "input"
    normal = _tar(input_dir, [
        _member("snapshot/first.py", b"print('normal first')\n"),
        _member("snapshot/second.py", b"print('normal second')\n"),
    ], name="normal")
    over_cap = _tar(input_dir, [
        _member("snapshot/partial.py", b"print('must not leak')\n"),
        _member("snapshot/directory", kind=tarfile.DIRTYPE),
        _member("snapshot/last.py", b"print('over the cap')\n"),
    ], name="over-cap")
    cfg["unpack"]["max_repo_files"] = 2
    cfg["run"].update(
        input_dir=str(input_dir), output_dir="runs", name="runner-regression",
        workers=2, max_tarballs=0,
    )
    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.safe_dump(cfg), encoding="utf-8")
    out_dir = tmp_path / "runs" / "runner-regression"

    report = runner.run_pipeline(config_path)

    assert report["workers"] == 2
    assert report["tarballs_seen"] == report["tarballs_processed"] == 2
    assert report["tarballs_failed"] == 0
    assert report["tarballs_repo_dropped"] == 1
    assert report["repos_out"] == 1
    assert report["files_out"] == 2
    assert report["stages"]["unpack"] == {
        "stage": "unpack", "seen": 5, "kept": 2, "dropped": 2,
        "reasons": {"symlink_or_nonregular": 1, "repo_exceeds_max_files": 1},
    }
    assert json.loads((out_dir / "report.json").read_text()) == report
    final_path = out_dir / "repos.jsonl"
    repos_path = out_dir / "intermediate" / "repos.ndjson"
    signatures_path = out_dir / "intermediate" / "signatures.ndjson"
    final_rows = _jsonl(final_path)
    intermediate_rows = _jsonl(repos_path)
    assert len(final_rows) == len(intermediate_rows) == 1
    assert final_rows[0]["repo_path"] == intermediate_rows[0]["repo_path"] == "owner/normal"
    assert final_rows[0]["num_files"] == 2
    assert [f["file_path"] for f in final_rows[0]["files"]] == ["first.py", "second.py"]
    assert signatures_path.read_text() == ""
    manifest = json.loads((out_dir / "manifest.json").read_text())
    assert set(manifest["processed"]) == {str(normal), str(over_cap)}
    assert len(manifest["processed"]) == 2
    assert manifest["failed"] == {}
    assert manifest["dropped_repos"] == {str(over_cap): "repo_exceeds_max_files"}

    before = {p: p.read_bytes() for p in (final_path, repos_path, signatures_path)}
    resumed = runner.run_pipeline(config_path, resume=True)

    assert resumed["tarballs_processed"] == 2
    assert resumed["tarballs_failed"] == 0
    assert resumed["tarballs_repo_dropped"] == 1
    assert resumed["repos_out"] == 1
    assert resumed["files_out"] == 2
    # Resume reconstructs cumulative counters from committed per-input shards.
    assert resumed["stages"] == report["stages"]
    assert {p: p.read_bytes() for p in before} == before
    assert json.loads((out_dir / "manifest.json").read_text()) == manifest


def test_corpus_statistics_count_only_retained_files_and_survive_resume(tmp_path, cfg, monkeypatch):
    from dataflow import execution
    body, unknown = b"print('alpha')\n", b"some unknown text\n"
    _tar(tmp_path / "input", [_member("snapshot/a.py", body),
                              _member("snapshot/copy.py", body),
                              _member("snapshot/data.weird", unknown)], name="normal")
    cfg["unpack"]["max_repo_files"] = 3
    dropped = _tar(tmp_path / "input", [_member(f"snapshot/{n}.py", b"discard") for n in range(4)], name="over-cap")
    total = len(body) + len(unknown)
    monkeypatch.setattr(execution, "LARGE_REPO_BYTES", total - 1)
    path, output = _setup_run(tmp_path, cfg)
    report = runner.run_pipeline(path)
    assert report["files_out"] == 2
    assert report["content_bytes_out"] == total
    assert report["byte_share_top_langs"] == {
        "Python": round(len(body) / total, 4), "unknown": round(len(unknown) / total, 4)}
    assert report["repos_over_1_5gb"] == report["tarballs_repo_dropped"] == 1
    assert json.loads((output / "manifest.json").read_text())["dropped_repos"] == {
        str(dropped): "repo_exceeds_max_files"}
    resumed = runner.run_pipeline(path, resume=True)
    for key in ("content_bytes_out", "byte_share_top_langs", "repos_over_1_5gb", "tarballs_repo_dropped"):
        assert resumed[key] == report[key]


def test_large_repository_stat_uses_strict_size_boundary(tmp_path, cfg, monkeypatch):
    from dataflow import execution
    body = b"print('boundary')\n"
    _tar(tmp_path / "input", [_member("snapshot/a.py", body)])
    monkeypatch.setattr(execution, "LARGE_REPO_BYTES", len(body))
    path, output = _setup_run(tmp_path, cfg)
    report = runner.run_pipeline(path)
    assert report["repos_over_1_5gb"] == 0
    assert report["content_bytes_out"] == len(body)


def _setup_run(tmp_path, cfg, name="reliable"):
    cfg["run"].update(input_dir=str(tmp_path / "input"), output_dir="runs",
                      name=name, workers=2, max_tarballs=0)
    config_path = tmp_path / f"{name}.yaml"
    config_path.write_text(yaml.safe_dump(cfg))
    return config_path, tmp_path / "runs" / name


def test_same_run_without_resume_is_rejected_without_mutating_outputs(tmp_path, cfg):
    _tar(tmp_path / "input", [_member("snapshot/a.py", b"print('hello')\n")])
    path, output = _setup_run(tmp_path, cfg)
    runner.run_pipeline(path)
    before = {p: p.read_bytes() for p in output.rglob("*") if p.is_file()}
    with pytest.raises(ValueError, match="run already exists"):
        runner.run_pipeline(path)
    assert {p: p.read_bytes() for p in before} == before


def test_committed_shard_survives_crash_before_manifest_update(tmp_path, cfg, monkeypatch):
    from dataflow import execution
    _tar(tmp_path / "input", [_member("snapshot/a.py", b"print('hello')\n")])
    path, output = _setup_run(tmp_path, cfg)
    write = execution.atomic_json
    def crash_after_commit(target, value):
        if target.name == "manifest.json" and value["processed"]:
            raise RuntimeError("injected crash after shard commit")
        return write(target, value)
    with monkeypatch.context() as patch:
        patch.setattr(execution, "atomic_json", crash_after_commit)
        with pytest.raises(RuntimeError, match="injected crash"):
            runner.run_pipeline(path)
    assert len(list((output / "intermediate" / "shards").glob("*.json"))) == 1
    assert json.loads((output / "manifest.json").read_text())["processed"] == []
    def must_not_reclean(args):
        raise AssertionError("a committed input must not be cleaned again")
    monkeypatch.setattr(runner, "_clean_one_tar", must_not_reclean)
    report = runner.run_pipeline(path, resume=True)
    assert report["repos_out"] == report["files_out"] == 1
    assert report["stages"]["unpack"]["seen"] == 1


def test_resume_rebuilds_torn_derivatives_from_shards(tmp_path, cfg):
    _tar(tmp_path / "input", [_member("snapshot/a.py", b"print('hello')\n")])
    path, output = _setup_run(tmp_path, cfg)
    first = runner.run_pipeline(path)
    final = (output / "repos.jsonl").read_bytes()
    (output / "intermediate" / "repos.ndjson").write_text('{"torn":')
    (output / "intermediate" / "signatures.ndjson").write_text('{"torn":')
    (output / "repos.jsonl").write_text('{"torn":')
    resumed = runner.run_pipeline(path, resume=True)
    assert resumed["stages"] == first["stages"]
    assert (output / "repos.jsonl").read_bytes() == final


def test_resume_refuses_changed_input_or_code(tmp_path, cfg, monkeypatch):
    from dataflow import execution
    tar = _tar(tmp_path / "input", [_member("snapshot/a.py", b"print('hello')\n")])
    path, output = _setup_run(tmp_path, cfg)
    runner.run_pipeline(path)
    with monkeypatch.context() as patch:
        patch.setattr(execution, "source_fingerprint", lambda: "different-code")
        with pytest.raises(ValueError, match="source code or dependencies changed"):
            runner.run_pipeline(path, resume=True)
    tar.write_bytes(b"different archive")
    with pytest.raises(ValueError, match="input snapshot changed"):
        runner.run_pipeline(path, resume=True)


def test_empty_input_produces_empty_outputs(tmp_path, cfg):
    (tmp_path / "input").mkdir()
    path, output = _setup_run(tmp_path, cfg)
    report = runner.run_pipeline(path)
    assert report["repos_out"] == report["files_out"] == 0
    assert (output / "repos.jsonl").read_text() == ""


def test_legacy_run_is_preserved_and_cannot_be_resumed(tmp_path, cfg):
    (tmp_path / "input").mkdir()
    path, output = _setup_run(tmp_path, cfg)
    output.mkdir(parents=True)
    old = '{"fingerprint":"old","processed":[]}'
    (output / "manifest.json").write_text(old)
    with pytest.raises(ValueError, match="legacy append-only run"):
        runner.run_pipeline(path, resume=True)
    assert (output / "manifest.json").read_text() == old


def test_dedup_counts_short_files_exact_and_near_duplicates(tmp_path, cfg):
    if not runner._HAS_DATASKETCH:
        pytest.skip("requires installed MinHash dependency")
    text = ("def compute(value):\n    return value + 123456789\n" * 12).encode()
    cfg["unpack"]["max_file_bytes"] = 10000
    cfg["run"]["steps"]["dedup"] = True
    _tar(tmp_path / "input", [_member("snapshot/a.py", text),
                              _member("snapshot/b.py", text),
                              _member("snapshot/c.py", text + b"# minor variant\n"),
                              _member("snapshot/d.py", b"x"),
                              _member("snapshot/e.py", b"x")])
    path, output = _setup_run(tmp_path, cfg)
    report = runner.run_pipeline(path)
    stats = report["stages"]["dedup"]
    assert stats["seen"] == 5
    assert stats["reasons"] == {"exact_duplicate": 2, "near_duplicate": 1}
    assert stats["seen"] == stats["kept"] + stats["dropped"]
    assert stats["kept"] == report["files_out"] == 2
    assert runner.run_pipeline(path, resume=True)["stages"] == report["stages"]


@pytest.mark.parametrize("enable_near", [False, True])
def test_output_is_stable_across_worker_counts(tmp_path, cfg, enable_near):
    if enable_near and not runner._HAS_DATASKETCH:
        pytest.skip("requires installed MinHash dependency")
    cfg["run"]["steps"]["dedup"] = enable_near
    for n in range(6):
        _tar(tmp_path / "input", [_member("snapshot/shared.py", b"print('shared')\n"),
                                  _member("snapshot/unique.py", f"value = {n}\n".encode())], name=f"repo{n}")
    one = copy.deepcopy(cfg)
    path1, out1 = _setup_run(tmp_path, one, name="one")
    one["run"]["workers"] = 1
    path1.write_text(yaml.safe_dump(one))
    path2, out2 = _setup_run(tmp_path, cfg, name="two")
    report1, report2 = runner.run_pipeline(path1), runner.run_pipeline(path2)
    assert (out1 / "repos.jsonl").read_bytes() == (out2 / "repos.jsonl").read_bytes()
    assert report1["stages"] == report2["stages"]


def test_inventory_only_allows_valid_unchanged_archives(tmp_path, cfg):
    from dataflow.inventory import validate_inputs
    root = tmp_path / "input"
    _tar(root, [_member("snapshot/a.py", b"print('accepted')\n")], name="valid")
    _tar(root, [_member("snapshot/a.py", b"print('unvalidated')\n")], name="pending")
    bad = root / "owner" / "bad_HEAD.tar.gz"
    bad.write_bytes(b"404: Not Found")
    inv = tmp_path / "inventory"
    validate_inputs(root, inv, workers=1)
    rows = _jsonl(inv / "inventory.jsonl")
    for row in rows:
        if "pending_HEAD" in row["path"]:
            row["status"] = "gzip_candidate"
    manifest = inv / "inventory.jsonl"
    manifest.write_text("".join(json.dumps(row) + "\n" for row in rows))
    cfg["run"]["input_manifest"] = str(manifest)
    path, output = _setup_run(tmp_path, cfg)
    report = runner.run_pipeline(path)
    assert report["tarballs_seen"] == 1
    assert report["tarballs_excluded_by_inventory"] == 2
    assert report["files_out"] == 1
    frozen_manifest = manifest.read_text()
    manifest.write_text(" " + frozen_manifest)
    with pytest.raises(ValueError, match="input inventory changed"):
        runner.run_pipeline(path, resume=True)
    manifest.write_text(frozen_manifest)
    valid = next(row for row in rows if row["status"] == "valid")
    Path(valid["path"]).write_bytes(b"changed")
    with pytest.raises(ValueError, match="validated input changed"):
        runner.run_pipeline(path, resume=True)
