"""Regression tests for the 2026-11-11 cleaning-rule changes.

Unicode alpha ratio, notebook-before-quality, true-Jaccard near-dup
verification, Stack v3 representative order, byte budget, repo downsampling,
and the removal of any activity-year filter.
"""

import base64
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

from dataflow import budget, dedup, execution, runner
from dataflow.models import FileRecord
from dataflow.quality import QualityFilter, alpha_ratio

needs_fork = pytest.mark.skipif("fork" not in mp.get_all_start_methods(), reason="runner requires fork")
needs_minhash = pytest.mark.skipif(not runner._HAS_DATASKETCH, reason="requires datasketch")


@pytest.fixture
def cfg():
    default = Path(__file__).resolve().parents[1] / "configs" / "default.yaml"
    config = copy.deepcopy(yaml.safe_load(default.read_text(encoding="utf-8")))
    config["run"]["steps"].update(quality=False, notebooks=False, dedup=False,
                                  licenses=False, pii=False)
    config["languages"]["go_enry_bin"] = ""
    config["unpack"].update(max_repo_files=100, max_file_bytes=1_000_000)
    return config


def _tar(root, files, owner="owner", name="repo"):
    path = root / owner / f"{name}_HEAD.tar.gz"
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        for member, data in files.items():
            info = tarfile.TarInfo(f"snapshot/{member}")
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))
    return path


def _run(tmp_path, cfg, name="run", workers=2):
    cfg["run"].update(input_dir=str(tmp_path / "input"), output_dir="runs",
                      name=name, workers=workers, max_tarballs=0)
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(cfg, allow_unicode=True), encoding="utf-8")
    report = runner.run_pipeline(path)
    rows = [json.loads(l) for l in (tmp_path / "runs" / name / "repos.jsonl").read_text().splitlines()]
    return report, rows


def _kept(rows):
    return {(r["repo_path"], f["file_path"]) for r in rows for f in r["files"]}


# ---------------------------------------------------------------- 1. alpha

def test_unicode_letters_count_as_alphabetic():
    chinese = "# 数据清洗说明\n\n本项目用于清洗训练数据，保留中文和英文文本。\n" * 5
    assert alpha_ratio(chinese, "Markdown") > 0.6
    assert alpha_ratio("Привет мир, это тест", "Text") > 0.7
    rec = FileRecord(repo_path="o/r", file_path="README.md", size_bytes=1, sha1="0",
                     content=chinese, language="Markdown")
    assert QualityFilter({"min_alpha_ratio": 0.25}).check(rec) is None


def test_symbol_heavy_text_still_fails_and_assembly_uses_unicode_alnum():
    rec = FileRecord(repo_path="o/r", file_path="x.txt", size_bytes=1, sha1="0",
                     content="0101 {} [] ;; == ++ -- 1234 5678 ** // ##\n" * 20, language="Text")
    assert QualityFilter({"min_alpha_ratio": 0.25}).check(rec) == "low_alpha_ratio"
    # digits count for Assembly only; full-width digits are Unicode alnum too
    assert alpha_ratio("１２３ mov", "Assembly") == pytest.approx(6 / 7)
    assert alpha_ratio("１２３ mov", "C") == pytest.approx(3 / 7)


# ---------------------------------------------------------------- 2. notebook

def _notebook_with_image():
    image = base64.b64encode(bytes(range(256)) * 400).decode()  # ~136 KB single-line blob
    nb = {
        "cells": [
            {"cell_type": "markdown", "metadata": {}, "source": ["# 绘图示例\n", "Plot a sine wave.\n"]},
            {"cell_type": "code", "execution_count": 3, "metadata": {"scrolled": True},
             "source": ["import numpy as np\n", "import matplotlib.pyplot as plt\n",
                        "x = np.linspace(0, 10, 100)\n", "plt.plot(x, np.sin(x))\n"],
             "outputs": [{"output_type": "display_data", "data": {"image/png": image}, "metadata": {}}]},
        ],
        "metadata": {"kernelspec": {"name": "python3"}, "language_info": {"name": "python"}},
        "nbformat": 4, "nbformat_minor": 5,
    }
    return json.dumps(nb, indent=1).encode()


def test_notebook_is_stripped_before_quality_filter(tmp_path, cfg):
    cfg["run"]["steps"].update(quality=True, notebooks=True)
    tar = _tar(tmp_path / "input", {"plot.ipynb": _notebook_with_image()})
    result = runner._clean_one_tar((str(tar), cfg))
    stages = [s["stage"] for s in result["stats"]]
    assert stages.index("notebooks") < stages.index("quality")
    assert [r["file_path"] for r in result["records"]] == ["plot.ipynb"]
    content = result["records"][0]["content"]
    assert "image/png" not in content and "outputs" not in content
    assert max(len(line) for line in content.splitlines()) < 1000
    assert json.loads(content)["cells"][1]["source"][0] == "import numpy as np\n"


# ---------------------------------------------------------------- 3. true Jaccard

def test_true_jaccard_exact_values():
    import numpy as np
    a = np.arange(1000, dtype=np.uint64)
    assert dedup.true_jaccard(a, np.arange(185, 1185, dtype=np.uint64)) == pytest.approx(815 / 1185)  # 0.688
    assert dedup.true_jaccard(a, np.arange(166, 1166, dtype=np.uint64)) == pytest.approx(834 / 1166)  # 0.715
    assert dedup.true_jaccard(a, a) == 1.0
    text = "def f(x):\n    return x + 1\n"
    assert len(dedup.shingle_set(text)) == len(set(dedup._tokenize(text)))


def _sig(values, perms=256):
    from datasketch import MinHash
    mh = MinHash(num_perm=perms, scheme="affine32")
    mh.update_batch([int(v).to_bytes(8, "little") for v in values])
    return mh


def _find_pair(overlap, want_estimate_above):
    """Shift a 1000-element window so the MinHash estimate lands on the wanted
    side of 0.7 while the true Jaccard stays fixed (= overlap / (2000 - overlap))."""
    from datasketch import MinHashLSH
    shift = 1000 - overlap
    for base in range(0, 400_000, 1000):
        a = range(base, base + 1000)
        b = range(base + shift, base + shift + 1000)
        ma, mb = _sig(a), _sig(b)
        est = ma.jaccard(mb)
        lsh = MinHashLSH(threshold=0.7, num_perm=256)
        lsh.insert("a", ma)
        if (est >= 0.7) == want_estimate_above and est >= 0.6 and lsh.query(mb):
            return list(a), list(b), est
    raise AssertionError("no suitable pair found")


@needs_minhash
@pytest.mark.parametrize("overlap,est_above,merge_exact,merge_estimate", [
    (815, True, False, True),    # true 0.688: estimate says dup, truth says keep both
    (834, False, True, False),   # true 0.715: estimate misses, truth merges
])
def test_near_dup_uses_true_jaccard(tmp_path, monkeypatch, overlap, est_above, merge_exact, merge_estimate):
    import numpy as np
    a, b, est = _find_pair(overlap, est_above)
    sets = {"A": np.array(sorted(a), dtype=np.uint64), "B": np.array(sorted(b), dtype=np.uint64)}
    monkeypatch.setattr(dedup, "shingle_set", lambda content: sets[content])
    sha_a, sha_b = "a" * 40, "b" * 40
    sig_path = tmp_path / "signatures.ndjson"
    sig_path.write_text("".join(
        json.dumps({"sha1": s, "sig": base64.b64encode(_sig(v).digest().tobytes()).decode()}) + "\n"
        for s, v in ((sha_a, a), (sha_b, b))))
    repos_path = tmp_path / "repos.ndjson"
    repos_path.write_text(json.dumps({"repo_path": "o/r", "records": [
        {"sha1": sha_a, "content": "A"}, {"sha1": sha_b, "content": "B"}]}) + "\n")
    base = {"dedup": {"minhash_permutations": 256, "lsh_jaccard_threshold": 0.7,
                      "verify_jaccard": True, "verify_prefilter_margin": 0.1}}
    exact_cfg = copy.deepcopy(base); exact_cfg["dedup"]["verify_method"] = "exact"
    est_cfg = copy.deepcopy(base); est_cfg["dedup"]["verify_method"] = "estimate"
    dropped_exact, info = execution._near_duplicates(sig_path, exact_cfg, repos_path=repos_path,
                                                     workdir=tmp_path, workers=1)
    dropped_est, _ = execution._near_duplicates(sig_path, est_cfg)
    assert (dropped_exact == {sha_b}) is merge_exact
    assert (dropped_est == {sha_b}) is merge_estimate
    assert info["verified"] == 1
    if est_above:
        assert info["rejected_but_estimate_passed"] == 1
        sample = info["rejected_samples"][0]
        assert sample["true_jaccard"] == pytest.approx(815 / 1185, abs=1e-4)
    else:
        assert info["accepted_but_estimate_failed"] == 1
    assert not (tmp_path / "near_dup_shingles.u64").exists()


# ---------------------------------------------------------------- 4. representative

def test_priority_key_follows_stack_v3_order():
    key = dedup.repo_priority_key
    assert key({"stars": 10}) < key({"stars": 9, "forks": 100})
    assert key({"stars": 5, "forks": 2}) < key({"stars": 5, "forks": 1})
    assert key({"stars": 5, "license": "MIT"}) < key({"stars": 5, "license": "GPL-3.0-only"})
    assert key({"stars": 5, "created_at": "2015-01-01"}) < key({"stars": 5, "created_at": "2020-01-01"})
    assert key({"stars": 5, "created_at": "2020-01-01"}) < key({"stars": 5})
    assert key({"stars": 0}) < key(None)          # known 0 stars beats unknown
    assert key(None) == key({})                    # all-missing ties -> input order


@needs_fork
@needs_minhash
def test_representative_selection_uses_metadata_for_exact_and_near(tmp_path, cfg):
    cfg["run"]["steps"]["dedup"] = True
    shared = ("def compute(value):\n    return value + 123456789\n" * 12).encode()
    variant = shared + b"# minor variant\n"
    _tar(tmp_path / "input", {"a.py": shared}, owner="aaa", name="first")
    _tar(tmp_path / "input", {"a.py": shared}, owner="bbb", name="starred")
    _tar(tmp_path / "input", {"b.py": variant}, owner="ccc", name="popular")
    _tar(tmp_path / "input", {"a.py": shared}, owner="ddd", name="older")
    meta = tmp_path / "meta.tsv"
    meta.write_text("repo_path\tstars\tforks\tcreated_at\tlicense\n"
                    "bbb/Starred\t10\t\t2020-01-01\t\n"
                    "ddd/older\t10\t\t2012-05-01\t\n"
                    "ccc/popular\t50\t3\t\t\n")
    # without metadata: the first path wins both the exact and the near cluster
    plain = copy.deepcopy(cfg)
    _, rows = _run(tmp_path, plain, name="plain")
    assert _kept(rows) == {("aaa/first", "a.py")}
    # with metadata: popular (50 stars) beats the 10-star ones for the near
    # cluster; among exact copies, the older 10-star repo beats the newer one
    cfg["dedup"]["repo_metadata_path"] = str(meta)
    report, rows = _run(tmp_path, cfg, name="meta")
    assert _kept(rows) == {("ccc/popular", "b.py")}
    assert report["stages"]["dedup"]["reasons"] == {"exact_duplicate": 2, "near_duplicate": 1}
    assert report["representative_policy"].startswith("stars_desc>forks_desc")
    assert report["repo_metadata"]["repos_matched"] == 3
    assert rows[0]["github_metadata"] == {"stars": 50, "forks": 3}
    # exact-only: the 2012 repo keeps the shared file
    cfg["run"]["steps"]["dedup"] = False
    _, rows = _run(tmp_path, cfg, name="exact-only")
    assert ("ddd/older", "a.py") in _kept(rows)
    assert ("aaa/first", "a.py") not in _kept(rows)


# ---------------------------------------------------------------- 5/6. budget + downsample

def test_downsample_decision_is_deterministic_and_unbiased():
    shas = [f"{i:040x}" for i in range(20_000)]
    first = [budget.keep_in_downsample(7, s, 0.5) for s in shas]
    assert first == [budget.keep_in_downsample(7, s, 0.5) for s in shas]
    assert 0.48 < sum(first) / len(first) < 0.52
    assert first != [budget.keep_in_downsample(8, s, 0.5) for s in shas]


def test_byte_budget_stops_at_first_overflow():
    dropped, kept = budget.apply_byte_budget([(0.3, "c", 5), (0.1, "a", 4), (0.2, "b", 4)], cap=9)
    assert kept == 8 and dropped == [("c",)]


@needs_fork
def test_pipeline_byte_budget_and_downsample(tmp_path, cfg):
    files = {f"f{i:02d}.py": f"value_{i} = {i}\n".encode() * 4 for i in range(40)}
    files.update({f"d{i:02d}.md": f"# doc {i}\n".encode() for i in range(10)})
    _tar(tmp_path / "input", files)
    py_bytes = sum(len(v) for k, v in files.items() if k.endswith(".py"))
    cfg["quality"]["byte_budget"] = {"Python": py_bytes // 2}
    cfg["quality"]["max_repo_bytes_before_downsample"] = 10**9
    report, rows = _run(tmp_path, cfg, name="budget")
    stage = report["stages"]["budget"]
    assert set(stage["reasons"]) == {"byte_budget"}
    assert report["byte_budget"]["Python"]["kept_bytes"] <= py_bytes // 2
    assert stage["seen"] == stage["kept"] + stage["dropped"] == 50
    assert sum(1 for f in rows[0]["files"] if f["file_path"].endswith(".md")) == 10
    # downsample: tiny threshold -> every repo is "large"
    cfg2 = copy.deepcopy(cfg)
    cfg2["quality"]["byte_budget"] = {}
    cfg2["quality"]["max_repo_bytes_before_downsample"] = 1
    r1, rows1 = _run(tmp_path, cfg2, name="ds1", workers=1)
    r2, rows2 = _run(tmp_path, cfg2, name="ds2", workers=2)
    assert rows1 == rows2
    assert r1["repo_downsample"]["repos_downsampled"] == 1
    dropped = r1["stages"]["budget"]["reasons"]["repo_downsample"]
    assert 10 < dropped < 40 and r1["files_out"] == 50 - dropped


# ---------------------------------------------------------------- 7. no year filter

@needs_fork
def test_no_year_based_filtering(tmp_path, cfg):
    _tar(tmp_path / "input", {"LICENSE": b"Copyright (c) 2014 someone\n", "a.py": b"print(1)\n"}, name="old")
    _tar(tmp_path / "input", {"README.md": b"Updated 2026\n", "a.py": b"print(2)\n"}, name="new")
    _tar(tmp_path / "input", {"a.py": b"print(3)\n"}, name="none")
    report, rows = _run(tmp_path, cfg, name="years")
    assert report["repos_out"] == 3
    assert "repos_active_2026" not in report
    assert report["activity_year_hist"] == {"2014": 1, "2026": 1, "None": 1}
