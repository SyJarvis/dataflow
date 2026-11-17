"""bundle_to_tar: .bundle / .git.tar / .dir.tar -> <user>/<repo>_HEAD.tar.gz."""

import importlib.util
import subprocess
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
spec = importlib.util.spec_from_file_location("b2t", ROOT / "scripts" / "bundle_to_tar.py")
b2t = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b2t)

from dataflow.unpack import repo_path_from_tarball  # noqa: E402


def git(*a, cwd):
    return subprocess.run(["git", *a], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def volume(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    git("init", "-q", "-b", "main", cwd=work)
    git("config", "user.email", "t@t", cwd=work)
    git("config", "user.name", "t", cwd=work)
    (work / "a.py").write_text("print(1)\n")
    git("add", ".", cwd=work)
    git("commit", "-qm", "one", cwd=work)
    rev1 = git("rev-parse", "HEAD", cwd=work)
    (work / "b.md").write_text("# two\n")
    git("add", ".", cwd=work)
    git("commit", "-qm", "two", cwd=work)
    rev2 = git("rev-parse", "HEAD", cwd=work)
    root = tmp_path / "vol"
    for d in ("u/bun", "u/gt", "u/dt"):
        (root / "repos" / d).mkdir(parents=True)
    git("bundle", "create", str(root / "repos/u/bun/x.bundle"), "--all", cwd=work)
    bare = tmp_path / f"swh:1:rev:{rev1}.git"
    git("clone", "-q", "--bare", str(work), str(bare), cwd=tmp_path)
    with tarfile.open(root / "repos/u/gt/x.git.tar", "w") as tf:
        tf.add(bare, arcname=bare.name)
    with tarfile.open(root / "repos/u/dt/x.dir.tar", "w") as tf:
        tf.add(work / "a.py", arcname="swh:1:dir:abc/a.py")
        tf.add(work / "b.md", arcname="swh:1:dir:abc/sub/b.md")
    return root, rev1, rev2


def names(path):
    with tarfile.open(path) as tf:
        return sorted(m.name for m in tf if m.isfile())


def test_all_kinds(volume, tmp_path):
    root, rev1, rev2 = volume
    out = tmp_path / "out"
    rows = [
        {"repo_id": "1", "swh_rev": rev1, "artifact": "repos/u/bun/x.bundle", "stars": "5"},
        {"repo_id": "2", "swh_rev": rev2, "artifact": "repos/u/gt/x.git.tar", "stars": ""},
        {"repo_id": "3", "swh_rev": "", "artifact": "repos/u/dt/x.dir.tar", "stars": "1"},
        {"repo_id": "4", "swh_rev": "f" * 40, "artifact": "repos/u/bun/x.bundle"},
    ]
    recs = [b2t.convert_one(r, root, out, tmp_path, False, 60) for r in rows[:3]]
    assert [r["status"] for r in recs] == ["ok", "ok", "ok"]
    assert recs[0]["rev_used"] == rev1  # manifest rev, not the newer HEAD
    assert names(out / "u/bun_HEAD.tar.gz") == ["bun-HEAD/a.py"]
    assert names(out / "u/gt_HEAD.tar.gz") == ["gt-HEAD/a.py", "gt-HEAD/b.md"]
    assert names(out / "u/dt_HEAD.tar.gz") == ["dt-HEAD/a.py", "dt-HEAD/sub/b.md"]
    assert repo_path_from_tarball(out / "u/bun_HEAD.tar.gz") == "u/bun"
    # resumable
    assert b2t.convert_one(rows[0], root, out, tmp_path, False, 60)["status"] == "skip_exists"
    # missing rev -> HEAD fallback
    (out / "u/bun_HEAD.tar.gz").unlink()
    rec = b2t.convert_one(rows[3], root, out, tmp_path, False, 60)
    assert rec["status"] == "ok" and rec["rev_used"] == f"fallback:{rev2}"
    assert not list(tmp_path.glob("b2t-*")) and not list(out.rglob("*.tmp"))
    b2t.write_metadata(rows[:3], out / "repo_metadata.tsv")
    assert (out / "repo_metadata.tsv").read_text().splitlines()[1] == "u/bun\t5\t\t\t"
