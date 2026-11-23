#!/usr/bin/env python3
"""Convert the swhkit ``repos/`` volume into github_downloads-style tarballs.

Input  : MANIFEST.tsv (repo_id url stars swh_rev bytes sha256 artifact) whose
         ``artifact`` points at ``repos/<user>/<repo>/<rev12>.{bundle,git.tar,dir.tar}``.
Output : ``<out>/<user>/<repo>_HEAD.tar.gz`` containing ``<repo>-HEAD/<files>``
         (same layout as github_downloads, which dataflow's unpack step expects),
         plus ``convert_log.tsv`` (one row per attempt) and ``repo_metadata.tsv``
         (``dedup.repo_metadata_path`` format, stars from MANIFEST).

Per artifact kind:
  * .bundle  : ``git init --bare`` in a temp dir, ``git bundle unbundle``, then
               ``git archive`` of ``swh_rev`` (falls back to the bundle's HEAD,
               then refs/heads/{main,master}, then the first head).
  * .git.tar : extract the bare repo to temp, ``git archive`` the same way.
  * .dir.tar : already a file tree (``swh:1:dir:<id>/...``); re-root it under
               ``<repo>-HEAD/`` and gzip.

Resumable (an existing final file is complete because writes go to ``.tmp``
then ``os.replace``), parallel (threads driving git subprocesses), and temp
dirs are always removed. Raw inputs are only read.

    python scripts/bundle_to_tar.py --manifest /mnt/data4/cleaning_data/MANIFEST.tsv \
        --root /mnt/data4/cleaning_data --out /mnt/data4/cleaning_data/repos_tar_X \
        --sample 300 --seed 20261117 --workers 32 [--only-new DIR] [--verify-sha256]
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import os
import random
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

LOG_FIELDS = ("repo_id", "repo_path", "artifact", "kind", "status", "rev_used",
              "in_bytes", "out_bytes", "seconds", "error")
KINDS = (".bundle", ".git.tar", ".dir.tar")


def read_manifest(path: Path) -> list[dict]:
    with path.open(encoding="utf-8", newline="") as fh:
        return [r for r in csv.DictReader(fh, delimiter="\t") if r.get("artifact")]


def repo_path_of(row: dict) -> str:
    """``repos/<user>/<repo>/<rev12>.bundle`` -> ``<user>/<repo>``."""
    parts = row["artifact"].split("/")
    return f"{parts[-3]}/{parts[-2]}"


def kind_of(artifact: str) -> str:
    for k in KINDS:
        if artifact.endswith(k):
            return k
    return ""


def existing_repos(dirpath: Path) -> set[str]:
    out = set()
    for tar in dirpath.glob("*/*.tar.gz"):
        out.add(f"{tar.parent.name}/{tar.name.removesuffix('.tar.gz').removesuffix('_HEAD')}".lower())
    return out


def _git(args, cwd=None, timeout=3600):
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0", GIT_CONFIG_NOSYSTEM="1")
    return subprocess.run(["git", *args], cwd=cwd, capture_output=True, text=True,
                          timeout=timeout, env=env)


def _pick_rev(gitdir: Path, want: str, heads: list[tuple[str, str]]) -> str:
    def has(rev):
        return rev and _git(["--git-dir", str(gitdir), "cat-file", "-e", f"{rev}^{{commit}}"]).returncode == 0
    if has(want):
        return want
    by_ref = {ref: sha for sha, ref in heads}
    for ref in ("HEAD", "refs/heads/main", "refs/heads/master"):
        if has(by_ref.get(ref)):
            return by_ref[ref]
    for sha, ref in heads:
        if ref.startswith("refs/heads/") and has(sha):
            return sha
    if has("HEAD"):
        return _git(["--git-dir", str(gitdir), "rev-parse", "HEAD"]).stdout.strip()
    raise RuntimeError(f"rev {want} and no fallback head present")


def _archive(gitdir: Path, rev: str, prefix: str, dest: Path, timeout: int):
    with dest.open("wb") as fh:
        p = subprocess.run(["git", "--git-dir", str(gitdir), "archive", "--format=tar.gz",
                            f"--prefix={prefix}", rev], stdout=fh, stderr=subprocess.PIPE,
                           timeout=timeout)
    if p.returncode:
        raise RuntimeError("git archive: " + p.stderr.decode(errors="replace")[-300:])


def convert_bundle(src: Path, tmp: Path, want: str, prefix: str, dest: Path, timeout: int) -> str:
    gitdir = tmp / "repo.git"
    if _git(["init", "-q", "--bare", str(gitdir)]).returncode:
        raise RuntimeError("git init failed")
    p = _git(["--git-dir", str(gitdir), "bundle", "unbundle", str(src)], timeout=timeout)
    if p.returncode:
        raise RuntimeError("unbundle: " + p.stderr[-300:])
    heads = [tuple(line.split(" ", 1)) for line in p.stdout.splitlines() if " " in line]
    rev = _pick_rev(gitdir, want, heads)
    _archive(gitdir, rev, prefix, dest, timeout)
    return rev


def convert_gittar(src: Path, tmp: Path, want: str, prefix: str, dest: Path, timeout: int) -> str:
    with tarfile.open(src) as tf:
        tf.extractall(tmp, filter="data") if hasattr(tarfile, "data_filter") else tf.extractall(tmp)
    cands = [d for d in tmp.iterdir() if d.is_dir() and (d / "HEAD").exists() and (d / "objects").is_dir()]
    if not cands:
        cands = [d for d in tmp.rglob("*") if d.is_dir() and (d / "HEAD").exists() and (d / "objects").is_dir()]
    if not cands:
        raise RuntimeError("no bare repo inside .git.tar")
    gitdir = cands[0]
    p = _git(["--git-dir", str(gitdir), "for-each-ref", "--format=%(objectname) %(refname)"])
    heads = [tuple(l.split(" ", 1)) for l in p.stdout.splitlines() if " " in l]
    rev = _pick_rev(gitdir, want, heads)
    _archive(gitdir, rev, prefix, dest, timeout)
    return rev


def convert_dirtar(src: Path, prefix: str, dest: Path) -> str:
    with tarfile.open(src) as tin, gzip.open(dest, "wb", compresslevel=6) as gz, \
            tarfile.open(fileobj=gz, mode="w", format=tarfile.PAX_FORMAT) as tout:
        for m in tin:
            name = m.name
            while name.startswith(("./", "/")):
                name = name[1:] if name.startswith("/") else name[2:]
            head, _, rest = name.partition("/")
            if not rest and m.isdir():
                continue  # the swh:1:dir:<id>/ wrapper itself
            m.name = prefix + (rest if head.startswith("swh:") else name)
            tout.addfile(m, tin.extractfile(m) if m.isfile() else None)
    return "dir"


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def convert_one(row: dict, root: Path, out: Path, tmp_root: Path, verify: bool, timeout: int) -> dict:
    t0 = time.time()
    repo_path = repo_path_of(row)
    owner, repo = repo_path.split("/")
    final = out / owner / f"{repo}_HEAD.tar.gz"
    src = root / row["artifact"]
    kind = kind_of(row["artifact"])
    rec = {"repo_id": row.get("repo_id", ""), "repo_path": repo_path, "artifact": row["artifact"],
           "kind": kind, "status": "", "rev_used": "", "in_bytes": "", "out_bytes": "",
           "seconds": "", "error": ""}
    if final.exists() and final.stat().st_size > 0:
        rec.update(status="skip_exists", out_bytes=final.stat().st_size)
        return rec
    final.parent.mkdir(parents=True, exist_ok=True)
    part = final.with_name(final.name + ".tmp")
    tmp = Path(tempfile.mkdtemp(prefix="b2t-", dir=tmp_root))
    try:
        rec["in_bytes"] = src.stat().st_size
        if verify and row.get("sha256") and sha256_file(src) != row["sha256"]:
            raise RuntimeError("sha256 mismatch")
        prefix = f"{repo}-HEAD/"
        want = (row.get("swh_rev") or "").strip()
        if kind == ".bundle":
            rev = convert_bundle(src, tmp, want, prefix, part, timeout)
        elif kind == ".git.tar":
            rev = convert_gittar(src, tmp, want, prefix, part, timeout)
        elif kind == ".dir.tar":
            rev = convert_dirtar(src, prefix, part)
        else:
            raise RuntimeError(f"unknown artifact kind: {row['artifact']}")
        os.replace(part, final)
        rec.update(status="ok", rev_used=rev if rev == "dir" or rev == want else f"fallback:{rev}",
                   out_bytes=final.stat().st_size)
    except Exception as exc:  # noqa: BLE001 - every failure is logged, never fatal
        rec.update(status="fail", error=str(exc).replace("\t", " ").replace("\n", " ")[:500])
        part.unlink(missing_ok=True)
    finally:
        shutil.rmtree(tmp, ignore_errors=True)
        rec["seconds"] = f"{time.time() - t0:.2f}"
    return rec


def write_metadata(rows: list[dict], path: Path):
    tmp = path.with_name(path.name + ".tmp")
    with tmp.open("w", encoding="utf-8") as fh:
        fh.write("repo_path\tstars\tforks\tcreated_at\tlicense\n")
        for r in sorted(rows, key=lambda r: repo_path_of(r).lower()):
            fh.write(f"{repo_path_of(r)}\t{(r.get('stars') or '').strip()}\t\t\t\n")
    os.replace(tmp, path)


def select(rows, args) -> list[dict]:
    rows = [r for r in rows if kind_of(r["artifact"])]
    if args.only_new:
        old = existing_repos(args.only_new)
        rows = [r for r in rows if repo_path_of(r).lower() not in old]
    if args.ids_file:
        want = {l.strip() for l in args.ids_file.read_text().splitlines() if l.strip()}
        rows = [r for r in rows if r.get("repo_id") in want or repo_path_of(r) in want]
    if args.sample:
        rows = random.Random(args.seed).sample(rows, min(args.sample, len(rows)))
    return rows


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--root", type=Path, required=True, help="dir that MANIFEST artifact paths are relative to")
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--tmp", type=Path, default=Path(tempfile.gettempdir()))
    ap.add_argument("--sample", type=int, default=0)
    ap.add_argument("--seed", type=int, default=20261117)
    ap.add_argument("--ids-file", type=Path, help="only convert these repo_ids / <user>/<repo> (one per line)")
    ap.add_argument("--only-new", type=Path, metavar="DIR", help="skip repos already present in this github_downloads-style dir")
    ap.add_argument("--verify-sha256", action="store_true")
    ap.add_argument("--timeout", type=int, default=3600)
    args = ap.parse_args(argv)

    rows = select(read_manifest(args.manifest), args)
    args.out.mkdir(parents=True, exist_ok=True)
    write_metadata(rows, args.out / "repo_metadata.tsv")
    log_path = args.out / "convert_log.tsv"
    new_log = not log_path.exists()
    lock = threading.Lock()
    counts: dict[str, int] = {}
    t0 = time.time()
    with log_path.open("a", encoding="utf-8") as log, ThreadPoolExecutor(args.workers) as pool:
        if new_log:
            log.write("\t".join(LOG_FIELDS) + "\n")
        futs = [pool.submit(convert_one, r, args.root, args.out, args.tmp, args.verify_sha256, args.timeout)
                for r in rows]
        for i, fut in enumerate(as_completed(futs), 1):
            rec = fut.result()
            with lock:
                counts[rec["status"]] = counts.get(rec["status"], 0) + 1
                log.write("\t".join(str(rec[f]) for f in LOG_FIELDS) + "\n")
                log.flush()
            if i % 100 == 0 or i == len(futs):
                print(f"[{time.time() - t0:7.1f}s] {i}/{len(futs)} {counts}", file=sys.stderr, flush=True)
    print(f"done in {time.time() - t0:.1f}s: {counts}", file=sys.stderr)
    return 1 if counts.get("fail") else 0


if __name__ == "__main__":
    sys.exit(main())
