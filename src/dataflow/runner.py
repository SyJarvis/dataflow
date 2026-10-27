"""Pipeline orchestrator: parallel per-tarball cleaning + two-pass global dedup.

Architecture (for the 10k-tarball scale):

    [Pool of N workers]                  each tarball -> cleaned repo records
        |  writes runs/<name>/intermediate/repos.ndjson   (one line = one repo)
        |  returns MinHash signatures (256 x uint32) per kept file
        v
    [Main process, pass 1]  incremental MinHash-LSH insert + query,
                            union-find clusters  ->  drop-set of sha1s
    [Main process, pass 2]  stream intermediate -> filter dropped sha1s
                            -> runs/<name>/repos.jsonl

Resume: atomic per-tar checkpoints are authoritative. The manifest and
combined NDJSON files are rebuilt from them in stable input-path order.

Run layout (DataForge-inspired):

    runs/<name>/
      manifest.json
      intermediate/
        repos.ndjson        # per-repo cleaned records, pre-dedup
        signatures.ndjson   # {"sha1": ..., "sig": base64(256*uint32)}
      repos.jsonl           # final grouped output
      report.json
"""

from __future__ import annotations

import base64
import hashlib
import json
from pathlib import Path


from .models import FileRecord, RepoRecord, StageStats
from . import unpack, quality as q_mod, licenses as lic_mod, notebooks as nb_mod
from . import activity as act_mod
from .languages import EnryDetector, decode_bytes
from .dedup import minhash_signature, UnionFind

try:
    from datasketch import MinHash, MinHashLSH
    import numpy as np
    _HAS_DATASKETCH = True
except ImportError:  # pragma: no cover
    _HAS_DATASKETCH = False


# ---------------------------------------------------------------------------
# Worker: clean one tarball end-to-end (unpack -> languages -> quality -> ...)
# ---------------------------------------------------------------------------

def _clean_one_tar(args):
    tar_s, cfg = args
    tar = Path(tar_s)
    repo_path = unpack.repo_path_from_tarball(tar)
    out = {
        "_tar": tar_s,
        "repo_path": repo_path, "failed": None, "records": [],
        "sigs": [], "stats": [], "activity": None, "evidence": 0,
        "dup_within": 0,
    }

    import tarfile as _tf
    raws: list = []
    st = StageStats(stage="unpack")
    max_repo_files = int(cfg["unpack"]["max_repo_files"])
    try:
        with _tf.open(tar, "r:*") as tf:
            for m in tf:
                # Match iter_tarball_files: count all tar members before any
                # filtering, not just retained files. Exactly the cap is OK.
                st.seen += 1
                if st.seen > max_repo_files:
                    raws.clear()
                    st.drop("repo_exceeds_max_files")
                    out["stats"].append(st.__dict__)
                    out["drop_reason"] = "repo_exceeds_max_files"
                    return out  # No records/signatures from a partial repo.
                if not m.isfile():
                    st.drop("symlink_or_nonregular")
                    continue
                if m.size > int(cfg["unpack"]["max_file_bytes"]):
                    st.drop("oversize_file")
                    continue
                fobj = tf.extractfile(m)
                if fobj is None:
                    st.drop("nonregular")
                    continue
                with fobj:
                    data = fobj.read()
                if bool(cfg["unpack"]["drop_binary"]) and b"\x00" in data[:65536]:
                    st.drop("binary_null_byte")
                    continue
                name = unpack._normalize_member_name(m.name)
                if name is None:
                    st.drop("empty_name")
                    continue
                raws.append((name, data))
    except Exception as e:
        out["failed"] = f"{type(e).__name__}: {e}"
        return out

    # strip common top-level wrapper dir
    segs = {n.split("/", 1)[0] + "/" for n, _ in raws if "/" in n}
    if len(segs) == 1 and any("/" in n for n, _ in raws):
        prefix = next(iter(segs))
        raws = [(n[len(prefix):] if n.startswith(prefix) else n, d) for n, d in raws]

    st.kept = len(raws)
    out["stats"].append(st.__dict__)

    enry = EnryDetector(cfg["languages"].get("go_enry_bin", ""))
    import hashlib as _h
    recs: list[FileRecord] = []
    st = StageStats(stage="languages")
    for name, data in raws:
        st.seen += 1
        text = decode_bytes(data)
        if text is None:
            st.drop("undecodable"); continue
        rec = FileRecord(repo_path=repo_path, file_path=name,
                         size_bytes=len(data), sha1=_h.sha1(data).hexdigest(),
                         content=text)
        rec.language, rec.is_vendor = enry.detect(name, text)
        recs.append(rec); st.kept += 1
    out["stats"].append(st.__dict__)

    if cfg["run"]["steps"].get("licenses", False):
        recs, s = lic_mod.annotate_repo_licenses(recs)
        out["stats"].append(s.__dict__)
        if cfg["licenses"].get("drop_non_permissive", True):
            recs, s = lic_mod.drop_non_permissive(recs)
            out["stats"].append(s.__dict__)

    if cfg["run"]["steps"].get("quality", True):
        recs, s = q_mod.filter_files(recs, cfg["quality"])
        out["stats"].append(s.__dict__)

    if cfg["run"]["steps"].get("notebooks", True):
        recs, s = nb_mod.process_notebooks(recs, cfg["notebooks"])
        out["stats"].append(s.__dict__)

    # activity-year annotation (content evidence)
    year, n_ev = act_mod.repo_activity_year(recs)
    out["activity"] = year
    out["evidence"] = n_ev

    # MinHash signatures for global dedup (one signature per unique sha1;
    # duplicates inside this tarball are counted, not re-signed)
    if cfg["run"]["steps"].get("dedup", True) and _HAS_DATASKETCH:
        perms = int(cfg["dedup"]["minhash_permutations"])
        seen: set = set()
        for rec in recs:
            if rec.sha1 in seen:
                out["dup_within"] += 1
                continue
            seen.add(rec.sha1)
            mh = minhash_signature(rec.content, perms)
            if mh is not None:
                out["sigs"].append((rec.sha1,
                                    base64.b64encode(mh.digest().tobytes()).decode()))

    out["records"] = [r.to_json() for r in recs]
    return out


# ---------------------------------------------------------------------------
# Main orchestration
# ---------------------------------------------------------------------------

def config_fingerprint(cfg: dict) -> str:
    blob = json.dumps(cfg, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def discover_tarballs(input_dir: Path) -> list[Path]:
    seen, out = set(), []
    for t in sorted(input_dir.glob("*/*.tar.gz")) + sorted(input_dir.glob("*/*.tgz")):
        if t not in seen:
            seen.add(t); out.append(t)
    return out


def _agg(stats_all: dict, st_dict: dict):
    stage = st_dict["stage"]
    cur = stats_all.setdefault(stage, {"stage": stage, "seen": 0, "kept": 0,
                                       "dropped": 0, "reasons": {}})
    cur["seen"] += st_dict["seen"]; cur["kept"] += st_dict["kept"]
    cur["dropped"] += st_dict["dropped"]
    for k, v in st_dict["reasons"].items():
        cur["reasons"][k] = cur["reasons"].get(k, 0) + v


def run_pipeline(config_path: Path, resume: bool = False) -> dict:
    """Run the checkpointed pipeline; legacy append-only runs remain untouched."""
    from .execution import run_pipeline as execute
    return execute(config_path, resume=resume)
