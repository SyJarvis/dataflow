"""Pipeline orchestrator: parallel per-tarball cleaning + two-pass global dedup.

Architecture (for the 10k-tarball scale):

    [Pool of N workers]                  each tarball -> cleaned repo records
        |  writes runs/<name>/intermediate/repos.ndjson   (one line = one repo)
        |  returns MinHash signatures (256 x uint64) per kept file
        v
    [Main process, pass 1]  incremental MinHash-LSH insert + query,
                            union-find clusters  ->  drop-set of sha1s
    [Main process, pass 2]  stream intermediate -> filter dropped sha1s
                            -> runs/<name>/repos.jsonl

Resume: manifest.json records processed/failed tarballs; signatures are
appended to intermediate/signatures.ndjson so a resumed run reloads them
before continuing.

Run layout (DataForge-inspired):

    runs/<name>/
      manifest.json
      intermediate/
        repos.ndjson        # per-repo cleaned records, pre-dedup
        signatures.ndjson   # {"sha1": ..., "sig": base64(256*uint64)}
      repos.jsonl           # final grouped output
      report.json
"""

from __future__ import annotations

import base64
import hashlib
import json
import multiprocessing as mp
import time
from pathlib import Path

import yaml

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
    try:
        with _tf.open(tar, "r:*") as tf:
            for m in tf:
                if not m.isfile() or m.size > int(cfg["unpack"]["max_file_bytes"]):
                    continue
                fobj = tf.extractfile(m)
                if fobj is None:
                    continue
                data = fobj.read()
                if bool(cfg["unpack"]["drop_binary"]) and b"\x00" in data[:65536]:
                    continue
                name = unpack._normalize_member_name(m.name)
                if name is None:
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

    st = StageStats(stage="unpack"); st.seen = st.kept = len(raws)
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
    cfg = yaml.safe_load(open(config_path, encoding="utf-8"))
    rcfg = cfg["run"]
    steps = rcfg.get("steps", {})
    out_dir = (config_path.parent / rcfg["output_dir"] / rcfg["name"]).resolve()
    inter = out_dir / "intermediate"
    inter.mkdir(parents=True, exist_ok=True)
    manifest_path = out_dir / "manifest.json"

    manifest = {"fingerprint": config_fingerprint(cfg), "processed": [],
                "failed": {}, "dup_within_total": 0}
    if resume and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest["fingerprint"] != config_fingerprint(cfg):
            raise SystemExit("config changed since last run; "
                             "use a new run name or drop --resume")
    dup_within_total = int(manifest.get("dup_within_total", 0))

    tarballs = discover_tarballs(Path(rcfg["input_dir"]))
    if int(rcfg.get("max_tarballs", 0)) > 0:
        tarballs = tarballs[: int(rcfg["max_tarballs"])]

    done = set(manifest["processed"])
    failed = manifest["failed"]
    todo = [t for t in tarballs if str(t) not in done and str(t) not in failed]

    workers = int(rcfg.get("workers", 8))
    t0 = time.time()
    stats_all: dict = {}   # NOTE: on --resume only newly processed tarballs are
    n_done = 0             # counted here; pass-2 output stats are always global

    # reload signatures from a previous (resumed) run
    sig_path = inter / "signatures.ndjson"
    sig_entries: list = []
    if sig_path.exists():
        for line in sig_path.open():
            d = json.loads(line)
            sig_entries.append((d["sha1"], d["sig"]))

    repos_file = inter / "repos.ndjson"
    sig_file = sig_path.open("a", encoding="utf-8")

    def flush_manifest():
        manifest["processed"] = list(done)
        manifest["failed"] = failed
        manifest["dup_within_total"] = dup_within_total
        manifest_path.write_text(json.dumps(manifest, indent=1))

    if todo:
        ctx = mp.get_context("fork")
        with ctx.Pool(workers) as pool, repos_file.open("a", encoding="utf-8") as rf:
            for result in pool.imap_unordered(
                    _clean_one_tar, ((str(t), cfg) for t in todo), chunksize=4):
                n_done += 1
                done.add(result["_tar"])
                if result["failed"]:
                    failed[result["_tar"]] = result["failed"]
                else:
                    rf.write(json.dumps(result, ensure_ascii=False) + "\n")
                    for s in result["stats"]:
                        _agg(stats_all, s)
                    dup_within_total += result.get("dup_within", 0)
                    for sha1, b64 in result["sigs"]:
                        sig_entries.append((sha1, b64))
                        sig_file.write(json.dumps({"sha1": sha1, "sig": b64}) + "\n")
                if n_done % 50 == 0:
                    flush_manifest()
                if n_done % 500 == 0:
                    print(f"[dataflow] {n_done}/{len(todo)} tarballs "
                          f"({time.time()-t0:.0f}s)", flush=True)
        flush_manifest()

    # ---- global near-dedup (pass 1: clusters -> drop set) ----
    drop_sha: set = set()
    if steps.get("dedup", True) and _HAS_DATASKETCH:
        st = {"stage": "dedup", "seen": 0, "kept": 0, "dropped": 0, "reasons": {}}
        perms = int(cfg["dedup"]["minhash_permutations"])
        threshold = float(cfg["dedup"]["lsh_jaccard_threshold"])
        lsh = MinHashLSH(threshold=threshold, num_perm=perms)
        sig_of: dict = {}
        uf = UnionFind()
        idx_of: dict = {}
        order: list = []
        inserted = 0
        for sha1, b64 in sig_entries:
            if sha1 in idx_of:
                st["dropped"] += 1
                st["reasons"]["exact_duplicate"] = st["reasons"].get("exact_duplicate", 0) + 1
                continue  # exact duplicate across repos: drop later occurrence
            arr = np.frombuffer(base64.b64decode(b64), dtype=np.uint32)
            mh = MinHash(num_perm=perms, hashvalues=arr, scheme="affine32")
            idx_of[sha1] = len(order)
            sig_of[sha1] = mh
            order.append(sha1)
            peers = [p for p in lsh.query(mh) if p != sha1]
            lsh.insert(sha1, mh)
            for p in peers:
                if cfg["dedup"].get("verify_jaccard", True):
                    if mh.jaccard(sig_of[p]) < threshold:
                        continue
                uf.union(idx_of[sha1], idx_of[p])
            inserted += 1
            if inserted % 100000 == 0:
                print(f"[dataflow] dedup index {inserted}", flush=True)

        best: dict = {}
        for sha1 in order:
            root = uf.find(idx_of[sha1])
            if root not in best:
                best[root] = sha1
        keep_sha = set(best.values())
        drop_sha = {s for s in order if s not in keep_sha}
        st["seen"] = len(order) + st["dropped"] + dup_within_total
        st["dropped"] += dup_within_total
        st["reasons"]["exact_duplicate"] = (st["reasons"].get("exact_duplicate", 0)
                                            + dup_within_total)
        st["kept"] = len(keep_sha)
        st["reasons"]["near_duplicate"] = st["reasons"].get("near_duplicate", 0) + len(drop_sha)
        stats_all["dedup"] = st

    # ---- pass 2: filter + group + final output ----
    repos_out = 0
    files_out = 0
    active_2026 = 0
    activity_hist: dict = {}
    seen_sha: set = set()   # content already emitted (exact-dup copies dropped)
    final_path = out_dir / "repos.jsonl"
    with repos_file.open("r", encoding="utf-8") as rf, \
            open(final_path, "w", encoding="utf-8") as wf:
        for line in rf:
            d = json.loads(line)
            files = []
            for f in d["records"]:
                if f["sha1"] in drop_sha or f["sha1"] in seen_sha:
                    continue
                seen_sha.add(f["sha1"])
                files.append(f)
            rr = RepoRecord(
                repo_path=d["repo_path"], num_files=len(files), files=files,
                last_activity_year=d.get("activity"),
                evidence_files=d.get("evidence", 0))
            wf.write(json.dumps(rr.__dict__, ensure_ascii=False) + "\n")
            repos_out += 1
            files_out += len(files)
            y = rr.last_activity_year
            activity_hist[str(y)] = activity_hist.get(str(y), 0) + 1
            if y == 2026:
                active_2026 += 1

    report = {
        "run_name": rcfg["name"],
        "workers": workers,
        "tarballs_seen": len(tarballs),
        "tarballs_processed": len(done),
        "tarballs_failed": len(failed),
        "repos_out": repos_out,
        "files_out": files_out,
        "repos_active_2026": active_2026,
        "activity_year_hist": dict(sorted(activity_hist.items())),
        "elapsed_sec": round(time.time() - t0, 1),
        "stages": stats_all,
    }
    (out_dir / "report.json").write_text(
        json.dumps(report, indent=1, ensure_ascii=False))
    return report
