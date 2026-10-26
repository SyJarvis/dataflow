"""Pipeline orchestrator with resume support.

Run layout (borrowed from DataForge's runs/ convention):

    runs/<name>/
      manifest.json     # config fingerprint + processed tarball list (resume)
      repos.jsonl       # grouped output (one repo per line)
      dropped.jsonl     # dropped file records with reasons
      report.json       # per-stage stats
"""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

import yaml

from .models import RepoRecord, StageStats
from . import unpack, languages as lang_mod, licenses as lic_mod, quality as q_mod
from . import notebooks as nb_mod, pii as pii_mod, dedup as dedup_mod, group as group_mod


def config_fingerprint(cfg: dict) -> str:
    blob = json.dumps(cfg, sort_keys=True, ensure_ascii=False).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def discover_tarballs(input_dir: Path) -> list[Path]:
    tars = sorted(input_dir.glob("*/*.tar.gz"))
    tars += sorted(input_dir.glob("*/*.tgz"))
    # de-dup preserve order
    seen, out = set(), []
    for t in tars:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def run_pipeline(config_path: Path, resume: bool = False) -> dict:
    cfg = yaml.safe_load(open(config_path, encoding="utf-8"))
    rcfg = cfg["run"]
    steps = rcfg.get("steps", {})
    out_dir = (config_path.parent / rcfg["output_dir"] / rcfg["name"]).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = out_dir / "manifest.json"
    manifest = {"fingerprint": config_fingerprint(cfg), "processed": []}
    if resume and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        if manifest["fingerprint"] != config_fingerprint(cfg):
            raise SystemExit(
                "config changed since last run; use a new run name or drop --resume")

    tarballs = discover_tarballs(Path(rcfg["input_dir"]))
    if int(rcfg.get("max_tarballs", 0)) > 0:
        tarballs = tarballs[: int(rcfg["max_tarballs"])]

    enry = lang_mod.EnryDetector(cfg["languages"].get("go_enry_bin", ""))
    all_records: list = []
    stats_all: dict[str, StageStats] = {}
    repos_meta: list[RepoRecord] = []
    t0 = time.time()

    def accumulate(stats: StageStats):
        if stats.stage in stats_all:
            s = stats_all[stats.stage]
            s.seen += stats.seen; s.kept += stats.kept; s.dropped += stats.dropped
            for k, v in stats.reasons.items():
                s.reasons[k] = s.reasons.get(k, 0) + v
        else:
            stats_all[stats.stage] = stats

    for tar in tarballs:
        if str(tar) in manifest["processed"]:
            continue  # resume: skip done tarballs
        if str(tar) in manifest.get("failed", {}):
            continue  # already known-bad
        repo_path = unpack.repo_path_from_tarball(tar)

        # ① unpack (streaming; raw bytes kept in memory per repo)
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
                    raws.append((m.name, data))
        except Exception as e:
            # corrupted / interrupted download (e.g. HTML error page saved as .tar.gz):
            # record and move on, never abort the whole run
            manifest.setdefault("failed", {})[str(tar)] = f"{type(e).__name__}: {e}"
            manifest["processed"].append(str(tar))
            continue
        # strip the common top-level dir wrapper ("<repo>-<sha>/") GitHub adds
        first_segs = {n.split("/", 1)[0] + "/" for n, _ in raws if "/" in n}
        if len(first_segs) == 1 and all(n.split("/", 1)[0] + "/" == list(first_segs)[0] for n, _ in raws if "/" in n) and any("/" in n for n, _ in raws):
            prefix = list(first_segs)[0]
            raws = [(n[len(prefix):] if n.startswith(prefix) else n, d) for n, d in raws]
        it = None; st = None  # stats computed from raws below
        st_unpack = StageStats(stage="unpack")
        st_unpack.seen = st_unpack.kept = len(raws)
        accumulate(st_unpack)
        if not raws:
            manifest["processed"].append(str(tar))
            continue

        # ② decode + language
        import hashlib as _h
        recs = []
        lang_stats = StageStats(stage="languages")
        for name, data in raws:
            lang_stats.seen += 1
            text = lang_mod.decode_bytes(data)
            if text is None:
                lang_stats.drop("undecodable")
                continue
            inner = unpack._normalize_member_name(name)
            from .models import FileRecord
            rec = FileRecord(
                repo_path=repo_path, file_path=inner, size_bytes=len(data),
                sha1=_h.sha1(data).hexdigest(), content=text,
            )
            rec.language, rec.is_vendor = enry.detect(rec.file_path, text)
            recs.append(rec)
            lang_stats.kept += 1
        accumulate(lang_stats)

        # ③ licenses (optional)
        if steps.get("licenses", False):
            recs, st = lic_mod.annotate_repo_licenses(recs)
            accumulate(st)
            if cfg["licenses"].get("drop_non_permissive", True):
                recs, st = lic_mod.drop_non_permissive(recs)
                accumulate(st)

        # ④ quality
        if steps.get("quality", True):
            recs, st = q_mod.filter_files(recs, cfg["quality"])
            accumulate(st)

        # ⑤ notebooks
        if steps.get("notebooks", True):
            recs, st = nb_mod.process_notebooks(recs, cfg["notebooks"])
            accumulate(st)

        all_records.extend(recs)
        manifest["processed"].append(str(tar))

        # periodic manifest flush so resume works on crash
        manifest_path.write_text(json.dumps(manifest, indent=1))

    # ⑥ PII (optional, global pass)
    if steps.get("pii", False):
        all_records, st, _model = pii_mod.redact_files(all_records)
        accumulate(st)

    # ⑦ global near-dedup (cross-repo, cross-language)
    if steps.get("dedup", True):
        all_records, st = dedup_mod.dedup_files(all_records, cfg["dedup"])
        accumulate(st)

    # ⑧ group by repo + export
    by_repo: dict[str, list] = {}
    for rec in all_records:
        by_repo.setdefault(rec.repo_path, []).append(rec)
    repos = [group_mod.group_by_repo(rp, recs) for rp, recs in sorted(by_repo.items())]

    fmt = cfg["group"].get("format", "jsonl")
    if steps.get("group", True):
        if fmt == "parquet":
            try:
                group_mod.write_parquet(repos, out_dir / "repos.parquet")
            except ImportError:
                group_mod.write_jsonl(repos, out_dir / "repos.jsonl")
        else:
            group_mod.write_jsonl(repos, out_dir / "repos.jsonl")

    manifest_path.write_text(json.dumps(manifest, indent=1))

    report = {
        "run_name": rcfg["name"],
        "tarballs_seen": len(tarballs),
        "tarballs_processed": len(manifest["processed"]),
        "tarballs_failed": len(manifest.get("failed", {})),
        "repos_out": len(repos),
        "files_out": sum(r.num_files for r in repos),
        "elapsed_sec": round(time.time() - t0, 1),
        "stages": {k: v.__dict__ for k, v in stats_all.items()},
    }
    (out_dir / "report.json").write_text(json.dumps(report, indent=1, ensure_ascii=False))
    return report
