#!/usr/bin/env python3
"""Dedup / funnel analysis from compact metadata + tar scan aggregate + report.json."""
from __future__ import annotations

import json
import os
import re
from collections import defaultdict
from pathlib import Path

OUT = Path(os.path.expanduser("~/dataflow_prep_20261111/funnel"))
REPORT = Path("/mnt/data4/cleaning_data/dataflow_runs/full-rules-20261111/report.json")
MANIFEST = Path("/mnt/data4/cleaning_data/dataflow_runs/full-rules-20261111/manifest.json")

VENDOR_RE = re.compile(
    r"(^|/)("
    r"node_modules|vendor|third_party|third-party|3rdparty|3rd_party|"
    r"externals?|external_libs?|deps|Pods|Carthage|"
    r"packages/[^/]+/lib|bower_components|jspm_packages|"
    r"venv|\.venv|site-packages|dist-packages|"
    r"__pycache__|\.gradle|\.m2|"
    r"Pods|"
    r"testdata/vendor"
    r")(/|$)",
    re.I,
)


def is_vendor_path(file_path: str, is_vendor_flag: bool) -> bool:
    if is_vendor_flag:
        return True
    return bool(VENDOR_RE.search(file_path.replace("\\", "/")))


def giB(n: int) -> float:
    return n / 1024**3


def GB(n: int) -> float:
    return n / 1e9


def load_jsonl(path: Path):
    with path.open() as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def analyze():
    with REPORT.open() as f:
        report = json.load(f)
    with MANIFEST.open() as f:
        manifest = json.load(f)

    # Load post-quality and final
    print("loading post_quality.jsonl", flush=True)
    pq = list(load_jsonl(OUT / "post_quality.jsonl"))
    print(f"  n={len(pq)}", flush=True)
    print("loading final_files.jsonl", flush=True)
    fin = list(load_jsonl(OUT / "final_files.jsonl"))
    print(f"  n={len(fin)}", flush=True)

    final_keys = set()
    final_sha1 = set()
    final_by_repo = defaultdict(lambda: {"files": 0, "bytes": 0})
    final_lang = defaultdict(lambda: {"files": 0, "bytes": 0})
    for r in fin:
        key = (r["repo_path"], r["file_path"], r["sha1"])
        final_keys.add(key)
        final_sha1.add(r["sha1"])
        final_by_repo[r["repo_path"]]["files"] += 1
        final_by_repo[r["repo_path"]]["bytes"] += r["size_bytes"]
        final_lang[r["language"]]["files"] += 1
        final_lang[r["language"]]["bytes"] += r["size_bytes"]

    # Group post-quality by sha1
    by_sha = defaultdict(list)
    pq_bytes = 0
    pq_lang = defaultdict(lambda: {"files": 0, "bytes": 0})
    pq_repo = defaultdict(lambda: {"files": 0, "bytes": 0})
    for i, r in enumerate(pq):
        by_sha[r["sha1"]].append(i)
        pq_bytes += r["size_bytes"]
        pq_lang[r["language"]]["files"] += 1
        pq_lang[r["language"]]["bytes"] += r["size_bytes"]
        pq_repo[r["repo_path"]]["files"] += 1
        pq_repo[r["repo_path"]]["bytes"] += r["size_bytes"]

    exact_dropped = []  # indices
    # For each sha1 group of size n, n-1 copies are exact_duplicate.
    # Which copies: any not in final if sha1 is in final; if sha1 not in final,
    # we mark n-1 as exact (arbitrary: all but the first) and 1 as "rep_not_in_final".
    exact_files = 0
    exact_bytes = 0
    exact_same_repo_files = 0
    exact_same_repo_bytes = 0
    exact_cross_repo_files = 0
    exact_cross_repo_bytes = 0
    exact_mixed_group_bytes_as_cross = 0  # bytes of extras in groups that span repos
    exact_lang = defaultdict(lambda: {"files": 0, "bytes": 0})
    # Attribution of exact-loss to repos
    exact_loss_repo = defaultdict(lambda: {"files": 0, "bytes": 0})
    # Unique sha1s whose representative is not in final (near or budget)
    rep_missing = []  # one record per such sha1 (the chosen "rep")
    within_repo_only_groups = 0
    cross_repo_groups = 0
    within_repo_only_extra_files = 0
    within_repo_only_extra_bytes = 0
    cross_repo_extra_files = 0
    cross_repo_extra_bytes = 0
    multi_groups = 0
    multi_files = 0
    multi_bytes = 0  # all copies, not just extras

    vendor_exact_files = 0
    vendor_exact_bytes = 0

    for sha1, idxs in by_sha.items():
        n = len(idxs)
        size = pq[idxs[0]]["size_bytes"]
        repos = {pq[i]["repo_path"] for i in idxs}
        in_final = [i for i in idxs if (pq[i]["repo_path"], pq[i]["file_path"], pq[i]["sha1"]) in final_keys]
        # also consider sha1 present in final at all
        sha_kept = sha1 in final_sha1
        if n > 1:
            multi_groups += 1
            multi_files += n
            multi_bytes += n * size
            extras = n - 1
            extra_bytes = extras * size
            exact_files += extras
            exact_bytes += extra_bytes
            exact_lang[pq[idxs[0]]["language"]]["files"] += extras
            exact_lang[pq[idxs[0]]["language"]]["bytes"] += extra_bytes
            if len(repos) == 1:
                within_repo_only_groups += 1
                within_repo_only_extra_files += extras
                within_repo_only_extra_bytes += extra_bytes
                exact_same_repo_files += extras
                exact_same_repo_bytes += extra_bytes
            else:
                cross_repo_groups += 1
                cross_repo_extra_files += extras
                cross_repo_extra_bytes += extra_bytes
                exact_cross_repo_files += extras
                exact_cross_repo_bytes += extra_bytes
            # attribute extras to repos that are not the kept one when possible
            if in_final:
                kept_set = set(in_final)
            elif sha_kept:
                # sha1 kept but path key didn't match (path rewrite?). attribute to non-matching
                kept_set = set()
            else:
                kept_set = {idxs[0]}  # arbitrary remaining copy is the "rep"
            for i in idxs:
                if i in kept_set:
                    continue
                # if we have a kept copy, all others are exact; if we reserved idxs[0] as rep
                # and sha not in final, only non-reserved are exact
                if not in_final and not sha_kept and i == idxs[0]:
                    continue
                r = pq[i]
                exact_loss_repo[r["repo_path"]]["files"] += 1
                exact_loss_repo[r["repo_path"]]["bytes"] += r["size_bytes"]
                if is_vendor_path(r["file_path"], r.get("is_vendor")):
                    vendor_exact_files += 1
                    vendor_exact_bytes += r["size_bytes"]
                exact_dropped.append(i)
        if not sha_kept:
            # representative of this sha1 did not survive to final
            # choose a record (the first)
            rep_missing.append(pq[idxs[0]])

    # Files / unique sha1s not in final after removing exact extras
    # near+budget candidates: unique sha1s absent from final
    miss_bytes = sum(r["size_bytes"] for r in rep_missing)
    miss_files = len(rep_missing)
    miss_lang = defaultdict(lambda: {"files": 0, "bytes": 0})
    miss_repo = defaultdict(lambda: {"files": 0, "bytes": 0, "json_files": 0, "json_bytes": 0})
    vendor_miss_files = 0
    vendor_miss_bytes = 0
    for r in rep_missing:
        miss_lang[r["language"]]["files"] += 1
        miss_lang[r["language"]]["bytes"] += r["size_bytes"]
        mr = miss_repo[r["repo_path"]]
        mr["files"] += 1
        mr["bytes"] += r["size_bytes"]
        if r["language"] == "JSON":
            mr["json_files"] += 1
            mr["json_bytes"] += r["size_bytes"]
        if is_vendor_path(r["file_path"], r.get("is_vendor")):
            vendor_miss_files += 1
            vendor_miss_bytes += r["size_bytes"]

    # Identify downsample repo: report says 35110 files in 1 repo.
    # Look at miss_repo file counts near 35110, or largest miss.
    ds_target_files = report.get("stages", {}).get("budget", {}).get("reasons", {}).get("repo_downsample", 0)
    # candidates sorted by how close file count is, then by bytes
    ds_ranked = sorted(
        miss_repo.items(),
        key=lambda kv: (abs(kv[1]["files"] - ds_target_files), -kv[1]["bytes"]),
    )
    downsample_guess = None
    if ds_target_files:
        repo, st = ds_ranked[0]
        downsample_guess = {
            "repo_path": repo,
            "missing_rep_files": st["files"],
            "missing_rep_bytes": st["bytes"],
            "target_files": ds_target_files,
            "file_count_delta": st["files"] - ds_target_files,
            "confidence": "high" if abs(st["files"] - ds_target_files) < 50 else "low",
        }

    # JSON budget from report
    json_budget = report.get("byte_budget", {}).get("JSON", {})
    budget_json_files = json_budget.get("dropped_files", 0)
    budget_json_bytes = json_budget.get("dropped_bytes", 0)

    # Split miss into near vs budget vs downsample (estimated when needed)
    # If downsample repo identified with high confidence, assign min(target, files)
    # of that repo's non-JSON? No: downsample is hash-based across all files in that repo
    # after dedup. Near-dup already happened, so downsample files are a subset of
    # that repo's post-dedup files, i.e. they appear in miss as unique sha1s from that repo.
    # If the repo also lost files to near-dup, miss_files for that repo = near + downsample.
    # If file count is very close to 35110, almost all miss in that repo is downsample.

    downsample_files = 0
    downsample_bytes = 0
    downsample_lang = {}
    if downsample_guess and downsample_guess["confidence"] == "high":
        # If miss files in that repo == target, all of them are downsample.
        # If slightly more, leftover is near-dup in that repo (we cannot split bytes
        # exactly without the hash); if equal, exact.
        repo = downsample_guess["repo_path"]
        st = miss_repo[repo]
        if st["files"] == ds_target_files:
            downsample_files = st["files"]
            downsample_bytes = st["bytes"]
            downsample_guess["bytes_measured"] = True
        else:
            downsample_guess["bytes_measured"] = False
            # estimated: average size of miss files in that repo * target
            avg = st["bytes"] / st["files"] if st["files"] else 0
            downsample_files = ds_target_files
            downsample_bytes = int(round(avg * ds_target_files))
            downsample_guess["bytes_estimated_from_avg"] = True
            downsample_guess["estimated_bytes"] = downsample_bytes

    # Remaining miss after removing downsample (by repo) and then JSON budget
    # JSON among remaining miss = near-dup JSON + budget JSON.
    # Budget JSON bytes/files are known from report. Residual JSON = near-dup JSON.

    near_files_report = report["stages"]["dedup"]["reasons"]["near_duplicate"]
    exact_files_report = report["stages"]["dedup"]["reasons"]["exact_duplicate"]

    # After subtracting downsample unique sha1s from miss, leftover unique sha1s
    # should be near_duplicate + byte_budget.
    # near_files_report should equal miss_files - downsample_files - budget_json_files
    # We'll report measured residuals.

    residual_after_ds = miss_files - downsample_files
    residual_bytes_after_ds = miss_bytes - downsample_bytes
    json_miss_files = miss_lang.get("JSON", {}).get("files", 0)
    json_miss_bytes = miss_lang.get("JSON", {}).get("bytes", 0)
    # JSON miss includes JSON that was downsampled (if that repo had JSON)
    json_ds_files = 0
    json_ds_bytes = 0
    if downsample_guess and downsample_guess.get("bytes_measured"):
        st = miss_repo[downsample_guess["repo_path"]]
        json_ds_files = st["json_files"]
        json_ds_bytes = st["json_bytes"]
    json_miss_after_ds_files = json_miss_files - json_ds_files
    json_miss_after_ds_bytes = json_miss_bytes - json_ds_bytes
    # These should equal near_json + budget_json
    near_json_files = json_miss_after_ds_files - budget_json_files
    near_json_bytes = json_miss_after_ds_bytes - budget_json_bytes

    near_files = residual_after_ds - budget_json_files
    near_bytes = residual_bytes_after_ds - budget_json_bytes

    # Language breakdown for near: miss_lang minus downsample (if measured, only one repo)
    # minus JSON budget (subtract from JSON).
    near_lang = {}
    for lang, st in miss_lang.items():
        d = {"files": st["files"], "bytes": st["bytes"]}
        near_lang[lang] = d
    if downsample_guess and downsample_guess.get("bytes_measured"):
        # subtract that repo's miss lang — would need per-lang per-repo. skip exact
        pass
    if "JSON" in near_lang:
        near_lang["JSON"] = {
            "files": max(0, near_lang["JSON"]["files"] - budget_json_files - json_ds_files),
            "bytes": max(0, near_lang["JSON"]["bytes"] - budget_json_bytes - json_ds_bytes),
        }

    # Combined dedup loss per repo = exact_loss + miss (near+budget+ds)
    # For verdict we also want dedup-only (exclude budget). We'll provide both.
    combined_loss = defaultdict(lambda: {
        "exact_files": 0, "exact_bytes": 0,
        "miss_files": 0, "miss_bytes": 0,
        "pq_files": 0, "pq_bytes": 0,
        "final_files": 0, "final_bytes": 0,
        "vendor_miss_files": 0, "vendor_miss_bytes": 0,
        "vendor_exact_files": 0, "vendor_exact_bytes": 0,
    })
    for repo, st in pq_repo.items():
        combined_loss[repo]["pq_files"] = st["files"]
        combined_loss[repo]["pq_bytes"] = st["bytes"]
    for repo, st in final_by_repo.items():
        combined_loss[repo]["final_files"] = st["files"]
        combined_loss[repo]["final_bytes"] = st["bytes"]
    for repo, st in exact_loss_repo.items():
        combined_loss[repo]["exact_files"] = st["files"]
        combined_loss[repo]["exact_bytes"] = st["bytes"]
    for r in rep_missing:
        repo = r["repo_path"]
        combined_loss[repo]["miss_files"] += 1
        combined_loss[repo]["miss_bytes"] += r["size_bytes"]
        if is_vendor_path(r["file_path"], r.get("is_vendor")):
            combined_loss[repo]["vendor_miss_files"] += 1
            combined_loss[repo]["vendor_miss_bytes"] += r["size_bytes"]
    for i in exact_dropped:
        r = pq[i]
        if is_vendor_path(r["file_path"], r.get("is_vendor")):
            combined_loss[r["repo_path"]]["vendor_exact_files"] += 1
            combined_loss[r["repo_path"]]["vendor_exact_bytes"] += r["size_bytes"]

    def looks_like_fork_or_vendor(repo, st):
        flags = []
        vend = st["vendor_exact_bytes"] + st["vendor_miss_bytes"]
        lost = st["exact_bytes"] + st["miss_bytes"]
        if lost and vend / lost >= 0.3:
            flags.append("vendor_paths>=30%_of_loss")
        # name heuristics
        low = repo.lower()
        for tok in ("fork", "copy", "mirror", "backup", "clone"):
            if tok in low:
                flags.append(f"name_contains_{tok}")
        return flags

    top20 = sorted(
        combined_loss.items(),
        key=lambda kv: -(kv[1]["exact_bytes"] + kv[1]["miss_bytes"]),
    )[:20]
    top20_out = []
    for repo, st in top20:
        lost = st["exact_bytes"] + st["miss_bytes"]
        top20_out.append({
            "repo_path": repo,
            **st,
            "lost_files": st["exact_files"] + st["miss_files"],
            "lost_bytes": lost,
            "lost_share_of_pq": (lost / st["pq_bytes"]) if st["pq_bytes"] else None,
            "flags": looks_like_fork_or_vendor(repo, st),
        })

    # Vendor totals among pq and among losses
    vendor_pq_files = vendor_pq_bytes = 0
    for r in pq:
        if is_vendor_path(r["file_path"], r.get("is_vendor")):
            vendor_pq_files += 1
            vendor_pq_bytes += r["size_bytes"]

    # Same-file within one repo vs across: already computed.
    # Also: of exact extra bytes, share that is within-repo-only groups.

    # Validation vs report counts
    validation = {
        "pq_files_loaded": len(pq),
        "pq_files_report_quality_kept": report["stages"]["quality"]["kept"],
        "final_files_loaded": len(fin),
        "final_files_report": report["files_out"],
        "exact_files_computed": exact_files,
        "exact_files_report": exact_files_report,
        "miss_rep_files": miss_files,
        "near_files_computed_residual": near_files,
        "near_files_report": near_files_report,
        "final_bytes_loaded": sum(r["size_bytes"] for r in fin),
        "final_content_bytes_out_report": report.get("content_bytes_out"),
        "pq_bytes": pq_bytes,
    }

    out = {
        "validation": validation,
        "post_quality": {
            "files": len(pq),
            "bytes": pq_bytes,
            "unique_sha1": len(by_sha),
            "vendor_files": vendor_pq_files,
            "vendor_bytes": vendor_pq_bytes,
            "top_langs": [
                {"language": k, **v}
                for k, v in sorted(pq_lang.items(), key=lambda kv: -kv[1]["bytes"])[:30]
            ],
        },
        "exact_duplicate": {
            "files": exact_files,
            "bytes": exact_bytes,
            "multi_groups": multi_groups,
            "within_repo_only_groups": within_repo_only_groups,
            "within_repo_only_extra_files": within_repo_only_extra_files,
            "within_repo_only_extra_bytes": within_repo_only_extra_bytes,
            "cross_repo_groups": cross_repo_groups,
            "cross_repo_extra_files": cross_repo_extra_files,
            "cross_repo_extra_bytes": cross_repo_extra_bytes,
            "within_repo_share_of_exact_bytes": (
                within_repo_only_extra_bytes / exact_bytes if exact_bytes else None
            ),
            "cross_repo_share_of_exact_bytes": (
                cross_repo_extra_bytes / exact_bytes if exact_bytes else None
            ),
            "vendor_files": vendor_exact_files,
            "vendor_bytes": vendor_exact_bytes,
            "top_langs": [
                {"language": k, **v}
                for k, v in sorted(exact_lang.items(), key=lambda kv: -kv[1]["bytes"])[:25]
            ],
        },
        "rep_missing_from_final": {
            "files": miss_files,
            "bytes": miss_bytes,
            "note": "unique sha1s present after quality but absent in final; this is near-dup + byte_budget + repo_downsample",
            "vendor_files": vendor_miss_files,
            "vendor_bytes": vendor_miss_bytes,
            "top_langs": [
                {"language": k, **v}
                for k, v in sorted(miss_lang.items(), key=lambda kv: -kv[1]["bytes"])[:25]
            ],
            "top_repos": [
                {"repo_path": k, **v}
                for k, v in sorted(miss_repo.items(), key=lambda kv: -kv[1]["bytes"])[:20]
            ],
        },
        "split_of_rep_missing": {
            "downsample_guess": downsample_guess,
            "downsample_files": downsample_files,
            "downsample_bytes": downsample_bytes,
            "budget_json_files_from_report": budget_json_files,
            "budget_json_bytes_from_report": budget_json_bytes,
            "near_files_residual": near_files,
            "near_bytes_residual": near_bytes,
            "near_json_files_residual": near_json_files,
            "near_json_bytes_residual": near_json_bytes,
            "near_top_langs_approx": [
                {"language": k, **v}
                for k, v in sorted(near_lang.items(), key=lambda kv: -kv[1]["bytes"])[:25]
            ],
            "arithmetic_check": {
                "miss_files": miss_files,
                "ds_plus_budget_plus_near": downsample_files + budget_json_files + near_files,
                "miss_bytes": miss_bytes,
                "ds_plus_budget_plus_near_bytes": downsample_bytes + budget_json_bytes + near_bytes,
                "near_files_vs_report": near_files - near_files_report,
                "exact_files_vs_report": exact_files - exact_files_report,
            },
        },
        "top20_repos_by_bytes_lost_after_quality": top20_out,
        "final": {
            "files": len(fin),
            "bytes": validation["final_bytes_loaded"],
            "top_langs": [
                {"language": k, **v}
                for k, v in sorted(final_lang.items(), key=lambda kv: -kv[1]["bytes"])[:30]
            ],
        },
        "levers": {
            "exact_bytes": exact_bytes,
            "near_bytes_residual": near_bytes,
            "if_disable_cross_repo_exact_only_keep_within": {
                "bytes_that_are_cross_repo_exact": cross_repo_extra_bytes,
                "note": "Turning off cross-repo exact dedup would keep these extra copies; within-repo exact dups would still drop.",
            },
            "if_disable_all_near_dup": {
                "files": near_files,
                "bytes": near_bytes,
                "note": "Measured residual after removing exact, JSON budget (report), and downsample (if identified).",
            },
        },
    }

    dest = OUT / "dedup_analysis.json"
    with dest.open("w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print("wrote", dest, flush=True)
    print(json.dumps(validation, indent=2), flush=True)
    print("exact_bytes", exact_bytes, "GB", GB(exact_bytes), flush=True)
    print("miss_bytes", miss_bytes, "GB", GB(miss_bytes), flush=True)
    print("near_bytes_residual", near_bytes, "GB", GB(near_bytes), flush=True)
    print("within_share", out["exact_duplicate"]["within_repo_share_of_exact_bytes"], flush=True)
    print("downsample_guess", downsample_guess, flush=True)


if __name__ == "__main__":
    analyze()
