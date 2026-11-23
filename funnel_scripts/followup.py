#!/usr/bin/env python3
import json
import re
from collections import defaultdict
from pathlib import Path

OUT = Path.home() / "dataflow_prep_20261111/funnel"

VENDOR_RE = re.compile(
    r"(^|/)(node_modules|vendor|third_party|third-party|3rdparty|3rd_party|externals?|bower_components|Pods|site-packages)(/|$)",
    re.I,
)


def load(path):
    with path.open() as f:
        for line in f:
            if line.strip():
                yield json.loads(line)


def main():
    print("loading", flush=True)
    fin = list(load(OUT / "final_files.jsonl"))
    pq = list(load(OUT / "post_quality.jsonl"))
    print("loaded", len(pq), len(fin), flush=True)

    final_sha1 = {r["sha1"] for r in fin}
    final_repo = defaultdict(lambda: {"files": 0, "bytes": 0})
    for r in fin:
        final_repo[r["repo_path"]]["files"] += 1
        final_repo[r["repo_path"]]["bytes"] += r["size_bytes"]

    by_sha = defaultdict(list)
    for i, r in enumerate(pq):
        by_sha[r["sha1"]].append(i)

    miss = []
    for sha1, idxs in by_sha.items():
        if sha1 not in final_sha1:
            miss.append(pq[idxs[0]])

    miss_repo = defaultdict(lambda: {"files": 0, "bytes": 0, "json_files": 0, "json_bytes": 0})
    for r in miss:
        st = miss_repo[r["repo_path"]]
        st["files"] += 1
        st["bytes"] += r["size_bytes"]
        if r["language"] == "JSON":
            st["json_files"] += 1
            st["json_bytes"] += r["size_bytes"]

    rows = []
    repos = set(list(final_repo) + list(miss_repo))
    for repo in repos:
        f = final_repo[repo]
        m = miss_repo[repo]
        post = f["bytes"] + m["bytes"]
        rows.append((post, f["bytes"], m["bytes"], f["files"], m["files"], m["json_files"], m["json_bytes"], repo))
    rows.sort(reverse=True)

    top = []
    for post, fb, mb, ff, mf, mjf, mjb, repo in rows[:30]:
        top.append({
            "repo_path": repo,
            "post_exact_unique_bytes": post,
            "final_bytes": fb,
            "miss_bytes": mb,
            "final_files": ff,
            "miss_files": mf,
            "miss_json_files": mjf,
            "miss_json_bytes": mjb,
            "nonjson_miss_files": mf - mjf,
            "nonjson_miss_bytes": mb - mjb,
        })

    TH = 1610612736
    DS = 35110
    cands = []
    for post, fb, mb, ff, mf, mjf, mjb, repo in rows:
        nonjson_m = mf - mjf
        if not (post >= 0.3 * TH or abs(nonjson_m - DS) < 5000 or abs(mf - DS) < 2000):
            continue
        cands.append({
            "repo_path": repo,
            "post_exact_unique_bytes": post,
            "final_bytes": fb,
            "miss_bytes": mb,
            "final_files": ff,
            "miss_files": mf,
            "nonjson_miss_files": nonjson_m,
            "nonjson_miss_bytes": mb - mjb,
            "miss_json_bytes": mjb,
            "delta_nonjson_to_35110": nonjson_m - DS,
            "delta_miss_to_35110": mf - DS,
            "over_1_5gib_post_exact": post >= TH,
            "over_1_5gib_final_plus_nonjson_miss": (fb + mb - mjb) >= TH,
        })
    cands.sort(key=lambda d: (abs(d["delta_nonjson_to_35110"]), -d["post_exact_unique_bytes"]))

    final_by_path = defaultdict(list)
    for r in fin:
        final_by_path[r["file_path"]].append(r)

    same_path_other_repo = 0
    same_path_other_repo_bytes = 0
    no_path_match = 0
    no_path_match_bytes = 0
    vendor = 0
    vendor_bytes = 0
    samples = []
    non_json_miss = [r for r in miss if r["language"] != "JSON"]
    for r in non_json_miss:
        path = r["file_path"]
        others = [x for x in final_by_path.get(path, []) if x["repo_path"] != r["repo_path"]]
        if others:
            same_path_other_repo += 1
            same_path_other_repo_bytes += r["size_bytes"]
            if len(samples) < 25:
                others_sorted = sorted(others, key=lambda x: abs(x["size_bytes"] - r["size_bytes"]))
                kept = others_sorted[0]
                samples.append({
                    "dropped_repo": r["repo_path"],
                    "kept_repo": kept["repo_path"],
                    "file_path": path,
                    "language": r["language"],
                    "dropped_sha1": r["sha1"],
                    "kept_sha1": kept["sha1"],
                    "dropped_bytes": r["size_bytes"],
                    "kept_bytes": kept["size_bytes"],
                    "size_ratio": (r["size_bytes"] / kept["size_bytes"]) if kept["size_bytes"] else None,
                    "n_other_repos_with_same_path_in_final": len({x["repo_path"] for x in others}),
                    "vendor_path": bool(VENDOR_RE.search(path.replace("\\", "/"))),
                })
        else:
            no_path_match += 1
            no_path_match_bytes += r["size_bytes"]
        if r.get("is_vendor") or VENDOR_RE.search(path.replace("\\", "/")):
            vendor += 1
            vendor_bytes += r["size_bytes"]

    def ext_of(p):
        b = p.lower().rsplit("/", 1)[-1]
        return b.rsplit(".", 1)[-1] if "." in b else ""

    ts_pq = ts_fin = ts_pq_b = ts_fin_b = 0
    video_ext = {"mp4", "mkv", "avi", "mov", "webm", "flv", "wmv", "m4v", "mpeg", "mpg", "m2ts", "mts", "3gp", "ogv", "vob", "m4p"}
    vid_pq = vid_pq_b = 0
    for r in pq:
        e = ext_of(r["file_path"])
        if e == "ts":
            ts_pq += 1
            ts_pq_b += r["size_bytes"]
        if e in video_ext:
            vid_pq += 1
            vid_pq_b += r["size_bytes"]
    for r in fin:
        e = ext_of(r["file_path"])
        if e == "ts":
            ts_fin += 1
            ts_fin_b += r["size_bytes"]

    out = {
        "top_repos_by_post_exact_unique_bytes": top,
        "downsample_candidates": cands[:15],
        "near_path_heuristic_non_json_miss": {
            "non_json_miss_files": len(non_json_miss),
            "non_json_miss_bytes": sum(r["size_bytes"] for r in non_json_miss),
            "same_path_in_other_repo_final_files": same_path_other_repo,
            "same_path_in_other_repo_final_bytes": same_path_other_repo_bytes,
            "no_same_path_in_final_files": no_path_match,
            "no_same_path_in_final_bytes": no_path_match_bytes,
            "vendor_files": vendor,
            "vendor_bytes": vendor_bytes,
            "note": "Non-JSON unique-sha1 files absent from final (near-dup + downsample, not JSON budget). Same file_path in another repo is a fork/copy signal, not a true Jaccard.",
            "samples_same_path": samples,
        },
        "extension_notes": {
            "post_quality_dot_ts_files": ts_pq,
            "post_quality_dot_ts_bytes": ts_pq_b,
            "final_dot_ts_files": ts_fin,
            "final_dot_ts_bytes": ts_fin_b,
            "post_quality_real_video_ext_files": vid_pq,
            "post_quality_real_video_ext_bytes": vid_pq_b,
        },
    }
    dest = OUT / "followup.json"
    with dest.open("w") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    print("wrote", dest, flush=True)
    for row in top[:8]:
        print("TOP", row["repo_path"], row["post_exact_unique_bytes"], "final", row["final_bytes"], "miss", row["miss_files"], row["miss_bytes"], flush=True)
    print("ds cands", len(cands), flush=True)
    for c in cands[:8]:
        print("CAND", c["repo_path"], "d_nonjson", c["delta_nonjson_to_35110"], "post", c["post_exact_unique_bytes"], "final", c["final_bytes"], flush=True)
    h = out["near_path_heuristic_non_json_miss"]
    print("near path", h["same_path_in_other_repo_final_files"], "/", h["non_json_miss_files"], "bytes", h["same_path_in_other_repo_final_bytes"], flush=True)
    print("ext ts_pq", ts_pq, ts_pq_b, "vid", vid_pq, vid_pq_b, flush=True)


if __name__ == "__main__":
    main()
