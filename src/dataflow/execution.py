"""Checkpointed execution. A durable per-input shard is the commit record.

Manifests and combined NDJSON files are derived from shards. A crash after a
shard rename but before a manifest update therefore cannot lose that result.
Legacy append-only runs are intentionally not resumed by this implementation.
"""

import base64
import hashlib
from importlib.metadata import version, PackageNotFoundError
import json
import multiprocessing as mp
from pathlib import Path
import platform
import time

import yaml

from .budget import apply_byte_budget, keep_in_downsample, unit_hash
from .dedup import REPRESENTATIVE_POLICY, load_repo_metadata, repo_priority_key
from .models import RepoRecord
from .storage import atomic_json, atomic_text, identity, run_lock


SCHEMA_VERSION = 2
LARGE_REPO_BYTES = 1_610_612_736


def source_fingerprint() -> str:
    digest = hashlib.sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    digest.update(platform.python_version().encode())
    for name in ("PyYAML", "datasketch", "numpy", "cchardet"):
        try:
            installed = version(name)
        except PackageNotFoundError:
            installed = "absent"
        digest.update(f"{name}={installed}".encode())
    return digest.hexdigest()


def _inputs(config_path, cfg):
    from .runner import discover_tarballs

    rcfg = cfg["run"]
    root = Path(rcfg["input_dir"]).resolve()
    if not root.is_dir():
        raise ValueError(f"input directory does not exist: {root}")
    manifest_path = rcfg.get("input_manifest")
    excluded = 0
    inventory_digest = None
    if manifest_path:
        path = (config_path.parent / manifest_path).resolve()
        inventory_text = path.read_text(encoding="utf-8")
        inventory_digest = hashlib.sha256(inventory_text.encode()).hexdigest()
        paths, expected, seen = [], {}, set()
        for line in inventory_text.splitlines():
            row = json.loads(line)
            target = Path(row["path"]).resolve()
            if not target.is_relative_to(root):
                raise ValueError(f"inventory path outside input directory: {target}")
            if str(target) in seen:
                raise ValueError(f"duplicate inventory path: {target}")
            seen.add(str(target))
            if row["status"] != "valid":
                excluded += 1
                continue
            paths.append(target)
            expected[str(target)] = {k: row[k] for k in ("path", "size_bytes", "mtime_ns")}
        paths.sort()
    else:
        paths = discover_tarballs(root)
        expected = {}
    limit = int(rcfg.get("max_tarballs", 0))
    if limit > 0:
        paths = paths[:limit]
    if manifest_path and not paths:
        raise ValueError("input manifest contains no fully validated archives")
    entries = []
    for path in paths:
        current = identity(path)
        if str(path) in expected and current != expected[str(path)]:
            raise ValueError(f"validated input changed; revalidate it: {path}")
        entries.append(current)
    return entries, excluded, inventory_digest


def _checked_clean(args):
    from .runner import _clean_one_tar

    entry, cfg = args
    path = Path(entry["path"])
    if identity(path) != entry:
        raise ValueError(f"input changed before cleaning: {path}")
    result = _clean_one_tar((str(path), cfg))
    if identity(path) != entry:
        raise ValueError(f"input changed during cleaning: {path}")
    result["records"].sort(key=lambda row: row["file_path"])
    result["sigs"].sort(key=lambda pair: pair[0])
    return entry, result


def _shard_path(directory, entry):
    key = hashlib.sha256(entry["path"].encode()).hexdigest()
    return directory / f"{key}.json"


def _read_shard(directory, entry, fingerprints):
    path = _shard_path(directory, entry)
    if not path.exists():
        return None
    shard = json.loads(path.read_text(encoding="utf-8"))
    if (shard["input"] != entry or shard["fingerprints"] != fingerprints
            or shard["result"]["_tar"] != entry["path"]):
        raise ValueError(f"checkpoint identity mismatch: {path}")
    return shard["result"]


def run_pipeline(config_path: Path, resume=False):
    from .runner import config_fingerprint

    config_path = Path(config_path).resolve()
    cfg = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    rcfg = cfg["run"]
    if rcfg.get("steps", {}).get("pii", False):
        raise ValueError("PII is excluded from this cleaning profile; set steps.pii=false")
    if int(rcfg.get("workers", 8)) < 1:
        raise ValueError("workers must be positive")
    output = (config_path.parent / rcfg["output_dir"] / rcfg["name"]).resolve()
    with run_lock(output):
        artifacts = [p for p in output.iterdir() if p.name != ".lock"]
        manifest_path = output / "manifest.json"
        if not resume and artifacts:
            raise ValueError(f"run already exists: {output}; use --resume or a new run name")
        if resume and not manifest_path.exists():
            raise ValueError("no checkpoint manifest; start a new run without --resume")
        fingerprints = {"config": config_fingerprint(cfg), "code": source_fingerprint()}
        meta_path = cfg.get("dedup", {}).get("repo_metadata_path")
        if meta_path:
            meta_path = (config_path.parent / meta_path).resolve()
            fingerprints["repo_metadata"] = hashlib.sha256(meta_path.read_bytes()).hexdigest()
        if resume:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            if manifest.get("schema_version") != SCHEMA_VERSION:
                raise ValueError("legacy append-only run: preserve it and use a new run name")
            if any(manifest["fingerprints"].get(k) != v for k, v in fingerprints.items()):
                raise ValueError("config, source code or dependencies changed; use a new run name")
        entries, excluded, inventory_digest = _inputs(config_path, cfg)
        fingerprints["inventory"] = inventory_digest
        if resume:
            if manifest["fingerprints"] != fingerprints:
                raise ValueError("input inventory changed; use a new run name")
            frozen = json.loads((output / "inputs.json").read_text(encoding="utf-8"))
            if frozen != entries:
                raise ValueError("input snapshot changed; use a new run name")
        else:
            atomic_json(output / "inputs.json", entries)
        return _execute(cfg, output, entries, excluded, fingerprints, meta_path)


def _execute(cfg, output, entries, excluded, fingerprints, meta_path=None):
    from .runner import _agg, _HAS_DATASKETCH

    started = time.monotonic()
    rcfg = cfg["run"]
    workers = int(rcfg.get("workers", 8))
    shard_dir = output / "intermediate" / "shards"
    shard_dir.mkdir(parents=True, exist_ok=True)
    processed, failed, stats = set(), {}, {}
    dropped_repos = {}
    dup_within = 0

    def account(result):
        nonlocal dup_within
        if result["failed"]:
            failed[result["_tar"]] = result["failed"]
        else:
            processed.add(result["_tar"])
            if result.get("drop_reason"):
                dropped_repos[result["_tar"]] = result["drop_reason"]
            dup_within += result.get("dup_within", 0)
            for stage in result["stats"]:
                _agg(stats, stage)

    def checkpoint(state):
        atomic_json(output / "manifest.json", {
            "schema_version": SCHEMA_VERSION, "state": state,
            "fingerprint": fingerprints["config"], "fingerprints": fingerprints,
            "processed": sorted(processed), "failed": dict(sorted(failed.items())),
            "dropped_repos": dict(sorted(dropped_repos.items())),
            "dup_within_total": dup_within, "stages": stats,
        })

    todo = []
    for entry in entries:
        result = _read_shard(shard_dir, entry, fingerprints)
        if result is None:
            todo.append(entry)
        else:
            account(result)
    checkpoint("cleaning")
    if todo:
        with mp.get_context("fork").Pool(workers) as pool:
            for number, (entry, result) in enumerate(pool.imap_unordered(
                    _checked_clean, ((entry, cfg) for entry in todo), chunksize=1), 1):
                atomic_json(_shard_path(shard_dir, entry), {
                    "input": entry, "fingerprints": fingerprints, "result": result})
                account(result)
                if number % 50 == 0:
                    checkpoint("cleaning")
                    print(f"[dataflow] committed {number}/{len(todo)} new archives", flush=True)
    checkpoint("deduplicating")

    dcfg = cfg.get("dedup", {})
    qcfg = cfg.get("quality", {})
    repo_meta = load_repo_metadata(meta_path) if meta_path else {}
    meta_cov = {"path": str(meta_path) if meta_path else None, "entries": len(repo_meta),
                "repos_matched": 0, "with_stars": 0, "with_forks": 0,
                "with_created_at": 0, "with_license": 0}

    # Rebuild derivative files in frozen path order, independent of worker timing.
    # index.ndjson is a content-free copy (sha1, size, language per file) used
    # for the dedup/budget decisions so the heavy repos.ndjson is streamed once.
    repos_path = output / "intermediate" / "repos.ndjson"
    sig_path = output / "intermediate" / "signatures.ndjson"
    index_path = output / "intermediate" / "index.ndjson"
    repo_keys, repo_metas = [], []
    best_occ = {}   # sha1 bytes -> row index of the representative repo for exact dups
    with atomic_text(repos_path) as repos, atomic_text(sig_path) as signatures, \
            atomic_text(index_path) as index:
        for entry in entries:
            result = _read_shard(shard_dir, entry, fingerprints)
            if result["failed"] or result.get("drop_reason"):
                continue
            row_idx = len(repo_keys)
            meta = repo_meta.get(result["repo_path"].lower())
            if meta:
                meta_cov["repos_matched"] += 1
                for field, label in (("stars", "with_stars"), ("forks", "with_forks"),
                                     ("created_at", "with_created_at"), ("license", "with_license")):
                    if meta.get(field) is not None:
                        meta_cov[label] += 1
            key = repo_priority_key(meta)
            repo_keys.append(key)
            repo_metas.append({k: v for k, v in (meta or {}).items() if v is not None})
            repos.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
            files = []
            for record in result["records"]:
                sha1 = record["sha1"]
                files.append([sha1, record.get("size_bytes", 0), record.get("language") or "unknown"])
                raw = bytes.fromhex(sha1)
                current = best_occ.get(raw)
                # Strictly better only: ties keep the earlier (sorted input path) repo.
                if current is None or key < repo_keys[current]:
                    best_occ[raw] = row_idx
            index.write(json.dumps(files) + "\n")
            for sha1, signature in result["sigs"]:
                signatures.write(json.dumps({"sha1": sha1, "sig": signature}) + "\n")

    def priority(sha1):
        return repo_keys[best_occ[bytes.fromhex(sha1)]]

    near_enabled = cfg["run"]["steps"].get("dedup", True) and _HAS_DATASKETCH
    near_info = {}
    if near_enabled:
        dropped_near, near_info = _near_duplicates(
            sig_path, cfg, repos_path=repos_path, workdir=output / "intermediate",
            priority=priority, workers=workers)
    else:
        dropped_near = set()

    # ---- decisions: exact dup -> near dup -> repo downsample -> byte budget ----
    KEEP, EXACT, NEAR, DOWNSAMPLE, BUDGET = range(5)
    budget_on = cfg["run"]["steps"].get("budget", True)
    budget_caps = {k: int(v) for k, v in (qcfg.get("byte_budget") or {}).items()} if budget_on else {}
    ds_threshold = int(qcfg.get("max_repo_bytes_before_downsample") or 0) if budget_on else 0
    ds_ratio = float(qcfg.get("downsample_keep_ratio", 1.0))
    seed = int(qcfg.get("sampling_seed", 0))
    dedup = {"stage": "dedup", "seen": 0, "kept": 0, "dropped": 0, "reasons": {}}
    budget = {"stage": "budget", "seen": 0, "kept": 0, "dropped": 0, "reasons": {}}
    codes, budget_cands, downsampled_repos = [], {lang: [] for lang in budget_caps}, 0
    with index_path.open(encoding="utf-8") as source:
        for row_idx, line in enumerate(source):
            files = json.loads(line)
            code = bytearray(len(files))
            local, repo_bytes = set(), 0
            for pos, (sha1, size, _language) in enumerate(files):
                dedup["seen"] += 1
                raw = bytes.fromhex(sha1)
                if best_occ[raw] != row_idx or raw in local:
                    code[pos] = EXACT
                    continue
                local.add(raw)
                if sha1 in dropped_near:
                    code[pos] = NEAR
                    continue
                repo_bytes += size
            if ds_threshold and ds_ratio < 1.0 and repo_bytes > ds_threshold:
                downsampled_repos += 1
                for pos, (sha1, _size, _language) in enumerate(files):
                    if code[pos] == KEEP and not keep_in_downsample(seed, sha1, ds_ratio):
                        code[pos] = DOWNSAMPLE
            for pos, (sha1, size, language) in enumerate(files):
                if code[pos] == KEEP and language in budget_caps:
                    budget_cands[language].append((unit_hash(seed, "budget", sha1), row_idx, pos, size))
            codes.append(code)
    budget_report = {}
    for language, cap in sorted(budget_caps.items()):
        cands = budget_cands[language]
        offered = sum(c[-1] for c in cands)
        dropped_idents, kept_bytes = apply_byte_budget(cands, cap)
        for row_idx, pos in dropped_idents:
            codes[row_idx][pos] = BUDGET
        budget_report[language] = {"cap_bytes": cap, "offered_bytes": offered,
                                   "kept_bytes": kept_bytes, "dropped_files": len(dropped_idents),
                                   "dropped_bytes": offered - kept_bytes}
    del budget_cands, best_occ

    reason_names = {EXACT: "exact_duplicate", NEAR: "near_duplicate",
                    DOWNSAMPLE: "repo_downsample", BUDGET: "byte_budget"}
    files_out, repos_out, empty_repos = 0, 0, 0
    years = {}
    lang_bytes, total_bytes, repos_over_1_5gb = {}, 0, 0
    with repos_path.open(encoding="utf-8") as source, atomic_text(output / "repos.jsonl") as target:
        for row_idx, line in enumerate(source):
            row = json.loads(line)
            code = codes[row_idx]
            files = []
            for pos, record in enumerate(row["records"]):
                decision = code[pos]
                if decision in (EXACT, NEAR):
                    stage = dedup
                else:
                    dedup["kept"] += 1
                    budget["seen"] += 1
                    stage = budget
                if decision == KEEP:
                    files.append(record)
                    budget["kept"] += 1
                else:
                    reason = reason_names[decision]
                    stage["dropped"] += 1
                    stage["reasons"][reason] = stage["reasons"].get(reason, 0) + 1
            if not files:
                empty_repos += 1
                continue
            repo_bytes = 0
            for record in files:
                # Preserve 60's existing reporting unit: retained source
                # size_bytes, rather than serialized JSONL or compressed bytes.
                size = record.get("size_bytes", 0)
                repo_bytes += size
                total_bytes += size
                language = record.get("language") or "unknown"
                lang_bytes[language] = lang_bytes.get(language, 0) + size
            if repo_bytes > LARGE_REPO_BYTES:
                repos_over_1_5gb += 1
            repo = RepoRecord(repo_path=row["repo_path"], num_files=len(files), files=files,
                              github_metadata=repo_metas[row_idx],
                              last_activity_year=row.get("activity"),
                              evidence_files=row.get("evidence", 0))
            target.write(json.dumps(repo.__dict__, ensure_ascii=False, sort_keys=True) + "\n")
            repos_out += 1
            files_out += len(files)
            key = str(repo.last_activity_year)
            years[key] = years.get(key, 0) + 1
    stats["dedup"] = dedup
    stats["budget"] = budget
    report = {
        "run_name": rcfg["name"], "workers": workers, "pii_enabled": False,
        "dedup_mode": "minhash_and_exact" if near_enabled else "exact_only",
        "near_dup_verification": near_info.get("method", "disabled"),
        "representative_policy": REPRESENTATIVE_POLICY,
        "repo_metadata": meta_cov,
        "input_validation": "inventory_valid_only" if rcfg.get("input_manifest") else "not_required_by_config",
        "input_manifest": rcfg.get("input_manifest"),
        "fingerprints": fingerprints, "tarballs_seen": len(entries),
        "tarballs_excluded_by_inventory": excluded,
        "tarballs_processed": len(processed) + len(failed), "tarballs_failed": len(failed),
        "tarballs_repo_dropped": len(dropped_repos),
        "repos_out": repos_out, "files_out": files_out,
        "content_bytes_out": total_bytes,
        "byte_share_top_langs": {
            language: round(size / max(total_bytes, 1), 4)
            for language, size in sorted(lang_bytes.items(), key=lambda pair: (-pair[1], pair[0]))[:12]},
        "repos_over_1_5gb": repos_over_1_5gb,
        "repos_empty_after_filter": empty_repos,
        "byte_budget": budget_report,
        "repo_downsample": {"threshold_bytes": ds_threshold, "keep_ratio": ds_ratio,
                            "seed": seed, "repos_downsampled": downsampled_repos},
        "near_dup": near_info,
        # Informational only: no year-based filtering happens anywhere.
        "activity_year_hist": dict(sorted(years.items())),
        "elapsed_sec": round(time.monotonic() - started, 1), "stages": stats,
    }
    atomic_json(output / "report.json", report)
    checkpoint("complete")
    return report


def _shingle_job(item):
    from .dedup import shingle_set
    position, content = item
    return position, shingle_set(content).tobytes()


def _build_shingle_store(repos_path, needed, store_path, workers):
    """Write the exact shingle sets of ``needed`` ({sha1: position}) to disk.

    Returns {position: (offset, length)} into a flat uint64 file. Work is fed
    to the pool in bounded batches so contents are never all held in memory.
    """
    offsets, done, offset = {}, set(), 0
    batch, batch_chars = [], 0
    pool = mp.get_context("fork").Pool(workers) if workers > 1 else None

    def flush(sink):
        nonlocal offset, batch, batch_chars
        results = pool.map(_shingle_job, batch, chunksize=8) if pool else map(_shingle_job, batch)
        for position, blob in results:
            sink.write(blob)
            length = len(blob) // 8
            offsets[position] = (offset, length)
            offset += length
        batch, batch_chars = [], 0

    try:
        with store_path.open("wb") as sink, repos_path.open(encoding="utf-8") as source:
            for line in source:
                if len(done) == len(needed):
                    break
                row = json.loads(line)
                for record in row["records"]:
                    sha1 = record["sha1"]
                    if sha1 in needed and sha1 not in done:
                        done.add(sha1)
                        batch.append((needed[sha1], record["content"]))
                        batch_chars += len(record["content"])
                if batch_chars > 256 * 1024 * 1024 or len(batch) >= 20_000:
                    flush(sink)
            if batch:
                flush(sink)
    finally:
        if pool:
            pool.close()
            pool.join()
    missing = set(needed) - done
    if missing:
        raise ValueError(f"near-dup verification: {len(missing)} candidate contents not found")
    return offsets


def _near_duplicates(path, cfg, repos_path=None, workdir=None, priority=None, workers=1):
    """MinHash-LSH candidates -> verification -> clusters -> representatives.

    verify_method (dedup.verify_method, default ``exact``):
      * ``exact``: every LSH candidate pair whose MinHash estimate is at least
        ``threshold - verify_prefilter_margin`` is checked with the TRUE
        Jaccard of the shingle sets (recomputed from intermediate content for
        candidate files only, spilled to a flat uint64 file); pairs already in
        the same cluster are skipped because they cannot change connectivity.
      * ``estimate``: legacy rule, MinHash estimate >= threshold.
      * ``verify_jaccard: false``: no verification (every LSH candidate).
    Representative per cluster: smallest ``(priority(sha1), input position)``.
    Returns (dropped sha1 set, info dict).
    """
    from array import array
    from .runner import MinHash, MinHashLSH, np, UnionFind
    from .dedup import true_jaccard

    dcfg = cfg["dedup"]
    perms = int(dcfg["minhash_permutations"])
    threshold = float(dcfg["lsh_jaccard_threshold"])
    if not dcfg.get("verify_jaccard", True):
        method = "none"
    else:
        method = dcfg.get("verify_method", "exact")
        if method == "exact" and repos_path is None:
            raise ValueError("exact near-dup verification needs intermediate contents")
    margin = float(dcfg.get("verify_prefilter_margin", 0.1))
    index = MinHashLSH(threshold=threshold, num_perm=perms)
    signatures, positions, ordered = [], {}, []
    pair_a, pair_b, pair_est = array("q"), array("q"), array("f")
    clusters = UnionFind()
    info = {"method": method, "threshold": threshold, "lsh_candidate_pairs": 0}
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            sha1 = row["sha1"]
            if sha1 in positions:
                continue
            values = np.frombuffer(base64.b64decode(row["sig"]), dtype=np.uint32)
            mh = MinHash(num_perm=perms, hashvalues=values, scheme="affine32")
            position = len(ordered)
            positions[sha1] = position
            signatures.append(mh)
            ordered.append(sha1)
            for peer in sorted(index.query(mh)):
                other = positions[peer]
                info["lsh_candidate_pairs"] += 1
                if method == "none":
                    clusters.union(other, position)
                    continue
                estimate = mh.jaccard(signatures[other])
                if method == "estimate":
                    if estimate >= threshold:
                        clusters.union(other, position)
                elif estimate >= threshold - margin:
                    pair_a.append(other)
                    pair_b.append(position)
                    pair_est.append(estimate)
            index.insert(sha1, mh)
            if len(ordered) % 100_000 == 0:
                print(f"[dataflow] dedup index {len(ordered)}", flush=True)
    del index, signatures

    if method == "exact":
        info.update(prefilter_margin=margin, pairs_after_prefilter=len(pair_a),
                    verified=0, accepted=0, rejected=0, skipped_already_clustered=0,
                    rejected_but_estimate_passed=0, accepted_but_estimate_failed=0,
                    rejected_samples=[], shingle_files=0, shingle_store_bytes=0)
        if len(pair_a):
            needed = {ordered[p]: p for p in sorted(set(pair_a) | set(pair_b))}
            store = Path(workdir) / "near_dup_shingles.u64"
            print(f"[dataflow] near-dup: exact Jaccard for {len(pair_a)} pairs / "
                  f"{len(needed)} files", flush=True)
            offsets = _build_shingle_store(repos_path, needed, store, workers)
            info["shingle_files"] = len(needed)
            info["shingle_store_bytes"] = store.stat().st_size
            data = np.memmap(store, dtype=np.uint64, mode="r") if store.stat().st_size else None

            def shingles(position):
                start, length = offsets[position]
                return data[start:start + length] if length else np.empty(0, dtype=np.uint64)

            for a, b, estimate in zip(pair_a, pair_b, pair_est):
                if clusters.find(a) == clusters.find(b):
                    info["skipped_already_clustered"] += 1
                    continue
                exact = true_jaccard(shingles(a), shingles(b))
                info["verified"] += 1
                if exact >= threshold:
                    clusters.union(a, b)
                    info["accepted"] += 1
                    if estimate < threshold:
                        info["accepted_but_estimate_failed"] += 1
                else:
                    info["rejected"] += 1
                    if estimate >= threshold:
                        info["rejected_but_estimate_passed"] += 1
                        if len(info["rejected_samples"]) < 50:
                            info["rejected_samples"].append({
                                "sha1_a": ordered[a], "sha1_b": ordered[b],
                                "estimate": round(float(estimate), 4),
                                "true_jaccard": round(float(exact), 4)})
            del data
            store.unlink()

    groups = {}
    for position in list(clusters.parent):
        groups.setdefault(clusters.find(position), []).append(position)
    dropped = set()
    for members in groups.values():
        if len(members) < 2:
            continue
        if priority is None:
            rep = min(members)
        else:
            rep = min(members, key=lambda p: (priority(ordered[p]), p))
        dropped.update(ordered[p] for p in members if p != rep)
    info["clusters"] = sum(1 for m in groups.values() if len(m) > 1)
    info["dropped"] = len(dropped)
    return dropped, info
