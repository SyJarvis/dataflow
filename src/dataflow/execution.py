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
        return _execute(cfg, output, entries, excluded, fingerprints)


def _execute(cfg, output, entries, excluded, fingerprints):
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

    # Rebuild derivative files in frozen path order, independent of worker timing.
    repos_path = output / "intermediate" / "repos.ndjson"
    sig_path = output / "intermediate" / "signatures.ndjson"
    with atomic_text(repos_path) as repos, atomic_text(sig_path) as signatures:
        for entry in entries:
            result = _read_shard(shard_dir, entry, fingerprints)
            if result["failed"] or result.get("drop_reason"):
                continue
            repos.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
            for sha1, signature in result["sigs"]:
                signatures.write(json.dumps({"sha1": sha1, "sig": signature}) + "\n")

    near_enabled = cfg["run"]["steps"].get("dedup", True) and _HAS_DATASKETCH
    dropped_near = _near_duplicates(sig_path, cfg) if near_enabled else set()
    seen, files_out, repos_out, empty_repos = set(), 0, 0, 0
    dedup = {"stage": "dedup", "seen": 0, "kept": 0, "dropped": 0, "reasons": {}}
    years = {}
    lang_bytes, total_bytes, repos_over_1_5gb = {}, 0, 0
    with repos_path.open(encoding="utf-8") as source, atomic_text(output / "repos.jsonl") as target:
        for line in source:
            row = json.loads(line)
            files = []
            for record in row["records"]:
                dedup["seen"] += 1
                sha1 = record["sha1"]
                if sha1 in seen:
                    reason = "exact_duplicate"
                else:
                    seen.add(sha1)
                    reason = "near_duplicate" if sha1 in dropped_near else None
                if reason:
                    dedup["dropped"] += 1
                    dedup["reasons"][reason] = dedup["reasons"].get(reason, 0) + 1
                else:
                    files.append(record)
                    dedup["kept"] += 1
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
                              last_activity_year=row.get("activity"),
                              evidence_files=row.get("evidence", 0))
            target.write(json.dumps(repo.__dict__, ensure_ascii=False, sort_keys=True) + "\n")
            repos_out += 1
            files_out += len(files)
            key = str(repo.last_activity_year)
            years[key] = years.get(key, 0) + 1
    stats["dedup"] = dedup
    report = {
        "run_name": rcfg["name"], "workers": workers, "pii_enabled": False,
        "dedup_mode": "minhash_and_exact" if near_enabled else "exact_only",
        "representative_policy": "first_in_sorted_input_path_then_sha1",
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
        "repos_active_2026": years.get("2026", 0), "activity_year_hist": dict(sorted(years.items())),
        "elapsed_sec": round(time.monotonic() - started, 1), "stages": stats,
    }
    atomic_json(output / "report.json", report)
    checkpoint("complete")
    return report


def _near_duplicates(path, cfg):
    from .runner import MinHash, MinHashLSH, np, UnionFind

    perms = int(cfg["dedup"]["minhash_permutations"])
    threshold = float(cfg["dedup"]["lsh_jaccard_threshold"])
    index = MinHashLSH(threshold=threshold, num_perm=perms)
    signatures, positions, ordered = {}, {}, []
    clusters = UnionFind()
    with path.open(encoding="utf-8") as stream:
        for line in stream:
            row = json.loads(line)
            sha1 = row["sha1"]
            if sha1 in positions:
                continue
            values = np.frombuffer(base64.b64decode(row["sig"]), dtype=np.uint32)
            mh = MinHash(num_perm=perms, hashvalues=values, scheme="affine32")
            positions[sha1] = len(ordered)
            signatures[sha1] = mh
            ordered.append(sha1)
            for peer in sorted(index.query(mh)):
                if cfg["dedup"].get("verify_jaccard", True) and mh.jaccard(signatures[peer]) < threshold:
                    continue
                clusters.union(positions[sha1], positions[peer])
            index.insert(sha1, mh)
            if len(ordered) % 100_000 == 0:
                print(f"[dataflow] dedup index {len(ordered)}", flush=True)
    best, dropped = {}, set()
    for sha1 in ordered:
        root = clusters.find(positions[sha1])
        if root in best:
            dropped.add(sha1)
        else:
            best[root] = sha1
    return dropped
