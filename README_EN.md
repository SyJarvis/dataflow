# dataflow

**A Stack v3-style source-code cleaning pipeline** — takes GitHub snapshot tarballs as input and produces repository-grouped training corpora.

[中文](README.md) | [Methodology source (stack-v3-train)](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train) | Python >=3.10

## Overview

dataflow cleans github_downloads-style repository snapshot directories into corpora ready for training. The methodology and all thresholds come from [The Stack v3 official dataset card](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train) (Dataset creation section), and the output schema has been verified against the official release. The current default profile is the 2026-11-11 rules (see the [rules comparison](docs/CLEANING-RULES-VS-STACKV3-20261111.md)).

Key features:

- **Official Stack v3 semantics**: filtering thresholds and dedup parameters replicate the official dataset card; the alphabetic-ratio filter counts Unicode letters (so non-Latin scripts such as Chinese are included), with no ad-hoc loosening of the other thresholds
- **Pure-Python core**: main steps have no heavy dependencies; dedup / parquet / scancode / pii are optional extras
- **Two-pass global dedup**: SHA-1 exact dedup + MinHash LSH (256 permutations × 5-gram, LSH threshold 0.7) cross-language near-duplicate removal; candidate pairs are verified with true 5-gram shingle Jaccard; the representative is chosen by stars → forks → permissive license → earliest creation time (falling back to path + sha1 when metadata is missing)
- **Post-dedup budgets**: per-language byte caps for XML / HTML / JSON / JavaScript (`quality.byte_budget`, 2e9 each by default); repos whose kept bytes exceed 1.5 GiB are downsampled at 50%
- **Resumable runs**: each input is committed as a shard before the combined manifest is updated; `--resume` skips finished work, and a changed config fingerprint refuses to resume
- **Official-shaped output**: repos.jsonl mirrors the stack-v3-train release format (one repo per line, files[] with inline content) and can be converted to Parquet for HF publishing

## News

- **2026-11-11** Cleaning-rules update: Unicode letter ratio; notebooks stripped before the quality filter; true shingle Jaccard verification; dedup representatives chosen by stars, forks, license, and creation time; per-language byte budgets (XML/HTML/JSON/JavaScript, 2e9 bytes each by default) and 50% downsampling of repos whose kept bytes exceed 1.5 GiB; the activity-year restriction is removed.
- **2026-10-27** First release: supports cleaning data with the Stack v3 data pipeline.

## QuickStart

### Requirements

Python >= 3.10. Optional extras installed on demand:

| extra | enables |
|---|---|
| `dedup` | MinHash LSH near-duplicate dedup (exact dedup only if not installed) |
| `parquet` | Parquet output (JSONL only if not installed) |
| `scancode` | ScanCode Toolkit license detection (regex fallback if not installed) |
| `pii` | StarPII model redaction (regex fallback if not installed; currently off, and enabling it in the execution path raises an error) |

### Install and try

```bash
cd ~/dataflow
python3 -m venv .venv && . .venv/bin/activate
pip install -e . -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -e ".[dedup]" -i https://pypi.tuna.tsinghua.edu.cn/simple   # optional: near-dup dedup

# Small pilot run (configs caps it at 10 tars via max_tarballs)
PYTHONPATH=src python3 -m dataflow run configs/default.yaml

# Resume (skips already-processed tars when config unchanged)
PYTHONPATH=src python3 -m dataflow run configs/default.yaml --resume

# View the report
PYTHONPATH=src python3 -m dataflow report runs/pilot10/report.json
```

### Enable go-enry language detection (recommended)

The builtin fallback is an extension+heuristic table covering ~60 languages; with go-enry, language and vendor detection match GitHub Linguist (the same engine used by official Stack v3):

```bash
# Install the CLI (requires Go >= 1.21)
go install github.com/go-enry/go-enry/v2/cmd/enry@latest
# The binary lands at $(go env GOPATH)/bin/enry
```

Then set `languages.go_enry_bin: /path/to/enry` (e.g. `~/go/bin/enry`) in your config. Without it the builtin fallback is used automatically.

Once `go_enry_bin` is set, if an executable `<go_enry_bin>-server` is present (for example `/usr/local/bin/enry` looks for `/usr/local/bin/enry-server`), each worker reuses a long-lived `enry-server` process. The result is equivalent to single-file `enry -json` analysis, without starting a new process per file. You can point to the server binary with the `DATAFLOW_ENRY_SERVER` environment variable, or set it to `off` to force the per-file CLI. If the server is missing or a call fails, the pipeline falls back to `enry -json`, and then to the builtin table, without interrupting the run.

Build the long-lived server locally:

```bash
cd tools/enry-server && go build -o enry-server .
# Place it next to enry as enry-server, or export DATAFLOW_ENRY_SERVER=/path/to/enry-server
```

The Docker image already ships both `/usr/local/bin/enry` and `/usr/local/bin/enry-server`.

### Output format

Each run produces `runs/<name>/` (or `output_dir/<name>/` from the config) with these files:

```
runs/<name>/
  repos.jsonl              # final corpus: one repo per line, files[] inline cleaned content
  report.json              # QC sheet: per-stage seen/kept/dropped and reason breakdown
  manifest.json            # progress ledger: config/code fingerprints + processed/failed tar list (--resume basis)
  intermediate/
    shards/                # one atomic shard per input; the primary resume record
    repos.ndjson           # pre-dedup repo results assembled from shards
    signatures.ndjson      # MinHash signatures
    index.ndjson           # light index used by dedup/budget
```

**repos.jsonl** — each line is one JSON object = one complete repository:

| field | type | description |
|---|---|---|
| `repo_path` | string | repository identity `owner/name` (derived from tar path) |
| `num_files` | int | number of files kept after cleaning |
| `files` | list | array of file objects, see below |
| `commit_id` | string | snapshot commit (injected by the collector; usually empty today) |
| `github_metadata` | object | non-empty fields from `dedup.repo_metadata_path` when the repo matches (e.g. stars / forks) |
| `last_activity_year` | int / null | activity year inferred from content evidence; informational only, not used as a filter |
| `dropped` / `drop_reason` | bool / string | whether the whole repo was dropped and why (e.g. `repo_exceeds_max_files`) |

`files[i]` fields: `file_path` (path relative to repo root), `content` (**cleaned full text**, usable directly as training text), `language` (language label, empty = unrecognized), `sha1` (SHA-1 of the **original content**, equivalent to the official `content_id`, the key for exact dedup and provenance), `size_bytes` (original size in bytes), `is_vendor` (third-party/dependency file), `license_type` (`permissive` / `no_license` / `non_permissive`, filled once step ③ is enabled), `drop_reason` (empty when kept).

Downstream reading example (streaming, no full load; sample by `language`, group by `repo_path` for repo-context training):

```python
import json

for line in open("runs/pilot10/repos.jsonl"):
    repo = json.loads(line)
    if repo["dropped"]:
        continue
    for f in repo["files"]:
        if f["language"] == "Python":   # sample by language
            text = f["content"]         # use directly as training text
```

**report.json** — QC sheet: `tarballs_seen / tarballs_processed / tarballs_failed / repos_out / files_out / elapsed_sec`, plus per-stage `seen / kept / dropped / reasons` under `stages`. If one reason dominates (e.g. `data_format_over_line_cap` drops 30%), that threshold is too aggressive for the current corpus → adjust the config and rerun under a **new run name**. Common drop reasons:

| stage | reason | meaning |
|---|---|---|
| unpack | `binary_null_byte` / `oversize_file` / `symlink_or_nonregular` | download-time drop rules |
| languages | `undecodable` | not UTF-8 and no encoding detected |
| quality | `low_alpha_ratio` | Unicode alphabetic ratio < 25% (alphanumerics for Assembly; StarCoder2 rule) |
| quality | `avg_line_length` / `max_line_length` | line length over limit |
| quality | `too_many_lines` / `data_format_over_line_cap` | too many lines / data formats like JSON·YAML capped at 512 lines |
| quality | `auto_generated` | auto-generated markers in first 5 lines |
| quality | `lfs_pointer` / `encoded_blob` / `blocklisted_extension` | LFS / large base64·hex blobs / non-code extensions |
| quality | `html_low_visible_text` / `html_low_visible_ratio` | insufficient HTML visible text |
| licenses | `non_permissive` | GPL-family (once step ③ is enabled) |
| dedup | `exact_duplicate` / `near_duplicate` | exact duplicate / true Jaccard ≥ 0.7 near-duplicate |
| budget | `repo_downsample` / `byte_budget` | large-repo downsampling / per-language byte cap exceeded |

**manifest.json** — three duties: ① `--resume` treats shards as the completed-work record, and known-bad tars are skipped without retry; ② if `fingerprints` (config and code hashes, plus repo-metadata when provided) do not match the current run, resume is refused, preventing mixed results from old/new configs in one run; ③ `failed` records the reason for every corrupted tar so the collector can repair and re-download.

## Pipeline

This is the actual execution order (in the config, quality is still numbered ④ and notebooks ⑤, but notebooks run first):

```
<user>/<repo>_HEAD.tar.gz
  ① unpack      stream-extract; drop >5MB/binary/symlink/1M-files cap
  ② languages   UTF-8 (cchardet fallback) + go-enry (enry-server preferred) language/vendor labels + SHA-1 content addressing
  ③ licenses    license file detection (ScanCode or regex) + directory-tree propagation + non_permissive removal   [off by default]
  ④ notebooks   .ipynb strip outputs/embedded images/volatile metadata (before the quality filter)
  ⑤ quality     StarCoder2 heuristics (line length/Unicode alpha ratio/512-line cap/auto-generated markers...)
  ⑥ pii         off by default; the current execution path raises if run.steps.pii is set to true
  ⑦ dedup       MinHash (256 perms × 5-gram) LSH + true Jaccard verification + connected components; cross-language global   [active with datasketch]
  ⑦b budget     after dedup: 50% downsample of repos whose kept bytes exceed 1.5 GiB; 2e9-byte caps for XML/HTML/JSON/JavaScript
  ⑧ group       group by repo → runs/<name>/repos.jsonl (or Parquet)
```

Each repository is also annotated with `last_activity_year` (from content evidence). The value is written to the output and to a report histogram only; it is not used to filter the training set.

### Implementation status

| step | status | notes |
|---|---|---|
| ①②④⑤ | ✅ complete | pure Python; notebooks are stripped before the quality filter |
| ③ | ✅ regex version | ScanCode hook reserved (`licenses.annotate_repo_licenses`), allowlist = Blue Oak + common permissive licenses; off by default |
| ⑦ | ✅ degraded mode | full MinHash LSH with `datasketch`, plus true Jaccard verification (`dedup.verify_method: exact`, or `estimate` for the older check); exact dedup only without datasketch |
| ② language detection | ✅ dual-mode + long-lived server | builtin extension+shebang table (~60 languages) by default; Linguist-grade with `go_enry_bin` set; reuses `enry-server` when present, with staged fallbacks |
| ⑦b budget | ✅ complete | `run.steps.budget` (on if omitted); keys `quality.byte_budget`, `max_repo_bytes_before_downsample`, `downsample_keep_ratio`, `sampling_seed` |
| ⑥ | off by default | the current execution path raises if `steps.pii` is enabled; it is not a simple on-switch |
| ⑧ Parquet | optional | install `pyarrow` |

Schema mapping to official stack-v3-train: `repo_path` / `num_files` / `files[].content` / `language` / `is_vendor` / `license_type` / `size_bytes` / `file_path` identical; `sha1` ↔ official `content_id` (name only). Missing (pending collector injection / ScanCode): `repo_id`, `commit_id`, `file_timestamp`, `detected_licenses`. `github_metadata` is filled with whatever fields `dedup.repo_metadata_path` can match. Extra extension: `last_activity_year` (content-evidence year annotation, working around tar mtimes being overwritten by packing time; informational only, not a filter).

## Usage

### Configs

| config | purpose |
|---|---|
| [default.yaml](configs/default.yaml) | default config, already on the 2026-11-11 rules, small pilot with `max_tarballs: 10` |
| [pilot-rules-20261111.yaml](configs/pilot-rules-20261111.yaml) | 2026-11-11 rules pilot |
| [full-rules-20261111.yaml](configs/full-rules-20261111.yaml) | 2026-11-11 rules full run |
| [pilot-repos-20261117.yaml](configs/pilot-repos-20261117.yaml) | 300-repo pilot for the repos/ batch |
| [pilot-enry.yaml](configs/pilot-enry.yaml) / [full-enry.yaml](configs/full-enry.yaml) | 2026-11-05 go-enry comparison configs |
| [full.yaml](configs/full.yaml) | earlier full-run config |
| [pilot-verified.yaml](configs/pilot-verified.yaml) | earlier real-input regression (9 valid of first 10, PII off) |
| [full-verified.yaml](configs/full-verified.yaml) | earlier acceptance full-run config (do not launch it as the current rules) |

License filtering (`run.steps.licenses`) and PII (`run.steps.pii`) are off in all of the configs above.

### CLI cheatsheet

```bash
# Run the pipeline (--resume to recover)
PYTHONPATH=src python3 -m dataflow run <config.yaml> [--resume]

# Read-only input validation (gzip/tar integrity inventory, no cleaning)
PYTHONPATH=src python3 -m dataflow validate-input <input_dir> --output-dir <dir> \
    [--workers 4] [--timeout-sec 1800] [--resume] [--headers-only]

# View a run report
PYTHONPATH=src python3 -m dataflow report runs/<name>/report.json
```

### Convert a repos/ volume

`scripts/bundle_to_tar.py` converts `.bundle` / `.git.tar` / `.dir.tar` artifacts in an swhkit `repos/` volume into the github_downloads layout that dataflow's unpack step expects: `<out>/<user>/<repo>_HEAD.tar.gz` containing `<repo>-HEAD/...`.

```bash
python scripts/bundle_to_tar.py \
  --manifest MANIFEST.tsv \
  --root /path/to/cleaning_data \
  --out /path/to/repos_tar \
  --workers 16 \
  --tmp /tmp \
  [--sample 300] [--seed 20261117] \
  [--ids-file ids.txt] \
  [--only-new /path/to/github_downloads] \
  [--verify-sha256] \
  [--timeout 3600]
```

| flag | meaning |
|---|---|
| `--manifest` / `--root` / `--out` | manifest, root for artifact paths in the manifest, and output directory (all required) |
| `--workers` | parallelism, default 16 |
| `--tmp` | temporary directory, defaulting to the system temp dir |
| `--sample` / `--seed` | sample size (0 means all) and RNG seed (default 20261117) |
| `--ids-file` | convert only these `repo_id` or `<user>/<repo>` values (one per line) |
| `--only-new DIR` | skip repos already present in this github_downloads-style directory |
| `--verify-sha256` | verify each artifact's sha256 against the manifest before converting |
| `--timeout` | per-git-operation timeout in seconds, default 3600 |

The output directory also receives `convert_log.tsv` (one row per repo: status, revision actually used, input/output sizes, elapsed time, error) and `repo_metadata.tsv` (in `dedup.repo_metadata_path` format, with stars taken from the MANIFEST).

Known limitation: repositories whose names end with `.` currently fail (for example `Snake_Game.`). This is not fixed yet.

### Run with Docker

The image is a multi-stage build: stage one compiles the go-enry CLI and enry-server, stage two installs dataflow (with dedup+parquet extras by default). The Dockerfile defaults to `GOPROXY=https://proxy.golang.org,direct` and sets `GOSUMDB=off`, so the image can still build when the checksum database is unreachable:

```bash
# Build (add --build-arg GOPROXY=https://goproxy.cn,direct on slow networks)
docker build -t dataflow .

# Heavy feature groups built on demand (scancode license detection / pii redaction, torch is multi-GB)
docker build --build-arg EXTRAS=dedup,parquet,scancode -t dataflow:scancode .

# Run: mount the input and output dirs; config paths use the in-container mount points
docker run --rm \
  -v /path/to/input:/input:ro \
  -v $PWD/runs:/runs \
  dataflow run /input/myconfig.yaml

# The image ships enry and enry-server; set in the config: languages.go_enry_bin: /usr/local/bin/enry
```

Note: the Dockerfile build has not been verified on the dev machine (no Docker installed); build it first on any machine with Docker. The `docker run` output dir `/runs` must match the config's `output_dir`.

## Roadmap

- [x] Integrate go-enry CLI (language detection on par with Linguist; enable via `languages.go_enry_bin`, builtin fallback when unset)
- [x] Long-lived enry-server (used automatically when `go_enry_bin` is set and a matching `enry-server` exists; already baked into the Docker image)
- [ ] Integrate ScanCode Toolkit (full SPDX detection)
- [x] Dedup representative selection by stars → forks → permissive license → earliest creation time (path + sha1 as a final tie-break); metadata is injected via `dedup.repo_metadata_path`, and coverage depends on the source
- [ ] Incremental mode: check new tars against the existing content store via SHA-1 + MinHash before ingesting
- [x] Global byte budgets for XML/HTML/JSON/JavaScript (`quality.byte_budget`, 2e9 bytes each by default; applied after dedup, drops recorded as `byte_budget`)
- [x] Probabilistic downsampling of repos whose kept bytes exceed 1.5 GiB (`quality.max_repo_bytes_before_downsample` / `downsample_keep_ratio`, defaults 1,610,612,736 and 0.5; reason `repo_downsample`)

## Documentation

- [Architecture and implementation](docs/architecture.md) (Chinese)
- [Cleaning-rules comparison: Stack v3 / previous / 2026-11-11](docs/CLEANING-RULES-VS-STACKV3-20261111.md)
- [github_downloads data notes](docs/DATA-github_downloads-20261117.md)
- [Full-run funnel: full-rules-20261111](docs/FUNNEL-full-rules-20261111.md)
- [repos / swhkit volume scan](docs/REPOS-SCAN-20261110.md)
- [repos/ conversion and 300-repo pilot](docs/PILOT-repos-20261117.md)
- [Output schema](docs/outputs.md)
- [Stack v3 alignment notes](docs/stack-v3-alignment.md)
- [Dataset build guide](docs/BUILD_DATASET_GUIDE.md)

## License

This project is licensed under the [MIT License](LICENSE).
