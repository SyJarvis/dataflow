# dataflow

**A Stack v3-style source-code cleaning pipeline** — takes GitHub snapshot tarballs as input and produces repository-grouped training corpora.

[中文](README.md) | [Methodology source (stack-v3-train)](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train) | Python >=3.10

## Overview

dataflow cleans github_downloads-style repository snapshot directories into corpora ready for training. The methodology and all thresholds come from [The Stack v3 official dataset card](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train) (Dataset creation section), and the output schema has been verified against the official release.

Key features:

- **Official Stack v3 semantics**: filtering thresholds and dedup parameters replicate the official dataset card, no ad-hoc tuning
- **Pure-Python core**: main steps have no heavy dependencies; dedup / parquet / scancode / pii are optional extras
- **Two-pass global dedup**: SHA-1 exact dedup + MinHash LSH (256 permutations × 5-gram, Jaccard ≥ 0.7) cross-language near-duplicate removal
- **Resumable runs**: manifest.json records the config fingerprint and processed-tar list; `--resume` skips finished work
- **Official-shaped output**: repos.jsonl mirrors the stack-v3-train release format (one repo per line, files[] with inline content) and can be converted to Parquet for HF publishing

## News

- **2026-10-27** First release: supports cleaning data with the Stack v3 data pipeline.

## QuickStart

### Requirements

Python >= 3.10. Optional extras installed on demand:

| extra | enables |
|---|---|
| `dedup` | MinHash LSH near-duplicate dedup (exact dedup only if not installed) |
| `parquet` | Parquet output (JSONL only if not installed) |
| `scancode` | ScanCode Toolkit license detection (regex fallback if not installed) |
| `pii` | StarPII model redaction (regex fallback if not installed, off by default) |

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

### Output format

Each run produces `runs/<name>/` with three files, each with its own role:

```
runs/<name>/
  repos.jsonl     # final corpus: one repo per line, files[] inline cleaned content
  report.json     # QC sheet: per-stage seen/kept/dropped and reason breakdown
  manifest.json   # progress ledger: config fingerprint + processed/failed tar list (--resume basis)
```

**repos.jsonl** — each line is one JSON object = one complete repository:

| field | type | description |
|---|---|---|
| `repo_path` | string | repository identity `owner/name` (derived from tar path) |
| `num_files` | int | number of files kept after cleaning |
| `files` | list | array of file objects, see below |
| `commit_id` | string | snapshot commit (injected by the collector, currently empty) |
| `github_metadata` | object | stars / forks etc. (injected by the collector, currently empty) |
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
| quality | `low_alpha_ratio` | alphabetic ratio < 25% (StarCoder2 rule) |
| quality | `avg_line_length` / `max_line_length` | line length over limit |
| quality | `too_many_lines` / `data_format_over_line_cap` | too many lines / data formats like JSON·YAML capped at 512 lines |
| quality | `auto_generated` | auto-generated markers in first 5 lines |
| quality | `lfs_pointer` / `encoded_blob` / `blocklisted_extension` | LFS / large base64·hex blobs / non-code extensions |
| quality | `html_low_visible_text` / `html_low_visible_ratio` | insufficient HTML visible text |
| licenses | `non_permissive` | GPL-family (once step ③ is enabled) |
| dedup | `exact_duplicate` / `near_duplicate` | exact duplicate / MinHash ≥ 0.7 near-duplicate |

**manifest.json** — three duties: ① `--resume` only processes tars outside `processed`, known-bad tars are skipped without retry; ② if the `fingerprint` (config content hash) does not match the current config, resume is refused, preventing mixed results from old/new configs in one run; ③ `failed` records the reason for every corrupted tar so the collector can repair and re-download.

## Pipeline

```
<user>/<repo>_HEAD.tar.gz
  ① unpack      stream-extract; drop >5MB/binary/symlink/1M-files cap
  ② languages   UTF-8 (cchardet fallback) + go-enry language/vendor labels + SHA-1 content addressing
  ③ licenses    license file detection (ScanCode or regex) + directory-tree propagation + non_permissive removal   [off by default]
  ④ quality     StarCoder2 heuristic 12 rules (line length/alpha ratio/512-line cap/auto-generated markers...)
  ⑤ notebooks   .ipynb strip outputs/embedded images/volatile metadata
  ⑥ pii         StarPII redaction (regex fallback without model)                                              [off by default]
  ⑦ dedup       MinHash (256 perms × 5-gram) LSH Jaccard≥0.7 + connected components + verification; cross-language global   [active with datasketch]
  ⑧ group       group by repo → runs/<name>/repos.jsonl (or Parquet)
```

### Implementation status

| step | status | notes |
|---|---|---|
| ①②④⑤ | ✅ complete | pure Python, no heavy dependencies |
| ③ | ✅ regex version | ScanCode hook reserved (`licenses.annotate_repo_licenses`), allowlist = Blue Oak + common permissive licenses |
| ⑦ | ✅ degraded mode | full MinHash LSH with `datasketch`; exact dedup only without it |
| ② language detection | ⚠️ fallback | built-in extension+shebang table (~60 languages); replaced by go-enry CLI later (`languages.go_enry_bin`) |
| ⑥ | ⚠️ regex fallback | requires transformers+StarPII for model redaction |
| ⑧ Parquet | optional | install `pyarrow` |

Schema mapping to official stack-v3-train: `repo_path` / `num_files` / `files[].content` / `language` / `is_vendor` / `license_type` / `size_bytes` / `file_path` identical; `sha1` ↔ official `content_id` (name only). Missing (pending collector injection / ScanCode): `repo_id`, `commit_id`, `github_metadata` (10 fields), `file_timestamp`, `detected_licenses`. Extra extension: `last_activity_year` (content-evidence year annotation, working around tar mtimes being overwritten by packing time).

## Usage

### Configs

| config | purpose |
|---|---|
| [default.yaml](configs/default.yaml) | default config, small pilot with `max_tarballs: 10` |
| [full.yaml](configs/full.yaml) | full run |
| [pilot-verified.yaml](configs/pilot-verified.yaml) | 60-merged real-input regression (9 valid of first 10, PII off) |
| [full-verified.yaml](configs/full-verified.yaml) | acceptance full run (alignment not finished, do not launch directly) |

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

## Roadmap

- [ ] Integrate go-enry CLI (language detection accuracy on par with Linguist)
- [ ] Integrate ScanCode Toolkit (full SPDX detection)
- [ ] github_metadata (stars/forks) injection → full representative selection
- [ ] Incremental mode: check new tars against the existing content store via SHA-1 + MinHash before ingesting
- [ ] Global byte budgets for XML/HTML/JSON/JS: report already outputs `byte_share_top_langs`; decide whether to implement after the full run
- [ ] Probabilistic downsampling of repos >1.5GB: report already outputs `repos_over_1_5gb`; decide whether to implement after the full run

## License

This project is licensed under the [MIT License](LICENSE).
