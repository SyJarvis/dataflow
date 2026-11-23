# dataflow

**Stack v3 风格的源代码清洗管线** —— 输入 GitHub 快照 tarball，输出仓库分组的训练语料。

[English](README_EN.md) | [方法论来源 (stack-v3-train)](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train) | Python >=3.10

## 概述

dataflow 将 github_downloads 风格的仓库快照目录清洗为可直接用于训练的语料。方法论与全部阈值来自 [The Stack v3 官方 dataset card](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train)（Dataset creation 节），并与官方产物做过 schema 对齐验证。当前默认口径是 2026-11-11 规则（见 [规则对照](docs/CLEANING-RULES-VS-STACKV3-20261111.md)）。

核心特性：

- **Stack v3 官方口径**：过滤阈值、去重参数均按官方 dataset card 复刻；字母占比按 Unicode 计算（中文等非拉丁字母也计入），其余阈值不擅自放宽
- **纯 Python 主干**：核心步骤无重依赖；dedup / parquet / scancode / pii 为可选 extras
- **两遍全局去重**：SHA-1 精确去重 + MinHash LSH（256 置换 × 5-gram，LSH 阈值 0.7）跨语言近重去重；候选对按真实 5-gram shingle Jaccard 复核；代表按 stars → forks → 宽松许可证 → 最早创建时间选择（缺元数据时回退路径 + sha1）
- **去重后配额**：XML / HTML / JSON / JavaScript 各有字节预算（`quality.byte_budget`，默认 2e9）；保留字节超过 1.5 GiB 的仓库按 50% 降采样
- **断点恢复**：每个输入先落盘为 shard，再汇总 manifest；`--resume` 跳过已完成部分，config 指纹变化则拒绝续跑
- **官方同构产物**：repos.jsonl 与 stack-v3-train 发布形态同构（每行一仓库、files[] 内联全文），可转 Parquet 直接对接 HF 发布

## 新闻

- **2026-11-11** 清洗规则更新：字母占比改按 Unicode 计算；Notebook 先剥离再过质量过滤；近重用真实 shingle Jaccard 复核；去重代表按 stars、forks、许可证、创建时间选择；按语言字节预算（XML/HTML/JSON/JavaScript，默认各 2e9 字节）和大仓降采样（保留字节 >1.5 GiB 时按 50% 保留）；去掉年份限制。
- **2026-10-27** 第一版本发布：支持 Stack v3 数据管线清洗数据。

## 快速开始

### 环境要求

Python >= 3.10。可选依赖按需安装：

| extra | 启用功能 |
|---|---|
| `dedup` | MinHash LSH 近重去重（不装则仅精确去重） |
| `parquet` | Parquet 输出（不装则仅 JSONL） |
| `scancode` | ScanCode Toolkit 许可证识别（不装则用正则版） |
| `pii` | StarPII 模型脱敏（不装则正则兜底；当前执行路径默认关闭，开启会报错拒绝） |

### 安装与试跑

```bash
cd ~/dataflow
python3 -m venv .venv && . .venv/bin/activate
pip install -e . -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -e ".[dedup]" -i https://pypi.tuna.tsinghua.edu.cn/simple   # 可选: 近重去重

# 小规模试跑（configs 里 max_tarballs 限 10 个 tar）
PYTHONPATH=src python3 -m dataflow run configs/default.yaml

# 断点恢复（config 未变时跳过已处理 tar）
PYTHONPATH=src python3 -m dataflow run configs/default.yaml --resume

# 查看报告
PYTHONPATH=src python3 -m dataflow report runs/pilot10/report.json
```

### 启用 go-enry 语言检测（推荐）

内置 fallback 为约 60 语言的扩展名+启发式表；接入 go-enry 后语言与 vendor 检测精度对齐 GitHub Linguist（官方 Stack v3 同款引擎）：

```bash
# 安装 CLI（需 Go >= 1.21）
go install github.com/go-enry/go-enry/v2/cmd/enry@latest
# 二进制位于 $(go env GOPATH)/bin/enry
```

然后在 config 中设置 `languages.go_enry_bin: /path/to/enry`（如 `~/go/bin/enry`）。不配置则自动用内置 fallback。

配置了 `go_enry_bin` 之后，若存在可执行的 `<go_enry_bin>-server`（例如 `/usr/local/bin/enry` 对应 `/usr/local/bin/enry-server`），每个 worker 会复用一个长驻 `enry-server` 进程，结果与 `enry -json` 单文件分析等价，避免逐文件起进程。也可用环境变量 `DATAFLOW_ENRY_SERVER` 指定服务器二进制；设为 `off` 则强制走逐文件 CLI。找不到服务器或调用失败时，自动降级到 `enry -json`，再失败则降级到内置表，运行不会中断。

本地构建长驻服务：

```bash
cd tools/enry-server && go build -o enry-server .
# 放到与 enry 相同的目录并命名为 enry-server，或 export DATAFLOW_ENRY_SERVER=/path/to/enry-server
```

Docker 镜像已同时内置 `/usr/local/bin/enry` 与 `/usr/local/bin/enry-server`。

### 产物格式

每次运行产出 `runs/<name>/`（或配置中的 `output_dir/<name>/`），主要文件如下：

```
runs/<name>/
  repos.jsonl              # 最终语料：每行一个仓库，files[] 内联清洗后全文
  report.json              # 质检单：各阶段 seen/kept/dropped 与原因分布
  manifest.json            # 进度账本：config/代码指纹 + 已处理/失败 tar 清单（--resume 依据）
  intermediate/
    shards/                # 每个输入一个原子 shard，是断点恢复的主记录
    repos.ndjson           # 由 shard 汇总的去重前仓库结果
    signatures.ndjson      # MinHash 签名
    index.ndjson           # 去重/预算用的轻量索引
```

**repos.jsonl** —— 每行一个 JSON 对象 = 一个完整仓库：

| 字段 | 类型 | 说明 |
|---|---|---|
| `repo_path` | string | 仓库标识 `owner/name`（由 tar 路径推出） |
| `num_files` | int | 清洗后留存的文件数 |
| `files` | list | 文件对象数组，见下 |
| `commit_id` | string | 快照 commit（采集侧注入，当前通常为空） |
| `github_metadata` | object | 若配置了 `dedup.repo_metadata_path` 且能匹配到该仓库，则写入其中非空字段（如 stars / forks） |
| `last_activity_year` | int / null | 根据文件内容证据标注的活动年份，仅作参考，不参与过滤 |
| `dropped` / `drop_reason` | bool / string | 整仓是否丢弃及原因（如 `repo_exceeds_max_files`） |

`files[i]` 字段：`file_path`（仓库内相对路径）、`content`（**清洗后全文**，直接作训练文本）、`language`（语言标注，空串 = 未识别）、`sha1`（**原始内容**的 SHA-1，与官方 `content_id` 同义，精确去重与溯源 key）、`size_bytes`（原始字节大小）、`is_vendor`（第三方/依赖文件）、`license_type`（`permissive` / `no_license` / `non_permissive`，③ 步启用后填充）、`drop_reason`（保留时为空）。

下游读取示例（流式，无需全量加载；按 `language` 做配比、按 `repo_path` 分组做 repo-context 训练）：

```python
import json

for line in open("runs/pilot10/repos.jsonl"):
    repo = json.loads(line)
    if repo["dropped"]:
        continue
    for f in repo["files"]:
        if f["language"] == "Python":   # 按语言配比采样
            text = f["content"]         # 直接作为训练文本
```

**report.json** —— 质检单：`tarballs_seen / tarballs_processed / tarballs_failed / repos_out / files_out / elapsed_sec`，以及 `stages` 内每阶段的 `seen / kept / dropped / reasons` 分布。某条 reason 占比异常高（如 `data_format_over_line_cap` 丢 30%）说明该阈值对当前语料偏激进 → 调 config 后用**新 run name** 重跑。常见丢弃原因速查：

| 阶段 | reason | 含义 |
|---|---|---|
| unpack | `binary_null_byte` / `oversize_file` / `symlink_or_nonregular` | 下载期丢弃规则 |
| languages | `undecodable` | 非 UTF-8 且检测不出编码 |
| quality | `low_alpha_ratio` | Unicode 字母占比 < 25%（汇编按字母数字；StarCoder2 规则） |
| quality | `avg_line_length` / `max_line_length` | 行长超限 |
| quality | `too_many_lines` / `data_format_over_line_cap` | 行数超限 / JSON·YAML 等数据格式超 512 行 |
| quality | `auto_generated` | 前 5 行含自动生成标记 |
| quality | `lfs_pointer` / `encoded_blob` / `blocklisted_extension` | LFS / 大块 base64·hex / 非代码扩展名 |
| quality | `html_low_visible_text` / `html_low_visible_ratio` | HTML 可见文本不足 |
| licenses | `non_permissive` | GPL 类（③ 步启用后） |
| dedup | `exact_duplicate` / `near_duplicate` | 精确重复 / 真实 Jaccard ≥ 0.7 近重 |
| budget | `repo_downsample` / `byte_budget` | 大仓降采样 / 语言字节预算超出上限 |

**manifest.json** —— 三项职责：① `--resume` 以 shard 为已完成记录，已知的坏 tar 直接跳过不再重试；② `fingerprints`（config 与代码哈希，若提供了仓库元数据则一并计入）与当前不一致时拒绝 resume，防止新旧配置结果混进同一 run；③ `failed` 记录每个损坏 tar 的原因，供采集侧修复重下。

## 数据管道

实际执行顺序如下（配置里 quality 仍编号为 ④、notebooks 为 ⑤，但 notebooks 会先跑）：

```
<user>/<repo>_HEAD.tar.gz
  ① unpack      流式解包；>5MB/二进制/symlink/百万文件上限丢弃
  ② languages   UTF-8(回退cchardet) + go-enry（优先长驻 enry-server）语言/vendor 标注 + SHA-1 内容寻址
  ③ licenses    license 文件识别(ScanCode或正则) + 目录树传播 + non_permissive 剔除   [默认关]
  ④ notebooks   .ipynb 剥输出/内嵌图/易变 metadata（在质量过滤之前）
  ⑤ quality     StarCoder2 启发式（行长/Unicode 字母占比/512行cap/自生成标记…）
  ⑥ pii         默认关；当前执行路径若将 run.steps.pii 设为 true 会报错退出
  ⑦ dedup       MinHash(256置换×5gram) LSH + 真实 Jaccard 复核 + 连通分量；跨语言全局   [装datasketch生效]
  ⑦b budget     去重后：保留字节 >1.5 GiB 的仓库按 50% 降采样；XML/HTML/JSON/JavaScript 各 2e9 字节封顶
  ⑧ group       按仓库分组 → runs/<name>/repos.jsonl (或 Parquet)
```

每个仓库还会标注 `last_activity_year`（内容证据年份），只写入产物和报告直方图，不用来筛选训练集。

### 实现状态

| 步骤 | 状态 | 说明 |
|---|---|---|
| ①②④⑤ | ✅ 完整 | 纯 Python；Notebook 先剥离再过质量规则 |
| ③ | ✅ 正则版 | ScanCode 接入点已留（`licenses.annotate_repo_licenses`），白名单=BlueOak+常见宽松license；默认关闭 |
| ⑦ | ✅ 降级可用 | 装 `datasketch` 后为完整 MinHash LSH + 真实 Jaccard 复核（`dedup.verify_method: exact`，可改 `estimate`）；不装则仅精确去重 |
| ② 语言检测 | ✅ 双模式 + 长驻服务 | 默认内置扩展名+shebang 表（~60语言）；配置 `go_enry_bin` 后为 Linguist 级检测；存在 `enry-server` 时自动复用进程，失败逐级降级 |
| ⑦b 预算 | ✅ 完整 | `run.steps.budget`（缺省为开）；配置项 `quality.byte_budget`、`max_repo_bytes_before_downsample`、`downsample_keep_ratio`、`sampling_seed` |
| ⑥ | 默认关 | 当前执行路径开启 `steps.pii` 会直接报错，不是打开就能跑 |
| ⑧ Parquet | 可选 | 装 `pyarrow` |

与官方 stack-v3-train 的 schema 对应：`repo_path` / `num_files` / `files[].content` / `language` / `is_vendor` / `license_type` / `size_bytes` / `file_path` 一致；`sha1` ↔ 官方 `content_id`（仅字段名不同）。暂缺（等采集侧注入 / ScanCode 后补）：`repo_id`、`commit_id`、`file_timestamp`、`detected_licenses`。`github_metadata` 在提供了 `dedup.repo_metadata_path` 且匹配成功时写入已有字段。独有扩展：`last_activity_year`（内容证据年份标注，绕开 tar mtime 被打包时间覆盖的问题；仅参考，不作过滤）。

## 使用

### 配置

| 配置 | 用途 |
|---|---|
| [default.yaml](configs/default.yaml) | 默认配置，已含 2026-11-11 规则，`max_tarballs: 10` 小规模试跑 |
| [pilot-rules-20261111.yaml](configs/pilot-rules-20261111.yaml) | 2026-11-11 规则试跑 |
| [full-rules-20261111.yaml](configs/full-rules-20261111.yaml) | 2026-11-11 规则全量 |
| [pilot-repos-20261117.yaml](configs/pilot-repos-20261117.yaml) | repos/ 批次 300 仓库试跑 |
| [pilot-enry.yaml](configs/pilot-enry.yaml) / [full-enry.yaml](configs/full-enry.yaml) | 2026-11-05 启用 go-enry 的对照配置 |
| [full.yaml](configs/full.yaml) | 较早的全量配置 |
| [pilot-verified.yaml](configs/pilot-verified.yaml) | 较早的真实输入回归（前 10 条中的 9 个 valid，PII 关闭） |
| [full-verified.yaml](configs/full-verified.yaml) | 较早的验收全量配置（勿直接当作当前规则启动） |

许可证过滤（`run.steps.licenses`）与 PII（`run.steps.pii`）在上述配置中均为关闭。

### CLI 命令速查

```bash
# 运行管线（--resume 断点恢复）
PYTHONPATH=src python3 -m dataflow run <config.yaml> [--resume]

# 只读校验输入目录（gzip/tar 完整性清点，不清洗）
PYTHONPATH=src python3 -m dataflow validate-input <input_dir> --output-dir <dir> \
    [--workers 4] [--timeout-sec 1800] [--resume] [--headers-only]

# 查看运行报告
PYTHONPATH=src python3 -m dataflow report runs/<name>/report.json
```

### 转换 repos/ 卷

`scripts/bundle_to_tar.py` 把 swhkit `repos/` 卷里的 `.bundle` / `.git.tar` / `.dir.tar` 转成 dataflow 解包所要求的 github_downloads 布局：`<out>/<user>/<repo>_HEAD.tar.gz`，包内为 `<repo>-HEAD/...`。

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

| 参数 | 说明 |
|---|---|
| `--manifest` / `--root` / `--out` | 清单、清单内 artifact 的根目录、输出目录（均必填） |
| `--workers` | 并发数，默认 16 |
| `--tmp` | 临时目录，默认系统临时目录 |
| `--sample` / `--seed` | 抽样条数（0 表示全部）与随机种子（默认 20261117） |
| `--ids-file` | 只转换这些 `repo_id` 或 `<user>/<repo>`（每行一个） |
| `--only-new DIR` | 跳过该 github_downloads 风格目录里已有的仓库 |
| `--verify-sha256` | 转换前核对清单中的 sha256 |
| `--timeout` | 单个 git 操作超时秒数，默认 3600 |

输出目录里还会写入 `convert_log.tsv`（每个仓库一行：状态、实际使用的提交、输入/输出大小、耗时、错误）和 `repo_metadata.tsv`（供 `dedup.repo_metadata_path` 使用，stars 来自 MANIFEST）。

已知限制：仓库名以 `.` 结尾时当前会失败（例如 `Snake_Game.`），尚未修复。

### Docker 运行

镜像为多阶段构建：第一阶段编译 go-enry CLI 和 enry-server，第二阶段安装 dataflow（默认含 dedup+parquet extras）。Dockerfile 默认 `GOPROXY=https://proxy.golang.org,direct`，并设 `GOSUMDB=off`，校验库不可达时也能构建：

```bash
# 构建（国内网络可加 --build-arg GOPROXY=https://goproxy.cn,direct）
docker build -t dataflow .

# 重型功能组按需构建（scancode 许可证识别 / pii 脱敏，torch 数 GB）
docker build --build-arg EXTRAS=dedup,parquet,scancode -t dataflow:scancode .

# 运行：挂载输入目录与产物目录，config 路径写容器内挂载点
docker run --rm \
  -v /path/to/input:/input:ro \
  -v $PWD/runs:/runs \
  dataflow run /input/myconfig.yaml

# 容器内已内置 enry 与 enry-server；config 里写: languages.go_enry_bin: /usr/local/bin/enry
```

注意：本仓库的 Dockerfile 未在开发机上验证过构建（无 Docker），首次在有 Docker 的机器上构建即可；`docker run` 的输出目录 `/runs` 需与 config 的 `output_dir` 一致。

## 路线图

- [x] 接入 go-enry CLI（语言检测精度对齐 Linguist；配置 `languages.go_enry_bin` 即启用，未配置用内置 fallback）
- [x] 长驻 enry-server（配置了 `go_enry_bin` 且存在对应 `enry-server` 时自动使用；Docker 镜像已内置）
- [ ] 接入 ScanCode Toolkit（SPDX 全量识别）
- [x] 去重代表选择按 stars → forks → 宽松许可证 → 最早创建时间（缺项回退路径 + sha1）；元数据经 `dedup.repo_metadata_path` 注入，覆盖率取决于来源
- [ ] 增量模式：新 tar 入库前先对已有内容库做 SHA-1 + MinHash 比对
- [x] XML/HTML/JSON/JavaScript 全局字节预算（`quality.byte_budget`，默认各 2e9 字节；去重后按语言累加，超出则丢，原因 `byte_budget`）
- [x] 仓库保留字节 >1.5 GiB 概率降采样（`quality.max_repo_bytes_before_downsample` / `downsample_keep_ratio`，默认 1,610,612,736 与 0.5；原因 `repo_downsample`）

## 文档

- [架构与实现](docs/architecture.md)
- [清洗规则对照：Stack v3 / 原来 / 2026-11-11](docs/CLEANING-RULES-VS-STACKV3-20261111.md)
- [github_downloads 数据说明](docs/DATA-github_downloads-20261117.md)
- [全量清洗漏斗：full-rules-20261111](docs/FUNNEL-full-rules-20261111.md)
- [repos / swhkit 卷扫描](docs/REPOS-SCAN-20261110.md)
- [repos/ 批次转换与 300 仓库试跑](docs/PILOT-repos-20261117.md)
- [产物 schema](docs/outputs.md)
- [与 Stack v3 对齐说明](docs/stack-v3-alignment.md)
- [构建数据集指南](docs/BUILD_DATASET_GUIDE.md)

## 证书

本项目采用 [MIT License](LICENSE)。
