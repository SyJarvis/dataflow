# dataflow

**Stack v3 风格的源代码清洗管线** —— 输入 GitHub 快照 tarball，输出仓库分组的训练语料。

[English](README_EN.md) | [方法论来源 (stack-v3-train)](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train) | Python >=3.10

## 概述

dataflow 将 github_downloads 风格的仓库快照目录清洗为可直接用于训练的语料。方法论与全部阈值来自 [The Stack v3 官方 dataset card](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train)（Dataset creation 节），并与官方产物做过 schema 对齐验证。

核心特性：

- **Stack v3 官方口径**：过滤阈值、去重参数均按官方 dataset card 复刻，不擅自调参
- **纯 Python 主干**：核心步骤无重依赖；dedup / parquet / scancode / pii 为可选 extras
- **两遍全局去重**：SHA-1 精确去重 + MinHash LSH（256 置换 × 5-gram，Jaccard ≥ 0.7）跨语言近重去重
- **断点恢复**：manifest.json 记录 config 指纹与已处理 tar 清单，`--resume` 跳过已完成部分
- **官方同构产物**：repos.jsonl 与 stack-v3-train 发布形态同构（每行一仓库、files[] 内联全文），可转 Parquet 直接对接 HF 发布

## 新闻

- **2026-10-27** 第一版本发布：支持 Stack v3 数据管线清洗数据。

## 快速开始

### 环境要求

Python >= 3.10。可选依赖按需安装：

| extra | 启用功能 |
|---|---|
| `dedup` | MinHash LSH 近重去重（不装则仅精确去重） |
| `parquet` | Parquet 输出（不装则仅 JSONL） |
| `scancode` | ScanCode Toolkit 许可证识别（不装则用正则版） |
| `pii` | StarPII 模型脱敏（不装则正则兜底，默认关闭） |

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

### 产物格式

每次运行产出 `runs/<name>/`，三个文件各司其职：

```
runs/<name>/
  repos.jsonl     # 最终语料：每行一个仓库，files[] 内联清洗后全文
  report.json     # 质检单：各阶段 seen/kept/dropped 与原因分布
  manifest.json   # 进度账本：config 指纹 + 已处理/失败 tar 清单（--resume 依据）
```

**repos.jsonl** —— 每行一个 JSON 对象 = 一个完整仓库：

| 字段 | 类型 | 说明 |
|---|---|---|
| `repo_path` | string | 仓库标识 `owner/name`（由 tar 路径推出） |
| `num_files` | int | 清洗后留存的文件数 |
| `files` | list | 文件对象数组，见下 |
| `commit_id` | string | 快照 commit（采集侧注入，当前为空） |
| `github_metadata` | object | stars / forks 等（采集侧注入，当前为空） |
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
| quality | `low_alpha_ratio` | 字母占比 < 25%（StarCoder2 规则） |
| quality | `avg_line_length` / `max_line_length` | 行长超限 |
| quality | `too_many_lines` / `data_format_over_line_cap` | 行数超限 / JSON·YAML 等数据格式超 512 行 |
| quality | `auto_generated` | 前 5 行含自动生成标记 |
| quality | `lfs_pointer` / `encoded_blob` / `blocklisted_extension` | LFS / 大块 base64·hex / 非代码扩展名 |
| quality | `html_low_visible_text` / `html_low_visible_ratio` | HTML 可见文本不足 |
| licenses | `non_permissive` | GPL 类（③ 步启用后） |
| dedup | `exact_duplicate` / `near_duplicate` | 精确重复 / MinHash ≥ 0.7 近重 |

**manifest.json** —— 三项职责：① `--resume` 只处理 `processed` 之外的 tar，已知的坏 tar 直接跳过不再重试；② `fingerprint`（config 内容哈希）与当前 config 不一致时拒绝 resume，防止新旧配置结果混进同一 run；③ `failed` 记录每个损坏 tar 的原因，供采集侧修复重下。

## 数据管道

```
<user>/<repo>_HEAD.tar.gz
  ① unpack      流式解包；>5MB/二进制/symlink/百万文件上限丢弃
  ② languages   UTF-8(回退cchardet) + go-enry 语言/vendor 标注 + SHA-1 内容寻址
  ③ licenses    license 文件识别(ScanCode或正则) + 目录树传播 + non_permissive 剔除   [默认关]
  ④ quality     StarCoder2 启发式 12 条规则（行长/字母占比/512行cap/自生成标记…）
  ⑤ notebooks   .ipynb 剥输出/内嵌图/易变 metadata
  ⑥ pii         StarPII 脱敏（无模型时正则兜底）                                      [默认关]
  ⑦ dedup       MinHash(256置换×5gram) LSH Jaccard≥0.7 + 连通分量 + 复核；跨语言全局   [装datasketch生效]
  ⑧ group       按仓库分组 → runs/<name>/repos.jsonl (或 Parquet)
```

### 实现状态

| 步骤 | 状态 | 说明 |
|---|---|---|
| ①②④⑤ | ✅ 完整 | 纯 Python，无重依赖 |
| ③ | ✅ 正则版 | ScanCode 接入点已留（`licenses.annotate_repo_licenses`），白名单=BlueOak+常见宽松license |
| ⑦ | ✅ 降级可用 | 装 `datasketch` 后为完整 MinHash LSH；不装则仅精确去重 |
| ② 语言检测 | ⚠️ fallback | 内置扩展名+shebang 表（~60语言）；接 go-enry CLI 后替换（`languages.go_enry_bin`） |
| ⑥ | ⚠️ 正则兜底 | 需 transformers+StarPII 才启用模型脱敏 |
| ⑧ Parquet | 可选 | 装 `pyarrow` |

与官方 stack-v3-train 的 schema 对应：`repo_path` / `num_files` / `files[].content` / `language` / `is_vendor` / `license_type` / `size_bytes` / `file_path` 一致；`sha1` ↔ 官方 `content_id`（仅字段名不同）。暂缺（等采集侧注入 / ScanCode 后补）：`repo_id`、`commit_id`、`github_metadata`（10 字段）、`file_timestamp`、`detected_licenses`。独有扩展：`last_activity_year`（内容证据年份标注，绕开 tar mtime 被打包时间覆盖的问题）。

## 使用

### 配置

| 配置 | 用途 |
|---|---|
| [default.yaml](configs/default.yaml) | 默认配置，`max_tarballs: 10` 小规模试跑 |
| [full.yaml](configs/full.yaml) | 全量运行 |
| [pilot-verified.yaml](configs/pilot-verified.yaml) | 60 合并版真实输入回归（前 10 条中的 9 个 valid，PII 关闭） |
| [full-verified.yaml](configs/full-verified.yaml) | 验收版全量（尚未完成口径对齐，勿直接启动） |

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

## 路线图

- [ ] 接入 go-enry CLI（语言检测精度对齐 Linguist）
- [ ] 接入 ScanCode Toolkit（SPDX 全量识别）
- [ ] github_metadata（stars/forks）注入 → 代表选择完整实现
- [ ] 增量模式：新 tar 入库前先对已有内容库做 SHA-1 + MinHash 比对
- [ ] XML/HTML/JSON/JS 全局字节预算：report 已输出 `byte_share_top_langs`，待全量跑完后决策是否实现
- [ ] repo >1.5GB 概率降采样：report 已输出 `repos_over_1_5gb`，待全量跑完后决策是否实现

## 证书

本项目采用 [MIT License](LICENSE)。
