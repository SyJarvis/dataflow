# 构建训练数据集指南（BUILD_DATASET_GUIDE）

> 从 dataflow 产物 `runs/<name>/repos.jsonl` 到训练就绪数据的下游管道。
> 配套工具：`scripts/build_dataset.py`（本文所有命令均已用 pilot10 真实产物验证）。

---

## 0. 端到端流程

```
runs/full-20261026/repos.jsonl        ← dataflow 全量产物（一行一仓库）
   │
   ① stats        质检：规模/语言分布/年份直方图 → 确认过滤参数
   │
   ② filter       子集选择：2026 活跃 / 语言 / vendor / 最小文件数
   │
   ③ split        按仓库切 train/val（防 repo 泄漏）
   │
   ④ 导出         repos.jsonl（repo-context 训练）
   │              flat.jsonl（文件级调试/抽查）
   │              .parquet（官方 stack-v3-train 同构格式，HF 发布用）
   │
   ⑤ 训练侧 reader 拆 files[] → 样本（单文件 / FIM / 跨文件）
```

所有命令流式逐行处理，多 GB 语料内存安全。

## 1. 质检：先看数再动手

```bash
.venv/bin/python scripts/build_dataset.py stats runs/<name>/repos.jsonl
```

pilot10 输出（示例）：

```
repos:            9  (empty: 0)
files:            2,581
content (UTF-8):  0.04 GB
top languages:
  Kotlin        1,008  39.1%
  ...
  unknown         182   7.1%     ← 未识别率（接 go-enry 后下降）
last_activity_year:
  2026              6  66.7%     ← 活跃语料占比
  unknown           2  22.2%     ← 无年份证据，保守可排除
```

看什么：`unknown` 语言占比（决定要不要 `--drop-unknown-lang`）、
年份直方图（决定 `--years`）、文件大小分位数（异常大的文件该在
上游质量过滤处理，不该流到训练侧）。

## 2. 过滤：推荐参数组合

```bash
# 活跃语料（当前目标）
.venv/bin/python scripts/build_dataset.py filter runs/full-20261026/repos.jsonl \
    --years 2026 --min-files 3 -o active2026.jsonl

# 保守版：排除无年份证据 + 排除未识别语言 + 排除 vendor 文件
... filter ... --years 2026 --min-files 3 --drop-unknown-lang --no-vendor -o ...

# 多年份 / 指定语言
... --years 2024,2025,2026 ...
... --langs Python,TypeScript ...
```

| 参数 | 建议值 | 理由 |
|---|---|---|
| `--years 2026` | 按需 | 内容证据年份（LICENSE/README/CHANGELOG 最大年份）；`unknown` 占比 ~20%，对外发布建议排除，内部训练可保留 |
| `--min-files 3` | 3~5 | 1-2 个文件的仓库多为占位/空仓，对 repo-context 训练无价值 |
| `--drop-unknown-lang` | 可选 | 排除扩展名表未识别的文件（当前 ~7%）；接 go-enry 后收益下降 |
| `--no-vendor` | 可选 | vendor 文件官方 Stack v3 **保留**（只在检测中标 `is_vendor`），训练侧自行决定 |

pilot 实测：`--years 2026 --min-files 3` 保留 5/9 仓库（一个 2026 仓库因
清洗后文件数不足被滤掉——符合预期）。

## 3. 切分：必须按仓库切

```bash
.venv/bin/python scripts/build_dataset.py split active2026.jsonl \
    -o ds_v1 --val-ratio 0.005 --seed 42
# → ds_v1.train.jsonl / ds_v1.val.jsonl
```

**为什么按仓库切而不是按行/按文件切**：同仓库文件高度相关
（import 同一模块、同一套 API）。文件级随机切会让 val 集里出现
train 集文件的同源近似副本，评测虚高。`--repo-list`（stdin 传入
repo_path 清单）可以固定 val 仓库集合，跨版本实验保持同一分界。

## 4. 导出格式

### repos.jsonl（默认，repo-context 训练）

直接消费 `files[]`。训练 reader 的拆样本策略：

| 样本类型 | 做法 |
|---|---|
| 单文件样本 | 逐文件取 content，按 tokenizer 上限截断 |
| FIM（fill-in-the-middle） | 单文件内随机选 span，前缀+后缀做上下文 |
| 跨文件依赖 | 读一行（一仓库）后，按 `file_path` 索引取 `import` 对应文件做上下文 |
| 仓库级长序列 | 同仓库文件按路径序拼接（仅统计/打包顺序变化，无需重跑管线） |

JSONL 的"一行"是**存储单位不是训练样本单位**——这是官方发布格式
（stack-v3-train 同构），不要按行直接 tokenize。

### flat.jsonl（文件级，调试与抽查）

```bash
.venv/bin/python scripts/build_dataset.py flatten active2026.jsonl -o flat.jsonl
# 一行一个文件，repo 级字段（repo_path / last_activity_year / github_metadata）拍平进每行
```

### parquet（官方发布格式，列式/HF 上传）

```bash
.venv/bin/python scripts/build_dataset.py to-parquet active2026.jsonl -o active2026.parquet
```

导出 schema 与官方 `stack-v3-train` **逐字段对齐**（已用 ZF5761 镜像实测校验）：
`sha1` → `content_id`，`github_metadata` 展开为官方 10 字段 struct
（采集侧注入前为类型零值），额外多一列 `last_activity_year`（我们的扩展，
官方没有）。snappy 压缩，1000 repos/row group。

## 5. 规模预估（全量 full-20261026）

| 项 | 估算 |
|---|---|
| 全量 repos.jsonl | ~9,800 仓库 / ~280 万文件 / ~38 GB |
| `--years 2026 --min-files 3` 后 | 按活跃占比 ~65-70% 估 ~6,400 仓库 / ~25 GB |
| parquet（snappy） | 约为 JSONL 的 60-70% |
| filter/split/flatten 单遍耗时 | 流式 ~10-20 分钟（IO bound） |

（全量 run 完成后用 `stats` 实测替换本表。）

## 6. 一条龙

```bash
cd ~/dataflow
R=/mnt/data2/reexen_datasets/llm_training/dataflow_runs/full-20261026/repos.jsonl

.venv/bin/python scripts/build_dataset.py stats $R | tee ds_report.txt
.venv/bin/python scripts/build_dataset.py filter $R \
    --years 2026 --min-files 3 -o active2026.jsonl
.venv/bin/python scripts/build_dataset.py split active2026.jsonl \
    -o active2026 --val-ratio 0.005 --seed 42
.venv/bin/python scripts/build_dataset.py to-parquet active2026.train.jsonl \
    -o active2026.train.parquet
```

产物建议归档到 `/mnt/data2/reexen_datasets/llm_training/` 并在
`USAGE_RULES.md` 语义下告知维护人；数据集名建议
`dataflow-stack-v3-style-2026active-v1`，版本号随过滤参数变更递增。
