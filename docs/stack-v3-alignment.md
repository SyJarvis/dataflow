# Stack v3 官方步骤 × dataflow 实现状态对比

> 基准：官方 dataset card（`stack-v3-train`）+ bigcode-dataset 源码；
> dataflow 代码逐模块核对于 commit `eda2a27`（2026-10-26）。
> 图例：✅ 对齐 · ⚠️ 简化实现 · ❌ 未启用 · 🔌 接线缺口（代码在、没接上）

---

## 1. 逐项对照

### 下载期丢弃（官方 4 条）

| 官方规则 | dataflow | 状态 |
|---|---|---|
| 单文件 > 5 MB 丢 | `unpack.max_file_bytes: 5242880` | ✅ |
| 含 null 字节判二进制丢 | `drop_binary: true` | ✅ |
| symlink / 非普通文件丢 | `m.isfile()` 过滤 | ✅（丢弃原因未计数，cosmetic） |
| 仓库文件数 > 1,000,000 整仓丢 | worker 检查 + `repo_drop_reason` 通道 | ✅

### ① 解码 + 语言检测

| 官方 | dataflow | 状态 |
|---|---|---|
| UTF-8 → cchardet 回退 | `decode_bytes` 同款（cchardet 可用时启用） | ✅ |
| go-enry 标注 language / is_vendor / generated | 扩展名表（~40 语言）+ shebang 探测；vendor = 目录前缀 + `.min.js/.lock` | ⚠️ pilot 未识别率 ~7% |
| SHA-1 内容寻址（同一 blob 一份） | worker 内 sha1；签名与去重均按 sha1 | ✅ |

### ② License 检测与传播（ScanCode）

| 官方 | dataflow | 状态 |
|---|---|---|
| ScanCode Toolkit SPDX 检测 | SPDX 正则 fallback + ScanCode hook 预留 | ⚠️ |
| license 文件名匹配（LICENSE/COPYING/NOTICE/…） | `license_filenames` + `license_name_patterns` | ✅ |
| 沿目录树传播、多处覆盖取并集 | `annotate_repo_licenses` | ✅ |
| permissive 白名单（Blue Oak + ScanCode 分类）/ non_permissive 剔除 / no_license 保留 | `classify` + `drop_non_permissive` | ✅ 逻辑在 |
| **步骤开关** | `steps.licenses: false` | ❌ 正则版误伤率未验证，默认关 |

### ③ MinHash 近重去重

| 官方 | dataflow | 状态 |
|---|---|---|
| 256 permutations / 5-gram / min 5 token | 同 | ✅ |
| LSH 候选 Jaccard ≥ 0.7 | `lsh_jaccard_threshold: 0.7` | ✅ |
| 候选对连通分量聚类 + 真实 Jaccard 复核（v3 新增） | UnionFind + `verify_jaccard: true` | ✅ |
| 跨语言全局去重（v3 新增） | 不分语言桶 | ✅ |
| 簇代表选择：stars → forks → permissive → 最早创建 | first-seen（出现顺序） | ⚠️ 无 stars 元数据，等采集侧注入后替换 |

### ④ 质量启发式过滤（StarCoder2 同款）

| 官方规则 | dataflow | 状态 |
|---|---|---|
| 字母占比 < 25%（汇编按字母数字） | `min_alpha_ratio` | ✅ |
| 平均行长 > 100 / 最大 > 1000（Text/JSON/XML/HTML/MD/Roff/TeX 豁免） | `line_len_exempt` | ✅ |
| 行数 > 100,000；JSON/YAML 等数据格式封顶 512 行 | `max_lines` / `data_format_line_cap` | ✅ |
| 大块 base64/hex/unicode 转义、LFS pointer 丢 | `drop_base64_hex_blobs` / `drop_lfs_pointers` | ✅ |
| 前 5 行含自动生成标记丢 | `auto_generated_marker_lines: 5` | ✅ |
| HTML 可见文本 ≥ 100 字符且 ≥ 20% | `html_min_visible_chars/ratio` | ✅ |
| 非代码语言 blocklist | `_BLOCKLIST_EXTS` | ✅（清单可继续长） |
| XML/HTML/JSON/JS 全局字节预算 | config 有 `byte_budget`，**代码未实现** | 🔌 G-2 |
| 仓库 > 1.5 GB 概率降采样 | config 有 `downsample`，**代码未实现** | 🔌 G-3 |

### ⑤ PII 脱敏

| 官方 | dataflow | 状态 |
|---|---|---|
| StarPII NER（email/key/name/password/IP → 占位符/合成 IP） | `pii.py` 双模式：正则 3 条 + StarPII pipeline 类（未装 torch 时不可达） | ❌ `steps.pii: false` |

### ⑥ Jupyter Notebook

| 官方 | dataflow | 状态 |
|---|---|---|
| 剥 cell 输出 / 内嵌图片 / 易变 metadata，保留结构化 JSON | `clean_notebook` | ✅ |

### ⑦ 按仓库分组输出

| 官方 | dataflow | 状态 |
|---|---|---|
| 每行一仓库，files[] 内联全文 | runner pass C，`RepoRecord` | ✅ |
| schema 与官方 parquet 对齐 | 已实测对照 ZF5761/stack-v3-train（见 outputs.md） | ✅（`sha1`↔`content_id` 仅字段名差异） |
| `repo_id` / `commit_id` / `github_metadata`（10 字段）/ `file_timestamp` / `detected_licenses` | 缺 | ❌ 采集侧注入 / ScanCode 后补 |

### ➕ dataflow 独有扩展（官方没有）

| 扩展 | 说明 |
|---|---|
| `last_activity_year` + `evidence_files` | 内容证据年份标注（LICENSE/README/CHANGELOG 等的最大年份）。动因：GitHub tarball 的 mtime 全被覆盖为打包时间，不可用；pilot 命中 7/9 仓库。准确年份等采集侧 `pushed_at` |

## 2. 接线缺口清单与处置

| # | 缺口 | 影响 | 状态（2026-10-27） |
|---|---|---|---|
| G-1 | `max_repo_files` 未接进并行 worker | 单仓 tar >100 万文件概率≈0 | ✅ 已修：worker 检查 + `repo_drop_reason` 通道 + manifest `dropped_repos` 独立记录 |
| G-2 | `byte_budget` 全局字节预算未实现 | 防数据格式淹没的第二道闸；文件级 512 行封顶已在挡 | 📊 已加决策统计（report `byte_share_top_langs`）；pilot 中 JSON 字节占 22.6% 而文件数仅 6%，全量跑完据 此决定是否实现 |
| G-3 | 1.5GB 仓库降采样未实现 | 触发条件罕见（>1.5GB 单仓） | 📊 已加决策统计（report `repos_over_1_5gb`，post-cleaning 口径）；全量跑完据此决定 |
| G-4 | symlink/二进制/超限丢弃无计数 | report 里看不到这类丢弃 | ✅ 已修：unpack 阶段 reasons 全计数（pilot 实测 4312→2909，symlink 838/binary 561/oversize 4） |

## 3. 结构性差距（非代码问题）

1. **语言检测精度**：go-enry 是官方口径的正身，接入后未识别率 7% → ~0.5%；
   `languages.go_enry_bin` 参数位已留，装二进制即可
2. **License 精度**：ScanCode 是官方正身；正则版启用前需小样本评测误伤率
3. **元数据注入**（采集侧配合）：`repo_id` / stars / forks / `pushed_at` /
   `commit_id` / `file_timestamp`——同时解锁官方代表选择顺序与精确年份切片
4. **PII**：内部训练可用官方 regex 路线（detect-secrets + gibberish 过滤）；
   对外发布需 StarPII NER（~40 GPU·h，见 starpii 文档 §7）

## 4. 结论

核心清洗链（质量 12 条 + MinHash 去重 + notebook + 分组输出）与官方口径
**一致且经 pilot 数字回归验证**（2909 → 2717 → 2581，与串行实现精确一致）。
剩余差距分三类：接线缺口（4 个，均小）、精度升级（go-enry / ScanCode /
StarPII，工程接入项）、元数据（依赖采集侧）。均不影响当前全量运行的
语料可用性。
