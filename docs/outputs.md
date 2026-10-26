# dataflow 产物说明

每次运行产出 `runs/<name>/` 目录，包含三个文件：**`repos.jsonl`（语料）+ `report.json`（质检单）+ `manifest.json`（进度账本）**。

```
runs/pilot10/
├── repos.jsonl     # 最终语料：训练侧消费的产品
├── report.json     # 各阶段统计：判断规则松紧的依据
└── manifest.json   # 断点恢复账本：--resume 的依据
```

---

## 1. repos.jsonl —— 最终语料

每行一个 JSON 对象 = **一个完整仓库**，与 The Stack v3 `stack-v3-train` 的发布形态同构
（每行一个 repo、`files[]` 内联内容，天然支持 repository-context 训练）。

### 顶层结构（一行）

| 字段 | 类型 | 说明 |
|---|---|---|
| `repo_path` | string | 仓库标识 `owner/name`（由 tar 路径推出） |
| `commit_id` | string | 快照对应的 commit（采集侧注入，当前为空） |
| `num_files` | int | 清洗后留存的文件数 |
| `files` | list[object] | 文件对象数组，见下表 |
| `github_metadata` | object | stars / forks / issues / is_fork 等（采集侧注入，当前为空） |
| `dropped` | bool | 整仓是否被丢弃 |
| `drop_reason` | string | 整仓丢弃原因（如 `repo_exceeds_max_files`） |

### 文件对象（files[i]）

| 字段 | 类型 | 说明 | 示例 |
|---|---|---|---|
| `file_path` | string | 仓库内相对路径（已剥掉 tar 顶层目录） | `.gitignore` |
| `content` | string | **清洗后的文件全文**（经质量过滤/去重/脱敏的最终文本） | `"# macOS\n..."` |
| `language` | string | 语言标注；空串 = 未识别（接 go-enry 后减少） | `Kotlin` |
| `sha1` | string | 原始内容 SHA-1，精确去重与溯源的 key | `50dd90b7...` |
| `size_bytes` | int | 原始字节大小 | 334 |
| `is_vendor` | bool | 是否第三方/依赖文件（vendor/ node_modules/ 等） | false |
| `license_type` | string | `permissive` / `no_license` / `non_permissive`（③ 步启用后填充） | |
| `drop_reason` | string | 保留时为空 | |

> 注意：`sha1` 是**原始内容**的哈希（与 Stack v3 的 `content_id` 一致）；
> `content` 若经过了 ⑥ PII 脱敏，与 sha1 对应的原文不同。需严格对齐时以发布时说明为准。

### 下游用法

```python
import json

for line in open("runs/pilot10/repos.jsonl"):
    repo = json.loads(line)
    if repo["dropped"]:
        continue
    for f in repo["files"]:
        if f["language"] == "Python":          # 按语言配比采样
            text = f["content"]                # 直接作为训练文本
```

- 训练侧逐行流式读取，无需全量加载
- 按 `files[].language` 做配比、按 `repo_path` 分组做 repo-context 训练
- 发布数据集时转 Parquet（`dataflow` 的 `group.py: write_parquet`）
- 量级到百 GB 后切换为 Stack v3 full 式"内容库 + 引用"分离存储，`sha1` 为 join key

---

## 2. report.json —— 质检单

```json
{
  "run_name": "pilot10",
  "tarballs_seen": 10,          // 发现的 tar 数
  "tarballs_processed": 10,     // 实际处理（含失败）
  "tarballs_failed": 1,         // 损坏/无法解析（下载中断的假 tar 等）
  "repos_out": 9,               // 输出仓库数
  "files_out": 2581,            // 输出文件数
  "elapsed_sec": 84.8,
  "stages": {                   // 每阶段: seen / kept / dropped / reasons 分布
    "quality": { "seen": 2909, "kept": 2717, "dropped": 192,
                 "reasons": { "low_alpha_ratio": 50, "...": 0 } },
    "dedup":    { "seen": 2717, "kept": 2581, "dropped": 154,
                 "reasons": { "exact_duplicate": 50, "near_duplicate": 104 } }
  }
}
```

用途：

- **判断规则松紧**：某条 reason 占比异常高（如 `data_format_over_line_cap` 丢 30%），
  说明该阈值对当前语料太激进 → 调 `configs/default.yaml` 后用**新 run name** 重跑
- **监控数据质量趋势**：不同批次对比丢弃率变化，发现采集端问题

各丢弃原因速查：

| 阶段 | reason | 含义 |
|---|---|---|
| unpack | `binary_null_byte` / `oversize_file` / `symlink_or_nonregular` | 下载期丢弃规则 |
| languages | `undecodable` | 非 UTF-8 且检测不出编码 |
| quality | `low_alpha_ratio` | 字母占比 <25%（StarCoder2 规则） |
| quality | `avg_line_length` / `max_line_length` | 行长超限 |
| quality | `too_many_lines` / `data_format_over_line_cap` | 行数超限 / JSON·YAML 等超 512 行 |
| quality | `auto_generated` | 前 5 行含自动生成标记 |
| quality | `lfs_pointer` / `encoded_blob` / `blocklisted_extension` | LFS / 大块 base64·hex / 非代码扩展名 |
| quality | `html_low_visible_text` / `html_low_visible_ratio` | HTML 可见文本不足 |
| licenses | `non_permissive` | GPL 类（③ 步启用后） |
| dedup | `exact_duplicate` / `near_duplicate` | 精确重复 / MinHash≥0.7 近重 |

---

## 3. manifest.json —— 进度账本

```json
{
  "fingerprint": "a3f9c2d1e0b84756",   // config 内容哈希
  "processed": [".../a11ce/docker-lisp_HEAD.tar.gz", "..."],
  "failed": {
    ".../ProAlit/aio-downloader_HEAD.tar.gz": "ReadError: not a gzip file"
  }
}
```

三项职责：

1. **断点恢复**：`dataflow run configs/default.yaml --resume` 只处理
   `processed` 之外的 tar；已知的坏 tar 直接跳过不再重试
2. **配置变更保护**：`fingerprint` 与当前 config 不一致时拒绝 resume，
   防止新旧配置的结果混进同一个 run
3. **坏文件台账**：`failed` 里记录每个损坏 tar 的原因，供采集侧修复重下

---

## 与 Stack v3 发布格式的对应（已对照 ZF5761/stack-v3-train 官方镜像实测确认）

官方形态：**每行一个仓库、`files[]` 内联文件全文**（Spark 写 Parquet，单分片 ~2.1 万仓库）。
JSONL 的"一行"是存储/分发单位，不是训练样本单位——训练 loader 拆开 `files[]` 后按文件/组合切样本。

| Stack v3 字段 | dataflow 字段 | 状态 |
|---|---|---|
| `repo_path` / `num_files` / `commit_id` | 同名 | ✅ 一致（commit_id 待采集注入） |
| `files[].content` (内联全文) | `files[].content` | ✅ 一致 |
| `files[].content_id` (SHA-1) | `files[].sha1` | ✅ 一致，仅字段名不同 |
| `files[].language` / `is_vendor` / `license_type` | 同名 | ✅ 一致（license 待③步启用） |
| `files[].size_bytes` / `file_path` | 同名 | ✅ 一致 |
| `files[].file_timestamp` | — | ❌ 缺，采集侧注入 |
| `files[].detected_licenses` (SPDX list) | — | ❌ 缺，③步 ScanCode 后补 |
| `repo_id` (GitHub 数字 ID) | — | ❌ 缺，采集侧注入 |
| `github_metadata` (结构化 10 字段: branch/commit_count/repo_created_at/is_fork/is_org_owned/forked_from/stars/forks/issues/pull_requests) | `{}` 占位 | ⚠️ 待采集注入 |
| `dedup_cluster` (full 版) | — | 路线图（去重中间产物） |
