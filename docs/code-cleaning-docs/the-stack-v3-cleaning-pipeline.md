# The Stack v3 代码清洗管线

> 数据集：[`HuggingFaceCode/stack-v3-train`](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train)（训练版）与 [`stack-v3-full`](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-full)（完整原始版）
> 发布方：Hugging Face / BigCode 系团队（Lozhkov et al., 2026），许可 ODC-By v1.0
> 本文整理自官方 dataset card，参数均为官方口径。

---

## 1. 概览

The Stack v3 是目前最大的开源代码数据集，直接爬取 GitHub（快照日期 **2025-08-07**），面向带仓库上下文的代码 LLM 预训练。

| | Stack v1 | Stack v2 | **Stack v3** |
|---|---|---|---|
| 截止时间 | 2022 | 2023 | 2025 |
| 完整语料 | 6.4 TB | 67.5 TB | **113.7 TB** |
| 去重后 | 2.9 TB | 32.1 TB | 99.3 TB |
| 训练子集 | ~200B tokens | 2.0 TB / ~550B tokens | **11.5 TB / ~3.6T tokens** |
| 语言数 | 358 | 618 | 713（完整版 770） |

- 训练子集：15.9 TB、173M 仓库、713 种语言、~3.6T tokens（v3.1 修复一个分区 bug 导致的精确重复泄漏后，从 4.9T 降到 3.6T）
- 完整语料：224M 仓库、439 亿文件（含二进制/超大/未识别语言的 stub 条目）

## 2. 数据采集

**仓库清单** = 以下两者的并集：

1. [GH Archive](https://www.gharchive.org/) 公开事件数据中出现过的所有仓库
2. Software Heritage 图谱 origin 表（2025-05-18 快照）中的所有 GitHub origin

**抓取规则：**

- 每个仓库只取**默认分支 HEAD 的单次快照 tarball**，不收 git history
- 同时抓取仓库页 metadata：stars / forks / issues / PRs / 默认分支 / HEAD commit / 是否 fork / 是否组织持有
- **fork 仓库只收 stars ≥ 5 的**
- 每个仓库的快照可通过 `commit_id` 精确追溯

**下载阶段即丢弃：**

| 规则 | 说明 |
|---|---|
| 文件 > 5 MB | 直接丢 |
| 二进制文件 | 含 null 字节即判为二进制 |
| symlink / 非普通文件 | 丢 |
| 仓库文件数 > 1,000,000 | 封顶丢弃 |

## 3. 清洗管线（Spark 实现，7 步）

### 3.1 解码与语言检测

- 文件字节解码为 UTF-8，失败时回退 `cchardet` 编码检测
- 用 [`go-enry`](https://github.com/go-enry/go-enry)（GitHub Linguist 的 Go 移植）标注：`language`、`is_vendor`（第三方/依赖文件）、generated 标记
- 每个唯一文件内容按 **SHA-1 内容寻址**（`content_id`），同一 blob 只存一份

### 3.2 License 检测与传播（ScanCode）

1. **定位 license 文件**：文件名匹配 license 模式（`LICENSE`、`COPYING`、`MIT.txt`、`Apache2.0`、`NOTICE`、`README`、`GUIDELINES` 等）
2. **检测**：[ScanCode Toolkit](https://github.com/nexB/scancode-toolkit) 扫描内容，返回 SPDX 标识
3. **沿目录树传播**：license 文件的检测结果传播给同目录前缀下的所有文件——仓库根的 LICENSE 覆盖全仓库，`src/lib/LICENSE` 只覆盖该子树；多目录覆盖时 license 取并集
4. **分类**：
   - `permissive`：至少一个宽松 license 且无冲突的非宽松 license
   - `no_license`：未检测到，或只有 CLA 之类非 license 法律文本
   - `non_permissive`：GPL 类等
   - **`non_permissive` 文件从两个发布版中全部剔除**

白名单 = [Blue Oak Council](https://blueoakcouncil.org/list) 列表 + ScanCode 归类为 Permissive / Public Domain 的 license。

### 3.3 近重去重（MinHash + LSH）

| 参数 | 值 |
|---|---|
| MinHash 置换数 | 256 |
| n-gram | 5-gram（最少 5 个 token） |
| LSH 候选阈值 | 估计 Jaccard ≥ 0.7 |
| 聚类方式 | 候选对连通分量（connected components） |

v3 相对 v2 的两个变化：

- **跨语言全局去重**（v2 是按语言分别做）
- 聚类后对候选对做 **Jaccard 复核**，消除 LSH 假阳性

`stack-v3-train` 每个重复簇只保留 **1 个代表文件**，代表挑选优先级：

1. stars 最高的仓库
2. forks 最多的仓库
3. permissive license
4. 仓库创建时间最早

`stack-v3-full` 保留全部近重复并给出 `dedup_cluster` ID，供自行去重。

### 3.4 质量启发式过滤（StarCoder2 同款，应用于 train 子集）

| 过滤规则 | 阈值/说明 |
|---|---|
| 字母字符占比 | < 25% 丢弃（汇编按字母数字占比） |
| 行长 | 平均 > 100 或最大 > 1000 丢弃（Text/JSON/XML/HTML/Markdown/Roff/TeX 豁免） |
| 行数 | > 100,000 行丢弃；JSON/YAML 等数据格式封顶 **512 行** |
| 数据块 | 大块 base64/hex/unicode 转义、LFS pointer 丢弃 |
| 自动生成 | **前 5 行**含 auto-generation 标记丢弃 |
| HTML | 可见文本 ≥ 100 字符且占内容 ≥ 20% 才保留 |
| 非代码语言 | 维护一份 blocklist（按语言/扩展名） |
| 大语种封顶 | XML/HTML/JSON/JavaScript 各设字节预算 |
| 超大仓库 | 总量 > 1.5 GB 的仓库按概率降采样（兼容 Arrow/Parquet 限制） |

### 3.5 PII 脱敏

用 [`StarPII`](https://huggingface.co/bigcode/starpii)（BigCode 训练的 NER 模型）扫描：

- 检测项：email、密钥、人名、密码、IP 地址
- 替换为占位符：`<EMAIL>`、`<KEY>`、`<NAME>`、`<PASSWORD>`、合成私有 IP

官方声明检测不完美，残留敏感信息仍可能存在。

### 3.6 Jupyter Notebook 特殊处理

`.ipynb` 单独处理后再进入 license / 去重 / PII 流程：

- **剥掉** cell 输出、内嵌图片、易变 metadata
- 保留为结构化 JSON
- `stack-v3-full` 保留原始 `.ipynb`，可自行处理

### 3.7 按仓库分组

最终 train 集每行 = 一个完整仓库：`repo_path`、`repo_id`、`commit_id`、`github_metadata`、`files[]`（内联 UTF-8 内容 + 逐文件元数据），天然支持 repository-context 训练。

## 4. 发布形态与合规

| | `stack-v3-train` | `stack-v3-full` |
|---|---|---|
| 内容 | 近重去重 + 质量过滤 + PII + notebook 清洗，内容内联 | 过滤前完整语料（metadata + contents 按 `content_id` join） |
| 用途 | 直接训练 | 自定义过滤/自建去重 |
| 额外信息 | — | 保留全部近重复及 `dedup_cluster`、过滤前信号、二进制/超大/未识别文件的 stub |

- **Opt-out**：开发者可通过 ["Am I in The Stack?"](https://huggingface.co/spaces/HuggingFaceCode/in-the-stack) 申请移除，每次 patch release 前生效
- 每个文件带 `detected_licenses`（SPDX）与 `commit_id` 溯源，方便下游遵守原 license 的 attribution 义务

## 5. 版本记录

| 版本 | 变更 |
|---|---|
| v3.1 | 修复分区 bug 导致的精确重复文件泄漏，token 数 4.9T → 3.6T |
| v3.0 | 首发 |

## 6. 参考

- Dataset card: https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train （本地存档：`/tmp/opencode/stack_v3_train.md`）
- The Stack v2 / StarCoder2: arXiv:2402.19173
- 工具：[go-enry](https://github.com/go-enry/go-enry)、[ScanCode](https://github.com/nexB/scancode-toolkit)、[StarPII](https://huggingface.co/bigcode/starpii)
