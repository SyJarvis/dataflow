# NVIDIA Nemotron 代码数据清洗管线（Code v1 / v2 / v3）

> 数据集：[`nvidia/Nemotron-Pretraining-Code-v3`](https://huggingface.co/datasets/nvidia/Nemotron-Pretraining-Code-v3)（CC-BY-4.0，仅增量 metadata）
> 配套模型：Nemotron 3 Nano（arXiv:2512.20848）→ Super → Ultra（arXiv:2606.15007）
> 本文整理自 Nemotron 3 Nano / Super / Ultra 技术报告、Nemotron-CC 论文（arXiv:2412.02595）及 HF 模型卡。

---

## 1. 版本演进与规模

| 版本 | 内容 | 规模 | 配套模型 |
|---|---|---|---|
| v1 + v2 | GitHub 精选代码 + 合成代码 | 合计 **1.09T tokens** | Nemotron 3 Nano / Super |
| v3 | GitHub 增量（截止 **2025-09-30**） | **146M 新文件 / 173B tokens** | Nemotron 3 Ultra |

要点：

- v3 **只发布 metadata**（`commit_id` 前 7 位、`rel_path`、`language`），与 v1/v2 联合使用，内容不含在发布物中
- v2 的采集截止 2025-04-15，v3 覆盖 2025-04-15 → 2025-09-30 的新数据
- 另有网页侧代码语料 **Nemotron-CC-Code-v1**（428B tokens，源自 Common Crawl 代码页）

## 2. GitHub 语料基础清洗（沿袭 Nemotron 2 Nano 管线）

NVIDIA 对 GitHub 原始代码的清洗相对"经典"，核心三件套：

### 2.1 License 白名单过滤

- 收集原始源码后，**license 不在 permissive 白名单内的一律剔除**
- 白名单口径与 Nemotron-CC 系列一致（permissive / public domain 类 license）

### 2.2 两级去重

| 层级 | 方法 | 工具 |
|---|---|---|
| 精确去重 | exact duplicate 文件级比对 | — |
| 精确子串去重 | snapshot 按八分之一分块做 exact substring dedup | [deduplicate-text-datasets](https://github.com/google-research/deduplicate-text-datasets)（Lee et al., 2022） |
| 模糊去重 | **全局 MinHash-LSH fuzzy dedup** | [NeMo Curator](https://github.com/NVIDIA-NeMo/Curator) |

- 去重方式与 Nemotron-CC 完全相同（官方模型卡原话）
- **增量语料（v3）必须对既有语料（v1/v2）做交叉去重**：先剔除已存在于老语料中的 exact 与 near-duplicate 文件，再入库

### 2.3 多阶段质量过滤

- 多阶段启发式过滤 + 质量过滤器（官方未公开逐条阈值，归属于 Nemotron 2 Nano 管线）
- 采集侧策略：补抓既有语料中**缺失的仓库** + 增量抓取新提交

## 3. LLM 增强（Nemotron 的核心特色，v2 起全面应用）

基础清洗之后，NVIDIA 用 **Qwen3-32B** 对语料做了四类模型增强：

### 3.1 代码改写（Code Rephrasing）

- 依据 Fujii et al. (2025) 的结论：LLM 改写源码能提升下游代码生成精度
- 对**全量 Python 源码**用两类 prompt 改写：
  - **SGCR**（Style-Guided Code Rewriting，风格引导改写）
  - **SCOR**（Self-Contained Optimization Rewriting，自包含优化改写）
  - 外加 NVIDIA 自研的同意图 prompt
- **质量把关**：改写结果逐文件过 **Pylint**——语法错误剔除，并量化评估改写前后的代码质量提升

### 3.2 跨语言转译（Transpilation）

- 用 Qwen3-32B 把 Python 源文件**转译为 C++**
- 应用于语料中**全部 Python 文件**，产物并入 C++ 子集
- 官方实测：显著提升下游 C++ 代码生成能力

### 3.3 合成对话数据

以自有源码为种子（seed）生成：

| 数据形态 | 范围 |
|---|---|
| 问答对（QA） | 以新源码为种子 |
| student-teacher 对话 | 仅 Python |
| code-review 对话 | Python / C++，基于代码片段 + 完整源文件 |

### 3.4 网页代码清洗（Nemotron-CC-Code-v1，428B tokens）

面向 Common Crawl 中代码页的独立管线：

1. **入口分类**：快速 pattern-matching 代码页分类器筛出 CC 中的代码页
2. **渲染**：用 **Lynx** 从原始 HTML 渲染文本（可靠保留代码排版、缩进、行内技术元素）
3. **LLM 清洗**：**Phi-4** 批量清洗——去样板噪声，但**严格保留**代码片段、配置块、API 引用、数学表达式；公式标准化为 LaTeX
4. **相关性过滤**：轻量 code-quality relevance 分类器，剔除非技术页面，只留代码占比高的文档

产物：427.92B tokens，代码块结构保真、噪声最小化。

## 4. 与 The Stack v3 的方法论对比

| 维度 | The Stack v3 | Nemotron Code |
|---|---|---|
| 采集 | GH Archive ∪ SWH 清单，爬默认分支 HEAD 快照 | 补缺 + 增量抓取（v3 截止 2025-09-30） |
| License | ScanCode 文件级检测 + 目录传播，non_permissive 剔除 | permissive 白名单，不在白名单剔除 |
| 去重 | MinHash(256×5gram) LSH ≥0.7 + 连通分量 + Jaccard 复核，跨语言全局 | exact + 子串去重 + NeMo Curator MinHash 全局模糊去重，**跨版本增量交叉去重** |
| 质量过滤 | StarCoder2 启发式（行长/字母占比/自动生成标记等，全部公开） | 多阶段过滤（细节未公开） |
| PII | StarPII 模型脱敏 | 未详述 |
| LLM 增强 | **无**（保持原始代码，可复现优先） | **重点投入**：SGCR/SCOR 全量改写 + Pylint 把关、Python→C++ 转译、QA/对话合成 |
| 发布物 | 内容内联 + 全量溯源（commit_id/license/dedup_cluster） | v3 仅 metadata（commit_id/rel_path/language） |
| 哲学 | "洗数据"：规则透明、可复现、可审计 | "洗数据 + 再加工"：用模型换语料质量 |

## 5. 可复用工具链

| 环节 | NVIDIA 侧工具 |
|---|---|
| 模糊去重 / 质量过滤框架 | [NeMo Curator](https://github.com/NVIDIA-NeMo/Curator)（含 Nemotron-CC 开源 recipe） |
| 精确子串去重 | [deduplicate-text-datasets](https://github.com/google-research/deduplicate-text-datasets) |
| 训练配方 | [NVIDIA-NeMo/Nemotron](https://github.com/NVIDIA-NeMo/Nemotron)（super3 / ultra3 recipe） |
| 改写/转译参考 | Fujii et al. 2025（SGCR / SCOR prompts） |

## 6. 参考

- Nemotron 3 Nano 报告：arXiv:2512.20848（v1/v2 管线与 LLM 增强细节，本地存档：`/tmp/opencode/nemotron3wp.txt`）
- Nemotron 3 Ultra 报告：arXiv:2606.15007（v3 增量说明，本地存档：`/tmp/opencode/nemotron3ultra.txt`）
- Nemotron-CC：arXiv:2412.02595（去重与质量分桶方法，本地存档：`/tmp/opencode/nemotroncc.pdf`）
- Dataset: https://huggingface.co/datasets/nvidia/Nemotron-Pretraining-Code-v3 （本地存档：`/tmp/opencode/nemotron_code_v3.md`）
