# dataflow 清洗规则对照：Stack v3 / 原来 / 改完之后

- 日期：2026-11-11（实施后更新）
- 对照来源：[Stack v3 数据卡 · Dataset creation](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train)
- 原来：`full-enry-20261105` 使用的规则，代码版本 `3d01d14`，配置 `configs/full-enry.yaml`
- 改完之后：已实施并跑过 pilot。代码见本地提交（未推送），镜像 `dataflow:rules-20261111`，配置 `configs/pilot-rules-20261111.yaml` / `configs/full-rules-20261111.yaml`

## 清洗目标

- **保留：** 英文文本、中文文本，以及各种后缀的文本类文件（代码、文档、配置等）。
- **剔除：** 图片、音频、视频、二进制文件。
- **不做年份限制。**
- **PII 脱敏保持关闭，许可证过滤保持关闭。**

## 对照表

| 步骤 | Stack v3 | 原来（20261105） | 改完之后 | 改完与 Stack v3 比 |
|---|---|---|---|---|
| ① 下载/解包 | 丢 >5MB 文件、含 NUL 字节的二进制、symlink；每仓库最多 100 万文件 | 相同 | 相同 | 一致 |
| ② 解码与语言识别 | UTF-8，失败回退 cchardet；go-enry；按 SHA-1 内容寻址 | 相同 | 相同 | 一致 |
| ③ 许可证 | ScanCode 识别并沿目录树传播，删除 non_permissive | 关闭 | 关闭 | ⚠️ 缺失 |
| ④ 近重去重：参数 | MinHash 256 置换、5-gram、LSH 0.7，跨语言 | 相同 | 相同 | 一致 |
| ④ 近重去重：复核 | 用 Jaccard 估计值复核 | 用估计值复核（真实 0.688、估计 0.715 的一对会被误删） | 对 LSH 候选对计算真实 shingle Jaccard（与 MinHash 同一套 5-gram）再合并 | 我们更准 |
| ④ 近重去重：代表选择 | 依次按 stars、forks、宽松许可证、最早创建 | 按路径排序取第一个 | 按 stars → forks → 宽松许可证 → 最早创建 → 路径排序+sha1 选代表；精确去重的保留副本也按同一顺序 | 逻辑一致，**元数据不全**（见下） |
| ⑤ 质量：字母占比 | <25% 丢 | 只算 `[A-Za-z]`，中文按 0% 计、被删 | 按 Unicode 字母计（`str.isalpha`；汇编用 `str.isalnum`） | 为多语言主动放宽 |
| ⑤ 质量：其他规则 | 行长、行数、数据格式 512 行、base64/hex 块、LFS、自动生成标记、HTML、黑名单 | 相同 | 相同 | 一致 |
| ⑤ 质量：字节预算 | XML/HTML/JSON/JavaScript 按字节封顶 | 配置里写了，代码未实现 | 已实现：去重后每种语言按固定种子哈希顺序累加，超过 2e9 字节之后的文件丢弃 | 一致（上限值自定） |
| ⑤ 质量：>1.5GB 仓库降采样 | 概率降采样 | 配置里写了，代码未实现 | 已实现：去重后保留字节 >1.5 GiB 的仓库，每个文件按固定种子哈希以 0.5 概率保留 | 一致 |
| ⑥ PII 脱敏 | StarPII | 关闭 | 关闭 | ⚠️ 缺失 |
| ⑦ Notebook | 剥离输出、图片、易变 metadata，保存为 JSON | 先过质量规则再剥离，带图的基本被删 | 先剥离，再过质量规则（输出 JSON 用 nbformat 的 indent=1 格式） | 改完一致 |
| ⑧ 按仓库分组 | 每个仓库一行 | 相同 | 相同；有元数据的仓库在 `github_metadata` 里带上 stars | 一致 |
| 年份限制 | 无 | 标注“2026 活跃”，训练集 `active2026` 按此筛选 | 去掉：报告不再有 `repos_active_2026`，`build_dataset.py filter` 去掉 `--years`；`last_activity_year` 仅作参考字段保留 | 改完一致 |

## 本次修改清单（已实施）

1. **字母占比改按 Unicode 计算。** `src/dataflow/quality.py` `alpha_ratio`。
2. **Notebook 先剥离输出和图片，再过质量规则。** `src/dataflow/runner.py` 调换两步顺序；`notebooks.py` 输出改为 indent=1，避免整份 notebook 变成一行而触发 1000 字符行长规则。
3. **近重去重按真实 Jaccard 复核。** `src/dataflow/execution.py` `_near_duplicates`：先用 LSH 找候选对，估计值低于 0.6（阈值减 0.1，256 置换下约 3.4σ）的直接跳过；其余候选对用到的文件从中间结果重算 shingle 集合，存到临时文件（用完删除），逐对算真实 Jaccard，≥0.7 才合并。已在同一簇里的对直接跳过。可配置 `dedup.verify_method: exact | estimate`。
4. **代表选择按 Stack v3 顺序。** 新配置项 `dedup.repo_metadata_path`（TSV/JSONL：repo_path 或 url、stars、forks、created_at、license），由 `scripts/build_repo_metadata.py` 生成。缺失字段排最后，全平手时回退“路径排序 + sha1”。报告里记 `representative_policy` 和元数据覆盖率。
5. **字节预算与大仓库降采样。** `src/dataflow/budget.py`，在去重之后执行：先降采样，再按语言做字节预算。丢弃原因在报告 `stages.budget` 里分别记为 `repo_downsample` / `byte_budget`。种子 `quality.sampling_seed: 20261111`，同样输入每次结果相同。
6. **去掉 2026 年份限制。** 见对照表最后一行。
7. **datasketch 固定为 `>=2.0.0,<3`**（需要 `scheme="affine32"`，镜像内为 2.0.0）。

> 不做：控制字符比例检查、密钥文件过滤（均已从方案中删除）、PII（关闭）、许可证（关闭）。

## 元数据实际可用情况

| 来源 | stars | forks | 创建时间 | 许可证 |
|---|---|---|---|---|
| `github_downloads/` 本身 | 无（只有 `<user>/<repo>_HEAD.tar.gz`） | 无 | 无 | 无 |
| `cleaning_data/MANIFEST.tsv`（`repos/` 批次清单） | 有，覆盖 github_downloads 10,034 个仓库中的 **4,533 个（45%）** | 无 | 无 | 无 |
| `softwareheritage-graph-2026-06-04` | 没有 stars/forks；目录里只有 content ORC 和未下完的压缩图文件 | — | — | — |

所以对 `github_downloads` 来说，代表选择实际生效的只有 **stars**（45% 仓库有值）。forks、创建时间、许可证都拿不到，这几级对所有仓库都是平手，最后落到“路径排序 + sha1”。stars 是 2026-11 抓取 `repos/` 时的值。

## Pilot 结果（同一批 300 个包，对比 `pilot-enry-opt2-20261105`）

| 指标 | 原来 | 改完 | 变化 |
|---|---|---|---|
| 保留文件 | 103,393 | 104,101 | +708 |
| 保留字节 | 913.4 MB | 938.1 MB | +24.8 MB（+2.7%） |
| 保留仓库 | 267 | 268 | +1 |
| 质量过滤：`low_alpha_ratio` | 2,774 | 2,342 | −432 |
| 质量过滤：`max_line_length` | 1,352 | 1,327 | −25（notebook） |
| 精确去重 | 18,767 | 18,780 | +13 |
| 近重去重 | 18,568 | 18,303 | −265 |
| 字节预算 / 降采样 | — | 0 / 0 | pilot 规模达不到上限 |
| 保留的 notebook | 23 | 40 | +17 |
| 耗时 | 368 s | 409 s | +11% |

- **Unicode 字母占比：** 因此新增保留 422 个文件（11.9 MB），其中 359 个以中日韩文字为主，大部分是 Markdown（328）。
- **近重复复核：** 有 2,149 对估计值 ≥0.7、真实值 <0.7，按旧规则会被合并，现在不合并；另有 709 对估计值 <0.7、真实值 ≥0.7，现在会合并。抽样对如 `login/index.ts` 与 `logout/index.ts`（估计 0.70，真实 0.64）。
- **代表选择：** 有 90 个内容的保留副本换到了有 stars 的仓库；有 stars 的仓库保留文件从 15,518 增加到 15,880。
- **少保留的 437 个文件：** 来自上面两项，一是新合并的近重复对，二是簇代表换成了高 stars 仓库里的版本。

## 仍与 Stack v3 不同的地方

许可证关闭、PII 关闭；代表选择缺 forks、创建时间、许可证三项数据，github_downloads 只有 45% 仓库有 stars；字节预算上限 2e9 是自定的（Stack v3 未公开具体值）。
