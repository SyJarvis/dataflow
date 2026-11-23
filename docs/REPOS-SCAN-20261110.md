# 新数据扫描报告：repos / swhkit 卷

对 `/mnt/data4/cleaning_data/repos` 的全量元数据与完整性检查，再加分层抽样校验与内容抽查。时间均为亚洲/上海（UTC+8）。扫描完成于 2026-11-10 17:09。

页面版：[新数据扫描报告：repos / swhkit 卷](https://cursor.com/canvas/store-96cf0eff-2f1c-450e-959c-03fd65744db8/bdd93a93-631c-4817-a744-cf9beef37a10)

**数据来源：**

- `/mnt/data4/cleaning_data/VOLUME.json`
- `/mnt/data4/cleaning_data/MANIFEST.tsv`
- `/mnt/data4/cleaning_data/SHA256SUMS`
- `/mnt/data4/cleaning_data/repos/`
- `/mnt/data4/cleaning_data/.parts/`
- `/mnt/data4/cleaning_data/github_downloads/`
- `dataflow_runs/full-enry-20261105-acceptance/new-data/` 下 `compare.json` 与本次扫描结果

## 目录

- [先看结论](#先看结论)
- [1. 总览](#1-总览)
- [2. 完整性](#2-完整性)
- [3. 分布](#3-分布)
- [4. 内容抽样](#4-内容抽样)
- [5. 与 github_downloads 的重叠](#5-与-github_downloads-的重叠)
- [6. 可能的问题](#6-可能的问题)
- [7. 建议（仅建议，未开跑）](#7-建议仅建议未开跑)
- [检查方法与留档](#检查方法与留档)

## 先看结论

这是 swhkit 1.6.2 在 2026-11-04 15:46 至 11-07 17:17 封好的 git bundle 卷，共 53,558 仓、42,611 用户、427.48 GiB。现有 dataflow 管线不能直接吃 bundle，要先转成 `用户/仓名_HEAD.tar.gz`。清单与磁盘一一对应（除 16 个仓名末尾点被文件系统掉了，文件都在且大小一致）。`.parts` 里还占 441.55 GiB，是打包剩余工作目录，处理时可以忽略。

| 指标 | 数值 | 说明 |
| --- | --- | --- |
| 仓库数 | 53,558 | 清单全量，每仓一个产物 |
| 用户数 | 42,611 | `repos/` 下用户目录 |
| 封卷体积 | 427.48 GiB | 458,998,191,543 字节 |
| 只在新批 | 49,033 | 占新数据 381.04 GiB |
| 两边都有 | 4,525 | 与 `github_downloads` 重叠 |
| `.parts` 剩余 | 441.55 GiB | 可忽略，不影响处理 |

## 1. 总览

采集工具是 swhkit 1.6.2。卷标识 vol-01，`VOLUME.json` 里 path 为 Windows 盘符 `F:\`，状态 sealed。清单、校验和元数据在 `/mnt/data4/cleaning_data/` 根下，不在 `repos/` 里面。

| 项目 | 内容 |
| --- | --- |
| 工具 | swhkit 1.6.2 |
| 卷标识 | vol-01（state: sealed） |
| 打开时间 | 2026-11-04 15:46:09 亚洲/上海（UTC 为 07:46:09） |
| 封卷时间 | 2026-11-07 17:17:56 亚洲/上海（UTC 为 09:17:56） |
| 卷标记创建 | 2026-11-04 16:10:26 亚洲/上海（`.swh-volume`） |
| 布局 | `repos/<user>/<repo>/<12位十六进制修订>.bundle`（偶发 `.git.tar` / `.dir.tar`） |
| 清单字段 | `repo_id`, `url`, `stars`, `swh_rev`, `bytes`, `sha256`, `artifact` |

| 项目 | 新 `repos/` | 旧 `github_downloads` |
| --- | --- | --- |
| 用户目录 | 42,611 | 8,532 |
| 仓库数 | 53,558 | 10,034 个 tar |
| 体积 | 427.48 GiB（清单=卷面声明） | 162.7 GiB（按文件 stat） |
| 主要格式 | `.bundle` 53,522；`.git.tar` 35；`.dir.tar` 1 | `<repo>_HEAD.tar.gz` |
| 空包 / 404 | 清单里 0 条 0 字节 | 742 个 404 占位（≤64 字节） |

每仓只有一份产物，没有多版本并存。`.bundle` 是 `# v2 git bundle`，里面是 git 历史，不是工作区快照。`.git.tar` 是打好包的 git 目录（顶层名形如 `swh:1:rev:完整sha.git/`）。唯一的 `.dir.tar` 是 gzip 压缩的目录快照（`swh:1:dir:.../`，kyutai-labs/pocket-tts），没有 git 历史。

明星数最小是 5，清单里没有 0 星仓，像是采集时按 stars ≥ 5 筛过。`repo_id` 从 2 到 57,145，区间内缺 3,586 个号，应该是没被写进封卷清单的失败下载。

## 2. 完整性

下面几项都是全量该侧的对照（MANIFEST 与磁盘存在性/大小、SHA256SUMS 与清单哈希一致、`repos/` 遍历、`.parts` 盘点）。文件内容哈希和 `git bundle verify` 是抽样，样本量和随机种子写在各行。

| 检查 | 范围 | 结果 |
| --- | --- | --- |
| 清单路径存在且大小一致 | 全量 53,558 | 按清单原路径：53,542 条完全匹配；16 条路径差异，见下 |
| 大小不一致 | 全量 | 0 |
| SHA256SUMS 与清单哈希/路径 | 全量 53,558 行 | 完全一致，无坏行、无互缺 |
| 磁盘上有、清单里没有 | 全量遍历 `repos/`（53,559 个文件） | 17 个。16 个是上述路径差异的另一写法；1 个真正多余 |
| sha256 实算文件 | 抽样 300，种子 20261110，按体积分层，强制纳入全部 36 个非 bundle | 300 / 300 通过 |
| git bundle verify | 抽样 120 个 bundle，种子 20261111 | 120 / 120 通过（见注） |
| swh_rev 是否在 bundle 里，是否等于 HEAD | 同上 120 个；内容抽样 399 个 git 产物 | 全部在，且都等于 bundle / 仓 HEAD |

**16 个「缺文件」其实都在。** 清单里仓名带末尾点（例如 `Askit.`、`W.A.Y.N.E.`、`Snake_Game.`），磁盘目录把末尾点掉了。这 16 个文件都能在掉点之后的路径下找到，大小与清单完全一致。`VOLUME.json` 的 path 是 `F:\`，很像是 Windows 落盘时掉掉了目录名末尾的点。转换脚本要做这 16 条路径映射，否则会误报缺文件。

真正多余的只有 1 个：`repos/asherzod2197/LIST_method8_0401/0c507379eca9.bundle`（2,096 字节），不在清单里，sha256 也对不上任何清单记录。这个用户名下还有 122 个很小的仓在清单里。

`git bundle verify` 第一次直接跑命令时，Git 2.43 全部报「need a repository to verify a bundle」——这是命令用法问题，不是包损坏。改成在一个临时空仓里再 verify 后，120 / 120 通过（耗时 0.4 秒）。同一批样本的 `git bundle list-heads` 里，`swh_rev` 全部出现且都等于 bundle 的 HEAD。内容抽样 400 仓里，399 个 git 产物也是 `swh_rev` = HEAD；剩下 1 个是 `.dir.tar`，没有 commit。

### `.parts` 是什么，能不能忽略？

`/mnt/data4/cleaning_data/.parts` 有 53,550 项：53,548 个目录（命名 `gh-<repo_id>-*`，里面全是 `repo.git/`，主要是 `objects/pack`），再加 2 个顶层 `*.bundle.part` 文件。目录里 53,523 个 `repo_id` 能对上清单，25 个对不上。对得上清单的目录里，46,953 个 pack 与清单体积相差在 1% 以内；6,569 个相差更大，但多数是小仓（bundle 比 pack 多一点头部，相对差超 1%）；1 个 pack 更大。0 个缺 pack。两个 `.bundle.part`（5052，94,970,185 字节；33199，338,863 字节）的 `repo_id` 都不在封卷清单里，头部仍是合法 git bundle，应该是没写进卷的失败下载。全部正规文件共 441.55 GiB，几乎又是一整份语料的剩余副本。对已封卷产物做转换和清洗时可以忽略 `.parts`；它只占磁盘。本次没有删任何东西。

## 3. 分布

以下全部来自 `MANIFEST.tsv` 全量 53,558 行，不是抽样。

### 产物体积

| 分位 | 大小 |
| --- | --- |
| 最小 | 327 B |
| 均值 | 8.17 MiB |
| p25 | 94.3 KiB |
| 中位 p50 | 857.3 KiB |
| p75 | 6.39 MiB |
| p90 | 25.60 MiB |
| p95 | 45.86 MiB |
| p99 | 82.04 MiB |
| p99.9 | 186.72 MiB |
| 最大 | 1.16 GiB（jhanliufu-personal/STEVO-Bench） |

### GitHub stars

| 分位 | stars |
| --- | --- |
| 最小 | 5 |
| 均值 | 96.8 |
| p25 | 6 |
| 中位 p50 | 10 |
| p75 | 23 |
| p90 | 70 |
| p95 | 159 |
| p99 | 913 |
| p99.9 | 9,359 |
| 最大 | 312,132（mattpocock/skills） |

### 仓体积分档

| 分档 | 仓数 | 占仓数 | 体积 |
| --- | ---: | ---: | ---: |
| <100 KiB | 13,747 | 25.67% | 0.44 GiB |
| 100 KiB–1 MiB | 14,260 | 26.63% | 5.55 GiB |
| 1–10 MiB | 14,953 | 27.92% | 56.00 GiB |
| 10–50 MiB | 8,269 | 15.44% | 189.62 GiB |
| 50–100 MiB | 2,159 | 4.03% | 144.02 GiB |
| 100–500 MiB | 162 | 0.30% | 26.65 GiB |
| 500 MiB–1 GiB | 7 | 0.01% | 4.03 GiB |
| >=1 GiB | 1 | — | 1.16 GiB |

仓数上大头在 10 MiB 以下（共 80.2%）；体积主要在 10–100 MiB 这两档（189.62 + 144.02 = 333.64 GiB，约占全卷 78%）。大于 500 MiB 的只有 8 个仓。

### stars 分档

| 分档 | 仓数 |
| --- | ---: |
| 1–9 | 25,902 |
| 10–99 | 23,678 |
| 100–999 | 3,476 |
| 1k–9.9k | 448 |
| 10k–49.9k | 43 |
| >=50k | 11 |

### 每用户仓数

37,245 个用户只有 1 个仓（87.4%）。中位是 1，p90 是 2，p99 是 5，最大 122（asherzod2197，122 仓共仅 329 KiB）。均值 1.26 仓/用户。

| 每用户仓数 | 用户数 |
| --- | ---: |
| 1 仓 | 37,245 |
| 2 仓 | 3,535 |
| 3–5 | 1,470 |
| 6–10 | 262 |
| 11–20 | 65 |
| 21–50 | 30 |
| >=51 | 4 |

### 体积最大的 20 个仓

| # | 仓库 | 体积 | stars |
| ---: | --- | ---: | ---: |
| 1 | jhanliufu-personal/STEVO-Bench | 1,189.4 MiB | 29 |
| 2 | timowhite88/Farnsworth | 691.5 MiB | 67 |
| 3 | nanxiaobei/ai-global | 645.0 MiB | 72 |
| 4 | MindIntLab-HFUT/CEDAR | 603.9 MiB | 7 |
| 5 | mmalmi/hashtree | 562.5 MiB | 12 |
| 6 | omg-cli/omg | 548.1 MiB | 6 |
| 7 | agentic-learning-ai-lab/temporal-straightening | 543.9 MiB | 131 |
| 8 | ernbilen/Data400_Spring26 | 536.1 MiB | 18 |
| 9 | OtowoOrg/Stellar-K8s | 461.4 MiB | 6 |
| 10 | Robdel12/OrbitDock | 421.8 MiB | 93 |
| 11 | zhangdailin/API-Console | 372.4 MiB | 49 |
| 12 | clemensv/stopwatch | 362.5 MiB | 17 |
| 13 | nzy1997/rust-qec | 361.5 MiB | 9 |
| 14 | berkeleydeeprlcourse/homework_spring2026 | 361.0 MiB | 164 |
| 15 | Motphys/UniLab | 337.3 MiB | 980 |
| 16 | varpulis/varpulis | 325.1 MiB | 7 |
| 17 | StarMoe-org/Moesekai | 324.5 MiB | 57 |
| 18 | elle-lisp/elle | 322.5 MiB | 26 |
| 19 | HKUDS/nanobot | 305.1 MiB | 49,817 |
| 20 | sipeed/picoclaw | 303.5 MiB | 30,091 |

### stars 最高的 10 个仓

| # | 仓库 | stars | 产物体积 |
| ---: | --- | ---: | ---: |
| 1 | mattpocock/skills | 312,132 | 5.1 MiB |
| 2 | affaan-m/ECC | 296,826 | 126.8 MiB |
| 3 | multica-ai/andrej-karpathy-skills | 229,292 | 25.2 MiB |
| 4 | rtk-ai/rtk | 85,523 | 151.9 MiB |
| 5 | colbymchenry/codegraph | 76,475 | 91.5 MiB |
| 6 | mvanhorn/last30days-skill | 66,551 | 66.5 MiB |
| 7 | Alishahryar1/free-claude-code | 66,285 | 39.3 MiB |
| 8 | blader/humanizer | 64,627 | 15.9 MiB |
| 9 | coreyhaines31/marketingskills | 58,415 | 18.6 MiB |
| 10 | VoltAgent/awesome-openclaw-skills | 53,539 | 3.8 MiB |

### 仓数最多的用户（前 8）

| 用户 | 仓数 | 总体积 |
| --- | ---: | ---: |
| asherzod2197 | 122 | 329 KiB |
| lobar2024 | 111 | 218 KiB |
| 0xparomita | 58 | 352 KiB |
| rmansurbek2197 | 52 | 112 KiB |
| microsoft | 50 | 575.4 MiB |
| sujon12210 | 50 | 309 KiB |
| shab92216 | 50 | 95 KiB |
| zainab1980 | 50 | 94 KiB |

### 体积最大的用户（前 8）

| 用户 | 仓数 | 总体积 |
| --- | ---: | ---: |
| jhanliufu-personal | 1 | 1.16 GiB |
| timowhite88 | 1 | 0.68 GiB |
| nanxiaobei | 2 | 0.63 GiB |
| MindIntLab-HFUT | 1 | 0.59 GiB |
| microsoft | 50 | 0.56 GiB |
| aws-samples | 40 | 0.56 GiB |
| mmalmi | 1 | 0.55 GiB |
| omg-cli | 1 | 0.54 GiB |

前几个「仓数之王」总体积极小，更像作业或模板批量仓，不是大项目仓。microsoft、aws-samples 则是另一类：仓多，体积也大。

## 4. 内容抽样

400 个仓，种子 20261112，按体积分层，并强制纳入全部 36 个非 bundle 产物。用 `git archive` 取 `swh_rev` 工作区，语言由 `dataflow:latest` 里的 enry-server 判定。下面数字除外推都是这 400 仓的实测。

**400 / 400 全部检出成功。** 没有空工作区。36 个 `.git.tar` / `.dir.tar` 也全部可读。有 commit 的 399 个 git 产物里，`swh_rev` 全部存在且都是 HEAD。

| 指标 | 数值 | 说明 |
| --- | --- | --- |
| 样本文件数 | 126,254 | 均值 315.6 个文件/仓 |
| 样本工作区 | 4.60 GiB | 产物 4.36 GiB，加权比 1.056 |
| 检出耗时中位 | 0.10 秒 | 均值 0.59 秒，最长 30.6 秒 |
| swh_rev 年份 | 395 / 399 为 2026 | 另 3 个 2025，1 个 2021 |

### swh_rev 提交年份（有 commit 的 399 仓）

| 年份 | 仓数 |
| --- | ---: |
| 2021 | 1 |
| 2025 | 3 |
| 2026 | 395 |

### 工作区 / 产物体积比

| 指标 | 数值 |
| --- | --- |
| 加权总和比（更可信） | 1.056 |
| 比值的中位 | 1.183 |
| 比值的均值 | 1.932 |
| p25 – p75 | 0.76 – 2.20 |

单仓比值均值偏高，是因小仓把比值拉起来。大 bundle 往往历史比工作区重得多（例如 Farnsworth：bundle 691.5 MiB，工作区只有约 33.2 MiB）。外推全量应用加权比 1.056。

### 语言混合（按文件数，前 12）

| 语言 | 文件数 |
| --- | ---: |
| JavaScript | 25,701 |
| TypeScript | 18,072 |
| unknown | 15,188 |
| Markdown | 13,132 |
| Python | 10,947 |
| JSON | 7,928 |
| C++ | 4,595 |
| Rust | 3,261 |
| YAML | 2,533 |
| Go | 2,374 |
| Svelte | 2,201 |
| Text | 2,080 |

按字节，unknown 反而最大（2.98 GiB，占样本工作区 4.60 GiB 的 60.5%），集中在几个带大二进制/数据文件的仓（如 utxo-z/utxo-z 244 MB、AnubhavChoudhery/EZ-Chess 237 MB、Graph-and-Geometric-Learning/HypRAG 212 MB）。这是原始工作区的 enry 结果，还没经过清洗管线的质量过滤。按文件数，unknown 占 12.0%（15,188 / 126,254）。

| 项目 | 样本实测 | 说明 |
| --- | --- | --- |
| 被 enry 判为 vendored | 45,713 个文件（36.21%）；760.7 MiB（15.39%） | 只按路径判定 |
| 含空字节的二进制文件 | 6,877（5.45%） | 读前 8 KiB |
| Git LFS 指针 | 5 个文件，分布在 2 个仓 | MJohnson459/costmap（3）、sxhmilyoyo/sundayhao-plugins（2） |
| 含 `.gitmodules` | 10 / 400 仓（2.5%） | 转换时工作区里只有模块记录，不会自动拉子模块内容 |
| 空工作区 | 0 / 400 | 清单也没有 0 字节产物 |

**外推到全量（已标明是样本外推）。** 按加权比 1.056 乘以全卷产物 427.48 GiB，估计全集工作区约 451.5 GiB。按均值 315.6 个文件/仓 乘以 53,558，估计约 1,690 万个文件。只算新增的 49,033 仓，工作区当量约 402.5 GiB。这些不是实测全量，长尾仓会影响精度。转成 `*_HEAD.tar.gz` 之后会更小，但本次没有实测 gzip 压缩率。

检出+语言统计耗时：400 仓共 236.1 秒；p50 = 0.101 秒，均值 0.588 秒，p90 = 1.28 秒，最大 30.6 秒（timowhite88/Farnsworth）。单线程按均值外推全量 53,558 仓约 8.7 小时；用中位会低估长尾。这段时间已包含 enry。纯转换可能略短。并行可以再压。

## 5. 与 github_downloads 的重叠

仓和用户交集是全量按 `user/repo` 对出来的（复用此前 `compare.json`）。内容对比是另一批 80 个重叠仓的抽样，种子 20261113。

按仓库划分：

| 集合 | 仓库数 |
| --- | ---: |
| 只在新批 | 49,033 |
| 两边都有 | 4,525 |
| 只在旧批 | 5,509 |

| 集合 | 仓库 | 用户 | 新批体积 |
| --- | --- | --- | --- |
| 两边都有 | 4,525 | 4,635 | 46.44 GiB |
| 只在新批 | 49,033 | 37,976 | 381.04 GiB |
| 只在旧批 | 5,509（其中 736 个仍是旧 404） | 3,897 | — |

旧批 742 个 404 占位里，只有 6 个在新批里找到了正经包：zqlit/Hugo-Theme-Amigo、qdleader/Awesome-AI-Pedia、psmarter/PMPP-Learning、Scottcjn/nvidia-power8-patches、Scottcjn/bottube、liu-ziting/ThinkFlowAI。

### 重叠仓工作区对比（抽样 80 个，非 404）

把新批 `swh_rev` 的 `git archive` 文件清单+内容 sha256，和旧批 `*_HEAD.tar.gz` 解包后的文件清单+内容 sha256 对照（去掉旧包顶层 `<repo>-HEAD/` 前缀）。这 80 个都成功比对，没有因 404 或缺旧包被跳过。

| 对照结果 | 仓数 |
| --- | ---: |
| 完全相同 | 70 |
| 有差异 | 10 |

70 个文件树完全一致（87.5%）。0 个是严格的新包超集或旧包超集。10 个有差异，它们的 `swh_rev` 提交时间都在 2026 年 10 月底到 11 月初。其中几个明显像更新过（新快照多了文件，还有 CHANGELOG），例如 artifact-keeper/artifact-keeper（2,209 vs 1,721 个文件）、sbom-tool/sbom-tools（+312 个新文件，旧文件全部还在）、taaderbe/linuxgamebench、microclaw/microclaw。另几个只改了很少文件（knobik/sql-agent 只改了 `composer.json`，blader/humanizer 只改了插件元数据）。旧包没有 git 历史，不能从提交图上证明「一定更新」，只能说内容不一致。这一批里没有「旧快照反而更完整」的例子。

## 6. 可能的问题

| 问题 | 观测 | 对清洗的影响 |
| --- | --- | --- |
| 同名仓 / 像 fork | 1,135 个区分大小写的仓名被多个用户用过，多出 2,163 个仓；不分大小写则 1,390 个名。skills 一名就有 244 个用户（不分大小写 247），agent-skills 105 / 107。相同 swh_rev 的有 6 组（真正内容重复）；完全相同 sha256 的 1 组（同一用户下两个仓名）。 | 文件级去重会把很多同名/近似仓并掉，和此前 enry 全量跑里 microsoft/skills 被并进 agent-skills 是同一类情况。清单里没有 is_fork 字段。 |
| 超大仓 | 8 个产物 ≥500 MiB，最大 1.16 GiB。工作区不一定同等大。 | 转换要给足够超时和临时空间；去重负担也更重。 |
| 非 bundle 产物 | 35 个 `.git.tar`、1 个 `.dir.tar`。抽样里都能检出。 | 转换器要单独处理这两种格式，不能只跑 git bundle。 |
| 仓名末尾点 | 16 个清单路径与磁盘不一致 | 转换必须做路径映射，否则会丢这 16 仓。 |
| LFS 指针 | 抽样 2 / 400 仓，共 5 个指针文件 | bundle 里一般只有指针，不会自动取真正大文件 |
| 子模块 | 抽样 10 / 400 仓含 `.gitmodules` | 快照里只会留下模块元信息 |
| 404 / 空仓 | 新批清单无 0 字节行；抽样无空工作区 | 新批比旧批清净。旧 404 里只救回 6 个 |
| 小仓集中的用户 | 单用户上百个极小仓 | 去重后可能很快被并掉或清空 |
| `.parts` 占盘 | 441.55 GiB 剩余 | 不影响处理；若要腾空间，验证完整性后再决定是否删 |

## 7. 建议（仅建议，未开跑）

**现有管线不能直接吃。** `discover_tarballs` 只找 `input_dir/*/*.tar.gz`，`unpack.py` 按「用户目录下的 `<repo>_HEAD.tar.gz` 快照」解包。新数据是更深一层的 git bundle。只改配置里的 `input_dir` 不够，要先转成工作区快照，不要把整段 git 历史当源码喂进去。

1. **写转换器：bundle / `.git.tar` / `.dir.tar` → `用户/仓名_HEAD.tar.gz`。** 检出 `swh_rev`（抽样里它就是 HEAD）。输出放到新目录，不要覆盖 `github_downloads`。必须处理 16 条末尾点路径，以及 35+1 个非 bundle 产物。
2. **优先只跑新增的 49,033 仓（可以加上旧 404 被救回的 6 个）。** 结果放进新的 run 目录，例如 `full-enry-new-YYYYMMDD`。再与 `full-enry-20261105` 做文件级去重合并，不必重跑旧仓。
3. **重叠的 4,525 仓：先不必全量刷新。** 80 仓抽样里 70 个工作区与旧 HEAD 快照完全相同。若要刷，可以只针对有差异的子集再扩大抽查，而不是一开始就把 4,525 个都重洗。

| 预估项 | 数字 | 依据 |
| --- | --- | --- |
| 转换后工作区体积（全量） | 约 451.5 GiB | 抽样 400 仓的加权比 1.056 × 427.48 GiB；这是未压缩工作区，tar.gz 会更小，本次未测压缩率 |
| 只转新增 49,033 仓 | 约 402.5 GiB 工作区当量 | 同比 × 381.04 GiB 新增产物 |
| 转换耗时（单线程） | 约 8.7 小时（按均值） | 0.588 秒/仓 × 53,558；含本次 enry 统计，纯转换可能略短。建议按 4–12 小时规划，并行可压短 |
| 清洗只跑新增 | 约 16–25 小时 | 按仓数约为 full-enry-20261105（3 小时 14 分 / 9,110 有内容仓）的 5.3 倍，再留一点给更重的去重 |
| 磁盘预留 | 至少再准备约 450 GiB 可用空间 | 若与现有 bundle（427 GiB）和 `.parts`（442 GiB）并存，不要靠删旧数据来腾地盘 |

不建议把新旧两批合并后再全量重跑一遍。以上只是建议，还没有启动任何转换或清洗任务。

## 检查方法与留档

| 检查 | 状态 | 范围 / 种子 | 完成时间 |
| --- | --- | --- | --- |
| MANIFEST 全量分布、产物存在性与大小、SHA256SUMS 与清单哈希、`repos/` 遍历、`.parts` 盘点 | 已完成 | 全量 | 2026-11-10 17:01 亚洲/上海 |
| sha256 实算 300 个文件（含全部 36 个非 bundle） | 已完成 | 种子 20261110，按体积分层 | — |
| git bundle verify 120 个 + list-heads（swh_rev / HEAD） | 已完成 | 种子 20261111；verify 在临时空仓里重跑 | — |
| 检出 + enry 语言统计 400 仓 | 已完成 | 种子 20261112 | — |
| 重叠 80 仓的工作区对比 | 已完成 | 种子 20261113 | — |
| 与 `github_downloads` 的仓/用户交并集 | 已完成 | 全量，复用 `compare.json` | — |

源数据只读。脚本和结果在 `/mnt/data4/cleaning_data/dataflow_runs/full-enry-20261105-acceptance/new-data/`（`distributions.json`、`integrity_partial.json`、`parts_inventory.json`、`sha256_sample.json`、`bundle_verify_sample.json` 与 `bundle_verify_sample_redo.json`、`content_sample.json`、`overlap_content_sample.json`）。扫描本身没有改权限，也没有启动清洗任务。

---

报告统计时点：2026-11-10 16:53–17:09 亚洲/上海。所有数字均来自当次读盘或已标注范围的抽样，没有撒数字。
