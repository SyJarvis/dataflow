# dataflow 架构与实现

> 对应版本：本地 `main`（含 `747bad0` 2026-11-11 清洗规则、`3d01d14` enry-server、`dea679d` bundle_to_tar）。
> 设计目标：按 The Stack v3 官方清洗口径，把 GitHub 仓库 HEAD 快照 tarball
> （`github_downloads` 形态：每用户一个目录，内含 `<repo>_HEAD.tar.gz`）
> 清洗为 repo-context 训练语料。可断点续跑，产物写到配置中的 `output_dir/<name>/`。

---

## 1. 总体架构：并行清洗 + 两遍全局去重

单机内存放不下全量文件内容，所以架构上把**清洗（可并行、无全局状态）**和
**去重与配额（全局状态、单进程汇总）**拆开，中间用落盘的 shard 和派生的
中间产物衔接：

```
                       configs/*.yaml
                             │
             ┌───────────────┴───────────────┐
             │   execution.run_pipeline      │
             │   发现输入 tar（或读校验清单）  │
             │   已有 shard + manifest 过滤    │
             └───────────────┬───────────────┘
                             │  todo = 尚无合法 shard 的 tar
        ═════════════ 阶段 A：并行清洗（Pool, N workers）═════════════
                             │
             fork worker × N（每 worker 处理一个 tar）
             unpack → languages → [licenses] → notebooks
             → quality → activity 标注 → MinHash 签名
                             │
             每个输入原子写入 intermediate/shards/<shard>
             manifest 每 50 个新 tar 更新一次（shard 才是主记录，
             即使 manifest 尚未刷新，已写完的 shard 也不会丢）
                             │
        ═══════ 阶段 B：全局近重去重（主进程）═══════
                             │
             由 shard 汇总 repos.ndjson / signatures.ndjson / index.ndjson
             MinHash(num_perm=256, scheme=affine32) 重建 →
             MinHashLSH(threshold=0.7) 增量 insert + query →
             候选对按真实 5-gram shingle Jaccard 复核
             （`dedup.verify_method: exact`；可改 `estimate`）→
             UnionFind 连通分量聚类 →
             每簇保留一个代表（stars → forks → 宽松许可证 →
             最早创建时间 → 路径排序 + sha1）→ drop_sha 集合
                             │
        ═══════ 阶段 C：过滤 + 配额 + 分组输出（流式）═══════
                             │
             逐行流式读中间结果 →
             过滤近重 drop_sha + 精确副本（同一顺序选代表）→
             先对保留字节 >1.5 GiB 的仓库按 50% 降采样，
             再按语言做 XML/HTML/JSON/JavaScript 字节预算 →
             RepoRecord（含 last_activity_year，以及能匹配到的
             github_metadata）→ repos.jsonl
                             │
                       report.json
```

**为什么两遍**：MinHash-LSH 的聚类需要看到全部签名才能定簇，而全量文件
内容放不进内存。签名只占 1KB/文件（256 × uint32）；文件内容则始终只存在于
worker 内存、每输入一份的 shard，以及派生的中间产物里。

**datasketch 2.x 兼容点**：从序列化 hashvalues 重建 MinHash 必须显式
`scheme="affine32"` 且按 **uint32** 解码（默认创建的 MinHash 即 affine32，
digest 256 perm = 1024 字节）。踩过 uint64 的坑，已写进单测。`pyproject.toml`
把 datasketch 钉在 `>=2.0.0,<3`。

**语言检测**：配置 `languages.go_enry_bin` 后走 go-enry。若存在可执行的
`<go_enry_bin>-server`（或环境变量 `DATAFLOW_ENRY_SERVER`），每个 worker
复用一个长驻进程，结果与 `enry -json` 单文件分析等价；否则逐文件起 CLI。
失败则降级到内置扩展名+shebang 表。Docker 镜像同时提供 `/usr/local/bin/enry`
与 `/usr/local/bin/enry-server`。

## 2. 去重语义（容易搞错的三层）

| 层 | 判定 | 处置 | 统计口径 |
|---|---|---|---|
| 精确重复（tar 内） | 同一 tar 内相同 sha1 | worker 跳过重复签名，计数 `dup_within` 回传 | `exact_duplicate` |
| 精确重复（跨 tar） | 不同 tar 出现相同 sha1 | 按代表优先级只留一份（不再是 first-seen） | `exact_duplicate` |
| 近重复 | LSH 候选 + 真实 Jaccard ≥ 0.7，union-find 成簇 | 簇内按同一优先级留一个代表，其余进 `drop_sha` | `near_duplicate` |

代表选择顺序（与 Stack v3 一致，实现于 `dedup.repo_priority_key`，当前是代码写死的顺序，不是动态读取 `dedup.representative_order`）：最高 stars、最高 forks、宽松许可证、最早 `created_at`，缺失字段排最后，再平手则按输入路径排序 + sha1。元数据来自 `dedup.repo_metadata_path`（TSV/JSONL）。精确去重的保留副本也按同一顺序选择。

注意：**< 5 token 的文件不生成签名**（官方规则），它们不参与近重去重，
但精确去重（sha1）仍然生效。这就是为什么 `dedup.seen` 会略小于
`quality.kept`、`files_out` 还会再被预算阶段扣减——数字自洽要连 `stages.budget` 一起看。

去重之后（`run.steps.budget`，缺省为开）：

- 仓库保留字节 > `quality.max_repo_bytes_before_downsample`（默认 1,610,612,736，即 1.5 GiB）时，每个文件按 `blake2b(sampling_seed, "downsample", sha1)` 以 `downsample_keep_ratio`（默认 0.5）决定去留，原因 `repo_downsample`。
- 然后按 `quality.byte_budget`（默认 XML/HTML/JSON/JavaScript 各 2e9 字节）对每种语言依 `blake2b(sampling_seed, "budget", sha1)` 排序累加，超出上限的文件丢弃，原因 `byte_budget`。种子默认 `quality.sampling_seed: 20261111`，同样输入结果可复现。

## 3. 仓库布局

```
dataflow/
  configs/
    default.yaml                  # 官方阈值 + 2026-11-11 规则，pilot10
    full-rules-20261111.yaml      # 同口径全量
    pilot-rules-20261111.yaml     # 同口径试跑
    pilot-repos-20261117.yaml     # repos/ 批次 300 仓库试跑
    full-enry.yaml / pilot-enry.yaml  # 2026-11-05 go-enry 对照
    full.yaml                     # 较早的全量配置
  src/dataflow/
    runner.py         # worker：清洗单个 tar；run_pipeline 转给 execution
    execution.py      # 编排：shard 断点 / 两遍去重 / 预算 / resume / report
    unpack.py         # ① 解包 + 下载期丢弃（max_file_bytes / binary / symlink
                      #    / max_repo_files；并行 worker 已接线）
    languages.py      # ② 解码(UTF-8→cchardet) + go-enry / enry-server
                      #    或内置扩展名/shebang 回退 + vendor 检测
    licenses.py       # ③ 正则版 SPDX 检测 + 目录树传播 + permissive 分类
                      #    （默认关闭，ScanCode hook 预留）
    notebooks.py      # ④ .ipynb 剥 outputs/images/volatile metadata
                      #    （在质量过滤之前执行，输出 indent=1）
    quality.py        # ⑤ StarCoder2 启发式（Unicode 字母占比等，文件级）
    pii.py            # ⑥ 正则/StarPII 实现仍在；当前 execution 路径若
                      #    steps.pii=true 会直接报错拒绝
    dedup.py          # ⑦ MinHash 签名 + LSH + UnionFind + 代表选择
    budget.py         # ⑦b 语言字节预算 + 大仓降采样（去重之后）
    group.py          # ⑧ 分组工具（串行版遗留；分组逻辑已内联进 execution
                      #    输出阶段，write_parquet 是可复用的导出工具）
    activity.py       # 内容证据年份标注（只写 last_activity_year，不作过滤）
    inventory.py      # validate-input 用的只读 tar 校验
    storage.py        # 原子写、run lock
    models.py         # FileRecord / RepoRecord / StageStats
    cli.py            # run / report / validate-input
  scripts/
    bundle_to_tar.py          # repos/ 卷（bundle/git.tar/dir.tar）→ github_downloads 布局
    build_repo_metadata.py    # 生成 dedup.repo_metadata_path
    build_dataset.py          # 下游统计/展开/切分；已去掉 --years 过滤
  tools/enry-server/          # 长驻 go-enry 服务
  tests/                      # 管线步骤、2026-11-11 规则、enry、bundle_to_tar 等
  docs/
    architecture.md                         # 本文
    CLEANING-RULES-VS-STACKV3-20261111.md   # 规则对照
    DATA-github_downloads-20261117.md       # github_downloads 数据说明
    FUNNEL-full-rules-20261111.md           # 全量清洗漏斗
    REPOS-SCAN-20261110.md                  # repos / swhkit 卷扫描
    PILOT-repos-20261117.md                 # 转换与 300 仓库试跑
    outputs.md                              # 产物 schema + 官方字段对照
    stack-v3-alignment.md                   # 官方步骤 × 实现状态对比
    BUILD_DATASET_GUIDE.md
    code-cleaning-docs/                     # 方法论存档（Stack v3 / Nemotron / StarPII）
```

仓库名以 `.` 结尾时，`scripts/bundle_to_tar.py` 当前会失败（例如 `Snake_Game.`），尚未修复。

## 4. Run 产物布局

```
<output_dir>/<name>/
  manifest.json
    schema_version / state
    fingerprint / fingerprints     # config、代码，以及可选的仓库元数据哈希
    processed[] / failed{}         # 已完成或失败的 tar
    dropped_repos{}                # 整仓丢弃原因
    dup_within_total
    stages                         # 阶段 A 的累计统计
  intermediate/
    shards/                        # 每个输入一份原子结果，resume 主记录
    repos.ndjson                   # 由 shard 派生的去重前仓库结果
    signatures.ndjson              # {"sha1","sig":base64(256×uint32)}
    index.ndjson                   # 去重/预算用的轻量索引
  repos.jsonl                      # 最终产物：一行一仓库（schema 见 outputs.md）
  report.json                      # 阶段统计（含 budget）+ activity_year_hist
                                   # 不再输出 repos_active_2026
```

**resume 语义**：阶段 A 以 shard 为准（指纹一致即可跳过），manifest 是派生账本，每 50 个新 tar 以及阶段切换时更新。阶段 B/C 每次全量重算（读全部中间产物）。已知口径限制：`--resume` 后 report 的部分阶段数字以 manifest 累计为准，看 pass C 的输出统计则是全局准确的。中断安全点 = 单个 shard 的原子改名，不会因为 manifest 尚未刷新而丢掉已完成的 tar。旧的纯追加式中间产物不能用这套实现续跑。

当前执行路径若 `run.steps.pii=true` 会直接报错退出。许可证过滤仍可开，但默认关闭。

## 5. 配置系统

`default.yaml` 一文件装下：官方阈值（全部带出处注释）、2026-11-11 规则
（`run.steps.budget`、`quality.byte_budget`、`verify_method: exact` 等）、
步骤开关（`run.steps.*`）、运行参数（`workers / max_tarballs / output_dir / name`）。
`full-rules-20261111.yaml` / `pilot-rules-20261111.yaml` 与之同一套清洗口径，
只改运行参数。更早的 `full.yaml` 等配置仍在仓库里，不应当作当前规则的等同物。

## 6. 性能特征（实测）

| 规模 | 配置 | 结果 |
|---|---|---|
| 较早的 pilot10（10 tar，2909 文件） | 8 workers | 77s；当时与串行版 files_out 完全一致（2581）。这是并行化阶段的对照，不是 2026-11-11 规则的数字 |
| 2026-11-11 规则 300 包试跑 | 见 `docs/CLEANING-RULES-VS-STACKV3-20261111.md` | 409 s（对照旧口径 368 s，约 +11%） |

全量 `full-rules-20261111` 的漏斗与耗时见 `docs/FUNNEL-full-rules-20261111.md` 和 `docs/PILOT-repos-20261117.md`（后者记录处理 174.7 GB 压缩包用了 4 小时 15 分）。阶段 B 的 LSH 与真实 Jaccard 复核仍是单进程汇总，数据量再升高时需要另行评估。

## 7. 已知限制与不去修的理由

- **语言检测**：go-enry CLI 与 enry-server 均已接入；未配置 `go_enry_bin` 时仍走内置扩展名表，未识别率会更高。不要再用旧的“约 7%、尚未接线”说法。
- **代表选择的元数据覆盖不全**：选择逻辑已实现，但数据源常只有部分仓库的 stars，forks / 创建时间 / 许可证经常缺失，缺项会退化为路径 + sha1。
- **配置项 `dedup.representative_order` 目前只是注释性字段**，代码并不读取它；实际顺序写在 `dedup.repo_priority_key`。
- **stats 的 resume 口径**：重跑 report 的部分阶段数字不回填，看最终输出统计。
- **LSH 与真实 Jaccard 复核在主进程汇总**：当前规模可接受；若数据量再高一个数量级再考虑并行化。
- **`bundle_to_tar.py` 在仓库名以 `.` 结尾时会失败**，尚未修复。
- 其余历史差距见 `stack-v3-alignment.md`（其中部分条目可能尚未同步到 2026-11-11 规则，以代码和本文为准）。
