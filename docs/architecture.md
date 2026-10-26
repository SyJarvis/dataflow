# dataflow 架构与实现

> 对应版本：commit `eda2a27`（并行化 + activity 标注）
> 设计目标：按 The Stack v3 官方清洗口径，把 GitHub 仓库 HEAD 快照 tarball
> （`github_downloads` 形态：每用户一个目录，内含 `<repo>_HEAD.tar.gz`）
> 清洗为 repo-context 训练语料。规模目标 ~10k tarball / ~300 万文件，纯 CPU，
> 可断点续跑，产物落 `/mnt/data2`。

---

## 1. 总体架构：并行清洗 + 两遍全局去重

单机内存放不下全量文件内容（~300 万文件 × 平均 14KB ≈ 40GB 语料），
所以架构上把**清洗（可并行、无全局状态）**和**去重（全局状态、单进程）**
拆开，中间用落盘的中间产物衔接：

```
                       configs/*.yaml
                             │
             ┌───────────────┴───────────────┐
             │   runner.run_pipeline         │
             │   discover_tarballs (glob)    │
             │   manifest resume 过滤         │
             └───────────────┬───────────────┘
                             │  todo = 未处理的 tar
        ═════════════ 阶段 A：并行清洗（Pool, N workers）═════════════
                             │
             fork worker × N（默认 16，每 worker 处理一个 tar）
             unpack → languages → [licenses] → quality → notebooks
             → activity 标注 → MinHash 签名
                             │
             result = {_tar, repo_path, records[], sigs[],
                       stats[], activity, evidence, dup_within}
                             │  imap_unordered (chunksize=4)
             ┌───────────────┴───────────────┐
             │ 主进程逐 result：              │
             │  records 追加 → intermediate/repos.ndjson
             │  sigs   追加 → intermediate/signatures.ndjson
             │  manifest 每 50 tar 落盘       │
             └───────────────┬───────────────┘
        ═══════ 阶段 B：全局近重去重（主进程，单线程）═══════
                             │
             逐条读 signatures.ndjson →
             MinHash(num_perm=256, scheme=affine32) 重建 →
             MinHashLSH(threshold=0.7) 增量 insert + query →
             候选对 Jaccard 复核 → UnionFind 连通分量聚类 →
             每簇保留一个代表（first-seen）→ drop_sha 集合
                             │
        ═══════ 阶段 C：过滤 + 分组输出（流式）═══════
                             │
             逐行流式读 repos.ndjson →
             过滤 drop_sha（近重）+ seen_sha（精确副本只留第一份）→
             RepoRecord（含 last_activity_year）→ repos.jsonl
                             │
                       report.json
```

**为什么两遍**：MinHash-LSH 的聚类需要看到全部签名才能定簇，而全量文件
内容放不进内存。签名只占 1KB/文件（256 × uint32），300 万文件 ≈ 3GB，
主进程可全部持有；文件内容则始终只存在于 worker 内存和磁盘中间产物里。

**datasketch 2.x 兼容点**：从序列化 hashvalues 重建 MinHash 必须显式
`scheme="affine32"` 且按 **uint32** 解码（默认创建的 MinHash 即 affine32，
digest 256 perm = 1024 字节）。踩过 uint64 的坑，已写进单测。

## 2. 去重语义（容易搞错的三层）

| 层 | 判定 | 处置 | 统计口径 |
|---|---|---|---|
| 精确重复（tar 内） | 同一 tar 内相同 sha1 | worker 跳过重复签名，计数 `dup_within` 回传 | `exact_duplicate` |
| 精确重复（跨 tar） | 不同 tar 出现相同 sha1 | pass B 跳过重复插入；pass C 的 `seen_sha` 只放行第一份 | `exact_duplicate` |
| 近重复 | LSH 候选 + 真实 Jaccard ≥ 0.7，union-find 成簇 | 簇内保留一个代表（当前 first-seen），其余进 `drop_sha` | `near_duplicate` |

注意：**< 5 token 的文件不生成签名**（官方规则），它们不参与近重去重，
但精确去重（sha1）仍然生效。这就是为什么 `dedup.seen` 会略小于
`quality.kept`、`files_out` 会略大于 `dedup.kept`——数字自洽公式：

```
files_out = 唯一 sha1 数 − near_duplicate 数
dedup.seen = 唯一 sha1 数 + exact_duplicate 数（tar 内 + 跨 tar）
```

官方代表选择优先级是 stars → forks → permissive → 最早创建；
当前无 stars 元数据，退化为 first-seen（即 intermediate 里的出现顺序）。
等采集侧注入 `github_metadata` 后在阶段 B 的代表选择处替换即可。

## 3. 仓库布局

```
dataflow/
  configs/
    default.yaml      # 官方阈值全集 + pilot10 运行配置
    full.yaml         # 全量运行（output_dir → /mnt/data2，workers 16）
  src/dataflow/
    runner.py         # 编排：并行池 / 两遍去重 / resume / report
    unpack.py         # ① 解包 + 下载期丢弃（max_file_bytes / binary / symlink
                      #    / max_repo_files——注意最后这条只在串行函数里，
                      #    并行 worker 未接线，见 alignment 文档缺口表）
    languages.py      # ② 解码(UTF-8→cchardet) + 扩展名/shebang 语言检测
                      #    + vendor 启发式；留 go_enry_bin 接入位
    licenses.py       # ③ 正则版 SPDX 检测 + 目录树传播 + permissive 分类
                      #    （默认关闭，ScanCode hook 预留）
    quality.py        # ④ StarCoder2 12 条启发式（文件级）
    notebooks.py      # ⑤ .ipynb 剥 outputs/images/volatile metadata
    pii.py            # ⑥ 正则兜底版（默认关闭）；StarPII NER 类已写好，
                      #    装 pii extra 后自动启用
    dedup.py          # ⑦ MinHash 签名 + LSH + UnionFind（串行版完整实现，
                      #    并行 runner 复用其 minhash_signature/UnionFind）
    group.py          # ⑧ 分组工具（串行版遗留；分组逻辑已内联进 runner
                      #    pass C，write_parquet 是可复用的导出工具）
    activity.py       # ➕ 内容证据年份标注（官方没有的扩展）
    models.py         # FileRecord / RepoRecord / StageStats
    cli.py            # run / report 两个子命令
  tests/              # 11 个单测（管线各步 + datasketch roundtrip）
  docs/
    outputs.md                    # 产物 schema + 官方字段对照
    architecture.md               # 本文
    stack-v3-alignment.md         # 官方步骤 × 实现状态对比
    code-cleaning-docs/           # 方法论存档（Stack v3 / Nemotron / StarPII）
```

## 4. Run 产物布局

```
runs/<name>/                    # 全量运行指向
  manifest.json                 #   /mnt/data2/.../dataflow_runs/<name>
    fingerprint                 # config 全文哈希；resume 时校验，变了拒绝续跑
    processed[]                 # 已完成 tar 路径
    failed{}                    # tar 路径 → 错误信息（坏 tar 只失败一次）
    dup_within_total            # tar 内精确重复累计（resume 不丢统计）
  intermediate/
    repos.ndjson                # 每行 = 一个仓库的完整清洗结果（含文件内容，
                                # 全量 ~40GB；阶段 A 追加写）
    signatures.ndjson           # 每行 = {"sha1","sig":base64(256×uint32)}
                                # 全量 ~3GB
  repos.jsonl                   # 最终产物：一行一仓库（schema 见 outputs.md）
  report.json                   # 阶段统计 + activity 年份直方图
```

**resume 语义**：阶段 A 幂等（manifest 过滤 todo），阶段 B/C 每次全量
重算（读全部中间产物）。已知口径限制：`--resume` 后 report 的
`stages` 只含新处理 tar 的统计（pass C 的输出统计则是全局准确的），
manifest 里已注释。中断安全点 = manifest 落盘粒度（每 50 tar），
最坏重复清洗 50 个 tar。

## 5. 配置系统

`default.yaml` 一个文件装下：官方阈值（全部带出处注释）、步骤开关
（`run.steps.*`）、运行参数（`workers / max_tarballs / output_dir / name`）。
`full.yaml` 由 sed 派生，只改运行参数不动阈值——**保证全量和 pilot 的
清洗口径永远一致**，差异只在 manifest fingerprint 里可追溯。

## 6. 性能特征（实测）

| 规模 | 配置 | 结果 |
|---|---|---|
| pilot10（10 tar，2909 文件） | 8 workers | 77s；与串行版 files_out 完全一致（2581） |
| 全量启动段（2050/10031 tar） | 16 workers | ~3 tar/s 清洗阶段；瓶颈 = MinHash 计算（CPU 密集）+ NFS 读 |

全量估算：清洗 ~1h（16 workers），阶段 B LSH 300 万签名单线程 ~2-4h（已知瓶颈，
如需加速可分段并行 insert 或换 datasketch GPU 后端），阶段 C 流式 ~10min。
中间产物峰值占用 ~43GB（data2）。

## 7. 已知限制与不去修的理由

- **语言检测未识别率 ~7%**（扩展名表兜底）：接 go-enry 二进制即可，参数位已留
- **stats 的 resume 口径**：重跑 report 阶段数字不回填，看 pass C 输出统计
- **LSH 阶段单线程**：300 万签名可接受；若数据量 ×10 再考虑并行化
- 其余差距见 `stack-v3-alignment.md` 缺口表
