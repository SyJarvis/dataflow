# dataflow

Stack v3 风格的源代码清洗管线，输入 GitHub 快照 tarball，输出仓库分组的训练语料。
方法论与全部阈值来自 [The Stack v3 官方 dataset card](https://huggingface.co/datasets/HuggingFaceCode/stack-v3-train)。

## 管线

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

## 快速开始

```bash
cd ~/dataflow
python3 -m venv .venv && . .venv/bin/activate
pip install -e . -i https://pypi.tuna.tsinghua.edu.cn/simple
pip install -e ".[dedup]" -i https://pypi.tuna.tsinghua.edu.cn/simple   # 可选: 近重去重

# 小规模试跑（configs 里 max_tarballs 限 3 个 tar）
PYTHONPATH=src python3 -m dataflow run configs/default.yaml

# 断点恢复（config 未变时跳过已处理 tar）
PYTHONPATH=src python3 -m dataflow run configs/default.yaml --resume

# 查看报告
PYTHONPATH=src python3 -m dataflow report runs/demo/report.json
```

产物目录：

```
runs/<name>/
  manifest.json   # config 指纹 + 已处理 tar 清单（resume 依据）
  repos.jsonl     # 每行一个仓库，files[] 含清洗后内容
  report.json     # 各阶段 seen/kept/dropped 与原因分布
```

## 当前实现状态

| 步骤 | 状态 | 说明 |
|---|---|---|
| ①②④⑤ | ✅ 完整 | 纯 Python，无重依赖 |
| ③ | ✅ 正则版 | ScanCode 接入点已留（`licenses.annotate_repo_licenses`），白名单=BlueOak+常见宽松license |
| ⑦ | ✅ 降级可用 | 装 `datasketch` 后为完整 MinHash LSH；不装则仅精确去重 |
| ② 语言检测 | ⚠️ fallback | 内置扩展名+shebang 表（~60语言）；接 go-enry CLI 后替换（`languages.go_enry_bin`） |
| ⑥ | ⚠️ 正则兜底 | 需 transformers+StarPII 才启用模型脱敏 |
| ⑧ Parquet | 可选 | 装 `pyarrow` |

## 路线图

- [ ] 接入 go-enry CLI（语言检测精度对齐 Linguist）
- [ ] 接入 ScanCode Toolkit（SPDX 全量识别）
- [ ] github_metadata（stars/forks）注入 → 代表选择完整实现
- [ ] 增量模式：新 tar 入库前先对已有内容库做 SHA-1 + MinHash 比对
- [ ] repo >1.5GB 概率降采样、XML/HTML/JSON/JS 字节预算（需全语料统计后生效）
