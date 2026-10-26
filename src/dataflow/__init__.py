"""dataflow - Stack v3 style source-code cleaning pipeline.

Stages (see configs/default.yaml for every official threshold):

  1 unpack     tar snapshot -> files, with download-time drop rules
  2 languages  UTF-8 decode, go-enry detection, SHA-1 content addressing
  3 licenses   ScanCode + directory-tree propagation, non_permissive dropped
  4 quality    StarCoder2 heuristic filters (12 rules)
  5 notebooks  strip outputs/images/volatile metadata from .ipynb
  6 pii        StarPII redaction (optional)
  7 dedup      MinHash(256x5gram) LSH >=0.7 + connected components (optional)
  8 group      repository-grouped JSONL/Parquet export
"""

__version__ = "0.1.0"
