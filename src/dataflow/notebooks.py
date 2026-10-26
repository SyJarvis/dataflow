"""Step 5 - Jupyter notebook cleaning.

Stack v3 official behaviour:

    .ipynb files are processed separately: cell outputs, inline images, and
    volatile metadata are stripped, and the notebook is kept as structured
    JSON. They pass through the same license, dedup, and PII steps.
"""

from __future__ import annotations

import json

from .models import FileRecord, StageStats

_VOLATILE_CELL_KEYS = {"execution_count", "outputs", "id"}
_VOLATILE_NB_KEYS = {
    "kernel_spec", "language_info", "nbformat", "nbformat_minor",
}
_VOLATILE_META_KEYS = {"collapsed", "scrolled", "deletable", "editable", "raw_mimetype"}


def clean_notebook(text: str, cfg: dict) -> str | None:
    """Strip outputs / images / volatile metadata; return serialized JSON.

    Returns None if the notebook is not parseable (it will be dropped).
    """
    try:
        nb = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(nb, dict) or nb.get("cells") is None:
        return None

    strip_outputs = cfg.get("strip_outputs", True)
    strip_images = cfg.get("strip_images", True)

    for cell in nb["cells"]:
        if not isinstance(cell, dict):
            continue
        if strip_outputs and cell.get("cell_type") == "code":
            cell.pop("outputs", None)
            cell.pop("execution_count", None)
        if strip_images:
            for key in ("attachments", "media"):
                cell.pop(key, None)
        meta = cell.get("metadata")
        if isinstance(meta, dict):
            for k in _VOLATILE_META_KEYS:
                meta.pop(k, None)

    if cfg.get("stripvolatile_metadata", True):
        meta = nb.get("metadata")
        if isinstance(meta, dict):
            for k in _VOLATILE_NB_KEYS:
                meta.pop(k, None)

    try:
        return json.dumps(nb, ensure_ascii=False)
    except (TypeError, ValueError):
        return None


def process_notebooks(records: list[FileRecord], cfg: dict) -> tuple[list[FileRecord], StageStats]:
    stats = StageStats(stage="notebooks")
    kept: list[FileRecord] = []
    for rec in records:
        stats.seen += 1
        if rec.language == "Jupyter Notebook" or rec.file_path.endswith(".ipynb"):
            cleaned = clean_notebook(rec.content, cfg)
            if cleaned is None:
                stats.drop("unparseable_notebook")
                continue
            rec.content = cleaned
        kept.append(rec)
        stats.kept += 1
    return kept, stats
