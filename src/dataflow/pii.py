"""Step 6 - PII redaction (StarPII).

Stack v3 official behaviour:

    File contents are scanned with StarPII. Detected emails, keys, names,
    passwords, and IP addresses are replaced with placeholder tokens
    (<EMAIL>, <KEY>, <NAME>, <PASSWORD>, and synthetic private IPs).

This stage needs the optional `pii` extra (transformers + torch). When the
model is unavailable we apply a cheap regex pass for emails / keys / IPs
(names and passwords need the model), and mark the run report accordingly.
"""

from __future__ import annotations

import re

from .models import FileRecord, StageStats

try:
    from transformers import pipeline  # type: ignore
    _HAS_HF = True
except ImportError:  # pragma: no cover
    _HAS_HF = False

_EMAIL = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
_SECRET_KEY = re.compile(
    r"(?i)(api[_-]?key|secret|token|password|passwd|pwd)\s*[:=]\s*['\"]?[A-Za-z0-9_\-/.+]{16,}['\"]?")
_PRIVATE_IP = re.compile(r"\b(10\.\d{1,3}\.\d{1,3}\.\d{1,3}|"
                         r"192\.168\.\d{1,3}\.\d{1,3}|"
                         r"172\.(1[6-9]|2\d|3[01])\.\d{1,3}\.\d{1,3})\b")

_PLACEHOLDER_IP = "10.0.0.1"  # synthetic private IP


def regex_redact(text: str) -> tuple[str, int]:
    n = 0
    text, c = _EMAIL.subn("<EMAIL>", text); n += c
    text, c = _SECRET_KEY.subn(lambda m: m.group(0).split("=", 1)[0] + "=<KEY>", text); n += c
    text, c = _PRIVATE_IP.subn(_PLACEHOLDER_IP, text); n += c
    return text, n


class StarPII:
    """Lazy-loaded StarPII NER pipeline (bigcode/starpii)."""

    def __init__(self):
        self._pipe = None

    def _load(self):
        if self._pipe is None and _HAS_HF:
            self._pipe = pipeline("ner", model="bigcode/starpii",
                                  aggregation_strategy="simple")
        return self._pipe

    def redact(self, text: str) -> tuple[str, int]:
        pipe = self._load()
        if pipe is None:
            return regex_redact(text)
        n = 0
        ents = pipe(text[:100_000])
        # replace from the end to keep offsets valid
        for ent in sorted(ents, key=lambda e: -e["start"]):
            kind = ent["entity_group"].upper()
            token = {"EMAIL": "<EMAIL>", "KEY": "<KEY>", "NAME": "<NAME>",
                     "PASSWORD": "<PASSWORD>", "IP": _PLACEHOLDER_IP}.get(kind)
            if token:
                text = text[:ent["start"]] + token + text[ent["end"]:]
                n += 1
        # also run regex pass for what the model missed
        text, c = regex_redact(text)
        return text, n + c


def redact_files(records: list[FileRecord]) -> tuple[list[FileRecord], StageStats, bool]:
    stats = StageStats(stage="pii")
    pii = StarPII()
    model_used = _HAS_HF
    kept = []
    for rec in records:
        stats.seen += 1
        rec.content, n = pii.redact(rec.content)
        if n:
            stats.reasons["redactions"] = stats.reasons.get("redactions", 0) + n
        kept.append(rec)
        stats.kept += 1
    return kept, stats, model_used
