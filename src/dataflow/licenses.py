"""Step 3 - License detection (ScanCode) with directory-tree propagation.

Stack v3 official behaviour:

    1. Identify license files: names matching license-bearing patterns
       (LICENSE, COPYING, MIT.txt, Apache2.0, NOTICE, README, GUIDELINES, ...).
    2. Detect licenses with the ScanCode Toolkit -> SPDX identifiers.
    3. Propagate to siblings: detected licenses cover every file sharing the
       license file's directory prefix (repo root covers all; src/lib/ covers
       only that subtree). Multiple covering files merge their licenses.
    4. Classify each file:
         permissive     >=1 permissive license, no conflicting non-permissive
         no_license     nothing detected (kept by default)
         non_permissive GPL-like (dropped from released datasets)

The permissive allowlist follows Blue Oak Council + ScanCode
Permissive/Public-Domain categories.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath

from .models import FileRecord, StageStats

# Permissive SPDX ids (Blue Oak + ScanCode permissive/public-domain, common subset).
PERMISSIVE_SPDX = {
    "MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "BSD-3-Clause-Clear",
    "ISC", "Unlicense", "Zlib", "BSL-1.0", "Boost-Software-License-1.0",
    "BSD-2-Clause-FreeBSD", "BSD-2-Clause-NetBSD", "PostgreSQL",
    "Python-2.0", "X11", "Xerox", "Zpl-2.0", "Zpl-2.1",
    "CC0-1.0", "Unlicense", "WTFPL", "0BSD",
    "CC-BY-4.0", "CC-BY-3.0",
    "Artistic-2.0", "EPL-1.0", "EPL-2.0", "MPL-2.0",  # weak copyleft, Blue Oak lead/gold-ish
}
# Non-permissive families (dropped).
NON_PERMISSIVE_SPDX = {
    "GPL-2.0-only", "GPL-2.0-or-later", "GPL-3.0-only", "GPL-3.0-or-later",
    "AGPL-3.0-only", "AGPL-3.0-or-later", "LGPL-2.1-only", "LGPL-2.1-or-later",
    "LGPL-3.0-only", "LGPL-3.0-or-later", "SSPL-1.0",
}

_LICENSE_BASENAMES = {"license", "licence", "copying", "copyright", "notice",
                      "readme", "guidelines"}
_LICENSE_RE = re.compile(
    r"^(licen[cs]e|copying|copyright|notice|readme|guidelines)"
    r"([._\- ].*)?$", re.I)
_LICENSE_EXT_HINT = re.compile(
    r"(MIT|Apache|BSD|GPL|LGPL|AGPL|MPL|ISC|Zlib|Unlicense)", re.I)


def is_license_file(file_path: str) -> bool:
    name = PurePosixPath(file_path).name
    stem = name.rsplit(".", 1)[0] if "." in name else name
    if _LICENSE_RE.match(stem) or stem.lower() in _LICENSE_BASENAMES:
        return True
    # bare license names like MIT.txt, Apache2.0, BSD-3-Clause (Stack v3 patterns)
    return bool(_LICENSE_EXT_HINT.fullmatch(stem)) or bool(_LICENSE_EXT_HINT.search(name))


def detect_spdx_fallback(text: str) -> list[str]:
    """Regex fallback when ScanCode is unavailable. Best-effort SPDX ids."""
    found: list[str] = []
    head = text[:8000]
    patterns = {
        "MIT": r"MIT License|Permission is hereby granted, free of charge",
        "Apache-2.0": r"Apache License\s*2\.0|Version 2\.0, January 2004",
        "BSD-3-Clause": r"Redistribution and use in source and binary forms.*?NEGLIGENCE",
        "BSD-2-Clause": r"Redistribution and use in source and binary forms",
        "ISC": r"ISC License|Permission to use, copy, modify",
        "GPL-3.0-only": r"GNU GENERAL PUBLIC LICENSE\s*Version 3",
        "GPL-2.0-only": r"GNU GENERAL PUBLIC LICENSE\s*Version 2",
        "AGPL-3.0-only": r"GNU AFFERO GENERAL PUBLIC LICENSE",
        "MPL-2.0": r"Mozilla Public License\s*Version 2\.0",
        "Unlicense": r"This is free and unencumbered software released into the public domain",
        "CC0-1.0": r"Creative Commons Zero|CC0 1\.0",
    }
    for spdx, pat in patterns.items():
        if re.search(pat, head, re.S | re.I):
            found.append(spdx)
    return found


def classify(detected: list[str]) -> str:
    permissive = [s for s in detected if s in PERMISSIVE_SPDX]
    nonperm = [s for s in detected if s in NON_PERMISSIVE_SPDX]
    if nonperm:
        return "non_permissive"
    if permissive:
        return "permissive"
    return "no_license"


def annotate_repo_licenses(
    records: list[FileRecord], *, scancode_available: bool = False
) -> tuple[list[FileRecord], StageStats]:
    """Scan license files within one repo, propagate along the directory tree,
    and classify every file. Returns records annotated with license_type."""
    stats = StageStats(stage="licenses")
    covers: list[tuple[str, str, list[str]]] = []  # (dir_prefix, basename, spdx list)

    for rec in records:
        if is_license_file(rec.file_path):
            # ScanCode hook: when scancode_available, call it instead
            spdx = detect_spdx_fallback(rec.content)
            if spdx:
                prefix = str(PurePosixPath(rec.file_path).parent)
                covers.append((prefix, PurePosixPath(rec.file_path).name, spdx))

    for rec in records:
        stats.seen += 1
        merged: list[str] = []
        for prefix, _name, spdx in covers:
            parent = str(PurePosixPath(rec.file_path).parent)
            if parent == prefix or parent.startswith(prefix + "/") or prefix == ".":
                merged.extend(spdx)
        if merged:
            # Root-level README/GUIDELINES only weakly indicate; still count.
            rec.license_type = classify(merged)
        else:
            rec.license_type = "no_license"
        stats.kept += 1
    return records, stats


def drop_non_permissive(records: list[FileRecord]) -> tuple[list[FileRecord], StageStats]:
    stats = StageStats(stage="licenses_drop")
    kept = []
    for rec in records:
        stats.seen += 1
        if rec.license_type == "non_permissive":
            stats.drop("non_permissive")
            continue
        kept.append(rec)
        stats.kept += 1
    return kept, stats
