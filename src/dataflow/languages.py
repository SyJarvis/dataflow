"""Step 2 - Decode bytes and detect language (go-enry with a builtin fallback).

Stack v3 official behaviour:

    File bytes are decoded to UTF-8 (falling back to cchardet encoding
    detection). Language, vendor, and generated-file flags are assigned with
    go-enry. Each unique content blob is content-addressed by its SHA-1.

go-enry is a Go library; when no CLI binary is configured we fall back to an
extension-based table + shebang sniffing. The fallback detects ~40 common
languages and vendor directories; plug in the real enry binary for full
Linguist-compatible coverage (see configs/default.yaml: languages.go_enry_bin).
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
from pathlib import Path

from .models import FileRecord, StageStats

try:  # optional, only used when configured
    import cchardet  # type: ignore
except ImportError:  # pragma: no cover
    cchardet = None

# ---------------------------------------------------------------------------
# Extension-based language table (subset of Linguist, grows as needed)
# ---------------------------------------------------------------------------
_EXT_LANG = {
    ".py": "Python", ".pyw": "Python", ".pyi": "Python",
    ".js": "JavaScript", ".mjs": "JavaScript", ".cjs": "JavaScript",
    ".jsx": "JSX", ".tsx": "TSX", ".ts": "TypeScript",
    ".java": "Java", ".kt": "Kotlin", ".kts": "Kotlin",
    ".c": "C", ".h": "C", ".cc": "C++", ".cpp": "C++", ".cxx": "C++",
    ".hpp": "C++", ".hh": "C++", ".hxx": "C++",
    ".go": "Go", ".rs": "Rust", ".rb": "Ruby", ".php": "PHP",
    ".swift": "Swift", ".m": "Objective-C", ".mm": "Objective-C++",
    ".cs": "C#", ".scala": "Scala", ".sh": "Shell", ".bash": "Shell",
    ".zsh": "Shell", ".fish": "Shell", ".ps1": "PowerShell",
    ".r": "R", ".R": "R", ".jl": "Julia", ".lua": "Lua", ".pl": "Perl",
    ".pm": "Perl", ".hs": "Haskell", ".ml": "OCaml", ".ex": "Elixir",
    ".exs": "Elixir", ".erl": "Erlang", ".clj": "Clojure", ".dart": "Dart",
    ".vue": "Vue", ".svelte": "Svelte",
    ".sql": "SQL", ".css": "CSS", ".scss": "SCSS", ".less": "Less",
    ".html": "HTML", ".htm": "HTML", ".xhtml": "HTML",
    ".xml": "XML", ".svg": "SVG", ".md": "Markdown", ".rst": "reStructuredText",
    ".tex": "TeX", ".json": "JSON", ".yaml": "YAML", ".yml": "YAML",
    ".toml": "TOML", ".ini": "INI", ".cfg": "INI", ".conf": "INI",
    ".csv": "CSV", ".tsv": "CSV", ".proto": "Protocol Buffer",
    ".ipynb": "Jupyter Notebook", ".ipynb.txt": "Jupyter Notebook",
    ".asm": "Assembly", ".s": "Assembly", ".S": "Assembly",
    ".f90": "Fortran", ".f95": "Fortran", ".f03": "Fortran",
    ".cob": "COBOL", ".cbl": "COBOL", ".adb": "Ada", ".ads": "Ada",
    ".groovy": "Groovy", ".gradle": "Groovy", ".tf": "HCL",
    ".vim": "Vim Script", ".el": "Emacs Lisp", ".lisp": "Common Lisp",
    ".nix": "Nix", ".zig": "Zig", ".v": "Verilog", ".sv": "SystemVerilog",
    ".vhd": "VHDL", ".pas": "Pascal", ".d": "D", ".nim": "Nim",
    ".cr": "Crystal", ".raku": "Raku", ".st": "Smalltalk",
    ".txt": "Text", ".gitignore": "Text",
    ".Makefile": "Makefile", ".mk": "Makefile",
}

# Files matched by basename regardless of extension
_BASENAME_LANG = {
    "Makefile": "Makefile", "Dockerfile": "Dockerfile",
    "CMakeLists.txt": "CMake", "Rakefile": "Ruby", "Gemfile": "Ruby",
    "package.json": "JSON", "requirements.txt": "Text",
}

# Vendored / third-party path prefixes (subset of go-enry vendored rules)
_VENDOR_PREFIXES = (
    "vendor/", "node_modules/", "third_party/", "thirdparty/",
    "third-party/", "external/", "deps/", "Godeps/", "site-packages/",
)

_VENDOR_EXTS = {".min.js", ".min.css", ".lock"}


def detect_language(file_path: str, content_head: str) -> tuple[str, bool]:
    """Return (language, is_vendor). Empty language = undetected."""
    p = file_path
    for pref in _VENDOR_PREFIXES:
        if p.startswith(pref) or f"/{pref}" in p:
            return "", True
    name = p.rsplit("/", 1)[-1]
    if name in _BASENAME_LANG:
        lang = _BASENAME_LANG[name]
    elif name == "Makefile" or name.startswith("Makefile."):
        lang = "Makefile"
    elif name.startswith("Dockerfile"):
        lang = "Dockerfile"
    else:
        ext = "." + name.rsplit(".", 1)[-1].lower() if "." in name else ""
        lang = _EXT_LANG.get(ext, "")
        if not ext or ext in _VENDOR_EXTS:
            is_v = ext in _VENDOR_EXTS
            if not lang:
                # shebang sniffing for extensionless scripts
                if content_head.startswith("#!"):
                    head = content_head.splitlines()[0]
                    for tok, l in (
                        ("python", "Python"), ("bash", "Shell"), ("sh", "Shell"),
                        ("node", "JavaScript"), ("ruby", "Ruby"), ("perl", "Perl"),
                    ):
                        if tok in head:
                            return l, is_v
                return "", is_v
            if is_v:
                return lang, True
    # special-case: Python wheels' vendored packages
    return lang, False


class EnryDetector:
    """Wrapper over the go-enry CLI when available (per-file subprocess)."""

    def __init__(self, bin_path: str = ""):
        self.bin = bin_path or None

    def detect(self, file_path: str, content: str) -> tuple[str, bool]:
        if self.bin:
            try:
                return self._detect_via_cli(file_path, content)
            except Exception:
                pass
        return detect_language(file_path, content[:2048])

    def _detect_via_cli(self, file_path: str, content: str) -> tuple[str, bool]:
        """Same result as `enry -json <tmp>/<rel>` (see _detect_via_cli_orig),
        but served by a long-lived `enry-server` process (one per worker
        process) instead of temp dir + file + subprocess per file.

        enry v2.9.6 single-file analysis = GetLanguage(abs_path, first 16 MiB (CLI -limit default 16*1024 KB))
        and IsVendor(abs_path); we send the identical path string and bytes.
        """
        server = _enry_server_path(self.bin)
        if server is None:
            return self._detect_via_cli_orig(file_path, content)
        parts = [p for p in file_path.split("/") if p not in ("", ".", "..")]
        rel = os.path.join(*parts) if parts else "unnamed"
        tmp_dir = os.path.join(tempfile.gettempdir(),
                               "enry-" + next(tempfile._get_candidate_names()))
        tmp_path = os.path.join(tmp_dir, rel)
        # reproduce the failures the original would hit (makedirs/open)
        bpath = os.fsencode(tmp_path)
        if b"\0" in bpath or len(bpath) >= 4096 or any(
                len(c) > 255 for c in bpath.split(b"/")):
            raise OSError("path not creatable")
        data = content[:64 * 1024].encode("utf-8", errors="replace")[:16 * 1024 * 1024]
        info = _enry_server_query(server, bpath, data)
        lang = info.get("language") or ""
        if lang:
            return lang, bool(info.get("vendored", False))
        raise ValueError(f"enry returned empty language: {info!r}")

    def _detect_via_cli_orig(self, file_path: str, content: str) -> tuple[str, bool]:
        """Run `enry -json <file>`; raise on any CLI/JSON failure."""
        # enry needs a real path: the basename/extension drives language
        # detection, and the parent dirs drive vendor-path detection
        # (/dev/stdin or a bare basename loses both).
        parts = [p for p in file_path.split("/") if p not in ("", ".", "..")]
        rel = os.path.join(*parts) if parts else "unnamed"
        with tempfile.TemporaryDirectory(prefix="enry-") as tmp_dir:
            tmp_path = os.path.join(tmp_dir, rel)
            os.makedirs(os.path.dirname(tmp_path), exist_ok=True)
            with open(tmp_path, "w", encoding="utf-8", errors="replace") as f:
                f.write(content[:64 * 1024])
            out = subprocess.run(
                [self.bin, "-json", tmp_path],
                capture_output=True, timeout=10,
            )
            info = json.loads(out.stdout.decode("utf-8", "replace"))
        lang = info.get("language") or ""
        if lang:
            return lang, bool(info.get("vendored", False))
        raise ValueError(f"enry returned empty language: {info!r}")



_SERVERS: dict = {}


def _enry_server_path(bin_path):
    cand = os.environ.get("DATAFLOW_ENRY_SERVER") or (bin_path + "-server")
    if cand == "off":
        return None
    return cand if os.access(cand, os.X_OK) else None


def _enry_server_query(server, bpath: bytes, data: bytes) -> dict:
    import struct
    for attempt in (0, 1):
        proc = _SERVERS.get(server)
        if proc is None or proc.poll() is not None:
            proc = subprocess.Popen([server], stdin=subprocess.PIPE,
                                    stdout=subprocess.PIPE)
            _SERVERS[server] = proc
        try:
            proc.stdin.write(struct.pack(">I", len(bpath)) + bpath
                             + struct.pack(">I", len(data)) + data)
            proc.stdin.flush()
            line = proc.stdout.readline()
            if not line:
                raise BrokenPipeError("enry-server exited")
            return json.loads(line)
        except (BrokenPipeError, OSError, ValueError):
            try:
                proc.kill()
            except Exception:
                pass
            _SERVERS.pop(server, None)
            if attempt:
                raise
    raise RuntimeError("unreachable")


def decode_bytes(raw: bytes) -> str | None:
    """UTF-8 first, then cchardet fallback. Return None if undecodable."""
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError:
        pass
    if cchardet is not None:
        guess = cchardet.detect(raw)
        enc = guess.get("encoding")
        if enc:
            try:
                return raw.decode(enc)
            except (UnicodeDecodeError, LookupError):
                return None
    return None


def process_files(
    records: list[tuple[FileRecord, bytes]],
    enry: EnryDetector,
) -> tuple[list[FileRecord], StageStats]:
    """Decode + language-tag a batch of (record, raw bytes)."""
    stats = StageStats(stage="languages")
    out: list[FileRecord] = []
    for rec, raw in records:
        stats.seen += 1
        text = decode_bytes(raw)
        if text is None:
            stats.drop("undecodable")
            continue
        rec.content = text
        rec.language, rec.is_vendor = enry.detect(rec.file_path, text)
        out.append(rec)
        stats.kept += 1
    return out, stats
