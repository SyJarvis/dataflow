#!/usr/bin/env python3
"""Scan github_downloads tar.gz headers (+ first 64KiB null-byte check).

Writes per-tarball JSON under ~/dataflow_prep_20261111/funnel/tar_scan_parts/
and a final aggregate tar_scan_aggregate.json. Resumable: existing part
files with a matching input size are skipped.
"""
from __future__ import annotations

import argparse
import json
import os
import tarfile
import time
import traceback
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

OUT = Path(os.path.expanduser("~/dataflow_prep_20261111/funnel"))
PARTS = OUT / "tar_scan_parts"
RAW = Path("/mnt/data4/cleaning_data/github_downloads")
MAX_FILE_BYTES = 5_242_880
NULL_PREFIX = 65536

IMAGE_EXT = {
    "png", "jpg", "jpeg", "gif", "webp", "bmp", "ico", "svg", "svgz",
    "tif", "tiff", "heic", "heif", "avif", "psd", "raw", "cr2", "nef",
    "orf", "arw", "dng", "exr", "tga", "pcx", "jfif", "pnm", "ppm",
    "pgm", "pbm", "xpm", "xbm", "jp2", "jxl", "apng", "cur", "icns",
}
VIDEO_EXT = {
    "mp4", "mkv", "avi", "mov", "webm", "flv", "wmv", "m4v", "mpeg",
    "mpg", "m2ts", "mts", "3gp", "3g2", "ogv", "vob", "ts", "m4p",
    "rm", "rmvb", "asf", "f4v",
}
AUDIO_EXT = {
    "mp3", "wav", "flac", "aac", "ogg", "m4a", "wma", "opus", "aiff",
    "aif", "au", "mid", "midi", "amr", "pcm", "ra", "ac3", "caf",
}
ARCHIVE_EXT = {
    "zip", "tar", "gz", "tgz", "bz2", "tbz2", "xz", "txz", "7z", "rar",
    "zst", "zstd", "lz", "lz4", "lzma", "br", "cab", "jar", "war",
    "ear", "whl", "egg", "nupkg", "apk", "ipa", "deb", "rpm", "pkg",
    "crx", "vsix", "xpi",
}
MODEL_EXT = {
    "pt", "pth", "ckpt", "safetensors", "onnx", "pb", "tflite", "h5",
    "hdf5", "npy", "npz", "pkl", "pickle", "joblib", "gguf", "ggml",
    "msgpack", "arrow", "feather", "parquet", "params", "weights",
    "keras", "tfevents", "ot", "ort", "ncnn", "param", "mlmodel",
    "mlpackage", "engine", "trt",
}
FONT_EXT = {"ttf", "otf", "woff", "woff2", "eot", "ttc", "pfb", "pfm", "bdf", "ps"}
EXEC_EXT = {
    "exe", "dll", "so", "dylib", "a", "o", "obj", "lib", "class", "pyc",
    "pyo", "wasm", "sys", "msi", "com", "elf", "ko", "rlib", "rmeta",
    "pdb", "ilk", "exp", "bundle", "node",
}
OFFICE_EXT = {
    "pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "odt", "ods",
    "odp", "rtf", "pages", "numbers", "key", "epub", "djvu", "xps",
}
OTHER_BIN_EXT = {
    "dat", "data", "idx", "pack", "dmg", "iso", "img", "vmdk", "qcow2",
    "sqlite", "sqlite3", "db", "mdb", "accdb", "frm", "ibd", "myd", "myi",
    "rdb", "sst", "bloom", "pyd", "blend", "fbx",
    "glb", "gltf", "3ds", "max", "stl", "ply", "step", "stp",
    "iges", "igs", "skp", "dwg", "dxf", "swf", "fla", "unity3d", "asset",
    "assets", "pak", "wad", "bsp", "rpa", "rpyc",
}

COMPOUND = (
    (".tar.gz", "archives"),
    (".tar.bz2", "archives"),
    (".tar.xz", "archives"),
    (".tar.zst", "archives"),
    (".tar.lz", "archives"),
    (".tar.lzma", "archives"),
    (".tgz", "archives"),
    (".tbz2", "archives"),
    (".txz", "archives"),
)

# .bin is ambiguous (weights vs generic binary). Track separately.
EXT_TO_CAT: dict[str, str] = {}
for _ext, _cat in (
    (IMAGE_EXT, "images"),
    (VIDEO_EXT, "video"),
    (AUDIO_EXT, "audio"),
    (ARCHIVE_EXT, "archives"),
    (MODEL_EXT, "model_ml"),
    (FONT_EXT, "fonts"),
    (EXEC_EXT, "executables_libs"),
    (OFFICE_EXT, "pdfs_office"),
    (OTHER_BIN_EXT, "other_binary_ext"),
    ({"bin"}, "bin_generic"),
):
    for e in _ext:
        EXT_TO_CAT.setdefault(e, _cat)


def ext_category(name: str) -> str:
    lower = name.lower().replace("\\", "/")
    base = lower.rsplit("/", 1)[-1]
    for suf, cat in COMPOUND:
        if base.endswith(suf):
            return cat
    if "." not in base:
        return "no_extension"
    ext = base.rsplit(".", 1)[-1]
    if not ext or len(ext) > 14:
        return "no_extension"
    return EXT_TO_CAT.get(ext, "text_or_other")


def is_git_internal(name: str) -> bool:
    parts = [x for x in name.lower().replace("\\", "/").split("/") if x]
    return ".git" in parts


def member_kind(m: tarfile.TarInfo) -> str:
    if m.isdir():
        return "directory"
    if m.issym():
        return "symlink"
    if m.islnk():
        return "hardlink"
    if m.isfile():
        return "regular"
    if m.isfifo():
        return "fifo"
    if m.ischr() or m.isblk():
        return "device"
    return "other_nonregular"


def scan_one(path: str) -> dict:
    t0 = time.time()
    st = os.lstat(path)
    rec = {
        "path": path,
        "compressed_bytes": st.st_size,
        "ok": False,
        "error": None,
        "members": 0,
        "regular_files": 0,
        "regular_bytes": 0,
        "dir_count": 0,
        "symlink_count": 0,
        "symlink_bytes": 0,
        "hardlink_count": 0,
        "other_nonregular_count": 0,
        "git_internal_files": 0,
        "git_internal_bytes": 0,
        "oversize_files": 0,
        "oversize_bytes": 0,
        "null_byte_files": 0,
        "null_byte_bytes": 0,
        "unpack_kept_files": 0,
        "unpack_kept_bytes": 0,
        "by_category": {},
        "by_reason_category": {},
        "top_other_exts": {},
        "elapsed_sec": 0.0,
    }
    cat_files = defaultdict(int)
    cat_bytes = defaultdict(int)
    reason_cat_files = defaultdict(int)
    reason_cat_bytes = defaultdict(int)
    other_ext_bytes = defaultdict(int)
    other_ext_files = defaultdict(int)

    def add(reason: str, cat: str, size: int, n: int = 1):
        cat_files[cat] += n
        cat_bytes[cat] += size
        key = f"{reason}|{cat}"
        reason_cat_files[key] += n
        reason_cat_bytes[key] += size

    try:
        with tarfile.open(path, "r:*") as tf:
            while True:
                m = tf.next()
                if m is None:
                    break
                rec["members"] += 1
                kind = member_kind(m)
                size = int(m.size or 0)
                name = m.name or ""
                cat = ext_category(name)
                if is_git_internal(name):
                    rec["git_internal_files"] += 1
                    rec["git_internal_bytes"] += size

                if kind == "directory":
                    rec["dir_count"] += 1
                    add("symlink_or_nonregular", "directory", 0)
                    continue
                if kind == "symlink":
                    rec["symlink_count"] += 1
                    rec["symlink_bytes"] += size
                    add("symlink_or_nonregular", "symlink", size)
                    continue
                if kind != "regular":
                    if kind == "hardlink":
                        rec["hardlink_count"] += 1
                    else:
                        rec["other_nonregular_count"] += 1
                    add("symlink_or_nonregular", kind, size)
                    continue

                rec["regular_files"] += 1
                rec["regular_bytes"] += size
                if cat == "text_or_other":
                    base = name.lower().rsplit("/", 1)[-1]
                    ext = base.rsplit(".", 1)[-1] if "." in base else ""
                    if ext and len(ext) <= 14:
                        other_ext_files[ext] += 1
                        other_ext_bytes[ext] += size
                    else:
                        other_ext_files[""] += 1
                        other_ext_bytes[""] += size

                if size > MAX_FILE_BYTES:
                    rec["oversize_files"] += 1
                    rec["oversize_bytes"] += size
                    add("oversize_file", cat, size)
                    continue

                has_null = False
                try:
                    f = tf.extractfile(m)
                    if f is None:
                        rec["other_nonregular_count"] += 1
                        rec["regular_files"] -= 1
                        rec["regular_bytes"] -= size
                        add("symlink_or_nonregular", "extractfile_none", size)
                        continue
                    prefix = f.read(NULL_PREFIX)
                    has_null = b"\x00" in prefix
                    leftover = size - len(prefix)
                    while leftover > 0:
                        chunk = f.read(1024 * 1024 if leftover > 1024 * 1024 else leftover)
                        if not chunk:
                            break
                        leftover -= len(chunk)
                    f.close()
                except Exception:
                    has_null = False
                    add("unpack_kept", cat, size)
                    rec["unpack_kept_files"] += 1
                    rec["unpack_kept_bytes"] += size
                    continue

                if has_null:
                    rec["null_byte_files"] += 1
                    rec["null_byte_bytes"] += size
                    add("binary_null_byte", cat, size)
                else:
                    rec["unpack_kept_files"] += 1
                    rec["unpack_kept_bytes"] += size
                    add("unpack_kept", cat, size)

        rec["ok"] = True
    except Exception as e:
        rec["error"] = f"{type(e).__name__}: {e}"
        rec["traceback"] = traceback.format_exc()[-1500:]

    rec["by_category"] = {
        k: {"files": cat_files[k], "bytes": cat_bytes[k]} for k in cat_files
    }
    rec["by_reason_category"] = {
        k: {"files": reason_cat_files[k], "bytes": reason_cat_bytes[k]}
        for k in reason_cat_files
    }
    top = sorted(other_ext_bytes.items(), key=lambda kv: -kv[1])[:30]
    rec["top_other_exts"] = {
        e: {"files": other_ext_files[e], "bytes": b} for e, b in top
    }
    rec["elapsed_sec"] = round(time.time() - t0, 3)
    return rec


def part_path_for(tar_path: str) -> Path:
    rel = tar_path.replace("/mnt/data4/cleaning_data/github_downloads/", "").replace("/", "__")
    if len(rel) > 200:
        import hashlib
        rel = rel[:80] + "__" + hashlib.sha1(tar_path.encode()).hexdigest()
    return PARTS / (rel + ".json")


def already_done(tar_path: str, size: int) -> bool:
    p = part_path_for(tar_path)
    if not p.exists():
        return False
    try:
        with p.open() as f:
            obj = json.load(f)
        return obj.get("compressed_bytes") == size and (obj.get("ok") or obj.get("error"))
    except Exception:
        return False


def process(tar_path: str) -> tuple[str, bool, str]:
    try:
        size = os.path.getsize(tar_path)
        if already_done(tar_path, size):
            return tar_path, True, "skip"
        rec = scan_one(tar_path)
        dest = part_path_for(tar_path)
        tmp = dest.with_suffix(".tmp")
        with tmp.open("w") as f:
            json.dump(rec, f, ensure_ascii=False)
        tmp.replace(dest)
        status = "ok" if rec["ok"] else f"err:{rec.get('error')}"
        return tar_path, True, status
    except Exception as e:
        return tar_path, False, f"{type(e).__name__}: {e}"


def list_tarballs() -> list[str]:
    out = []
    for root, dirs, files in os.walk(RAW):
        dirs.sort()
        files.sort()
        for fn in files:
            if fn.endswith(".tar.gz") or fn.endswith(".tgz"):
                out.append(os.path.join(root, fn))
    out.sort()
    return out


def aggregate(part_dir: Path) -> dict:
    totals = {
        "tarballs": 0,
        "tarballs_ok": 0,
        "tarballs_error": 0,
        "compressed_bytes": 0,
        "compressed_ok": 0,
        "compressed_error": 0,
        "members": 0,
        "regular_files": 0,
        "regular_bytes": 0,
        "dir_count": 0,
        "symlink_count": 0,
        "symlink_bytes": 0,
        "hardlink_count": 0,
        "other_nonregular_count": 0,
        "git_internal_files": 0,
        "git_internal_bytes": 0,
        "oversize_files": 0,
        "oversize_bytes": 0,
        "null_byte_files": 0,
        "null_byte_bytes": 0,
        "unpack_kept_files": 0,
        "unpack_kept_bytes": 0,
        "by_category": defaultdict(lambda: {"files": 0, "bytes": 0}),
        "by_reason_category": defaultdict(lambda: {"files": 0, "bytes": 0}),
        "top_other_exts": defaultdict(lambda: {"files": 0, "bytes": 0}),
        "errors": [],
        "largest_regular_byte_tars": [],
    }
    largest = []
    for p in part_dir.glob("*.json"):
        if p.name.endswith(".tmp"):
            continue
        with p.open() as f:
            rec = json.load(f)
        totals["tarballs"] += 1
        totals["compressed_bytes"] += rec.get("compressed_bytes") or 0
        if rec.get("ok"):
            totals["tarballs_ok"] += 1
            totals["compressed_ok"] += rec.get("compressed_bytes") or 0
        else:
            totals["tarballs_error"] += 1
            totals["compressed_error"] += rec.get("compressed_bytes") or 0
            if len(totals["errors"]) < 80:
                totals["errors"].append({
                    "path": rec.get("path"),
                    "error": rec.get("error"),
                    "compressed_bytes": rec.get("compressed_bytes"),
                })
            continue
        for k in (
            "members", "regular_files", "regular_bytes", "dir_count",
            "symlink_count", "symlink_bytes", "hardlink_count",
            "other_nonregular_count", "git_internal_files", "git_internal_bytes",
            "oversize_files", "oversize_bytes", "null_byte_files",
            "null_byte_bytes", "unpack_kept_files", "unpack_kept_bytes",
        ):
            totals[k] += rec.get(k) or 0
        for cat, v in (rec.get("by_category") or {}).items():
            totals["by_category"][cat]["files"] += v.get("files") or 0
            totals["by_category"][cat]["bytes"] += v.get("bytes") or 0
        for key, v in (rec.get("by_reason_category") or {}).items():
            totals["by_reason_category"][key]["files"] += v.get("files") or 0
            totals["by_reason_category"][key]["bytes"] += v.get("bytes") or 0
        for ext, v in (rec.get("top_other_exts") or {}).items():
            totals["top_other_exts"][ext]["files"] += v.get("files") or 0
            totals["top_other_exts"][ext]["bytes"] += v.get("bytes") or 0
        largest.append((
            rec.get("regular_bytes") or 0,
            rec.get("path"),
            rec.get("compressed_bytes"),
            rec.get("regular_files"),
        ))

    largest.sort(reverse=True)
    totals["largest_regular_byte_tars"] = [
        {"regular_bytes": b, "path": p, "compressed_bytes": c, "regular_files": n}
        for b, p, c, n in largest[:40]
    ]
    totals["by_category"] = dict(sorted(
        totals["by_category"].items(), key=lambda kv: -kv[1]["bytes"]
    ))
    totals["by_reason_category"] = dict(sorted(
        totals["by_reason_category"].items(), key=lambda kv: -kv[1]["bytes"]
    ))
    top_other = sorted(totals["top_other_exts"].items(), key=lambda kv: -kv[1]["bytes"])[:80]
    totals["top_other_exts"] = dict(top_other)
    return totals


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=32)
    ap.add_argument("--aggregate-only", action="store_true")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    PARTS.mkdir(parents=True, exist_ok=True)

    if not args.aggregate_only:
        tars = list_tarballs()
        if args.limit:
            tars = tars[: args.limit]
        print(f"found {len(tars)} tarballs, workers={args.workers}", flush=True)
        todo = []
        skipped = 0
        for p in tars:
            try:
                sz = os.path.getsize(p)
            except OSError:
                todo.append(p)
                continue
            if already_done(p, sz):
                skipped += 1
            else:
                todo.append(p)
        print(f"todo={len(todo)} skipped_done={skipped}", flush=True)
        t0 = time.time()
        done = 0
        errs = 0
        if todo:
            with ProcessPoolExecutor(max_workers=args.workers) as ex:
                futs = [ex.submit(process, p) for p in todo]
                for fut in as_completed(futs):
                    path, ok, status = fut.result()
                    done += 1
                    if (status or "").startswith("err") or not ok:
                        errs += 1
                        print(f"ERR {status} {path}", flush=True)
                    if done % 50 == 0 or done == len(todo):
                        elapsed = time.time() - t0
                        rate = done / elapsed if elapsed else 0
                        print(
                            f"progress {done}/{len(todo)} errs={errs} "
                            f"elapsed={elapsed:.1f}s rate={rate:.2f}/s",
                            flush=True,
                        )
        print(
            f"scan finished todo={len(todo)} errs={errs} elapsed={time.time()-t0:.1f}s",
            flush=True,
        )

    totals = aggregate(PARTS)
    outp = OUT / "tar_scan_aggregate.json"
    with outp.open("w") as f:
        json.dump(totals, f, indent=2, ensure_ascii=False)
    print(
        "wrote", outp, "tarballs", totals["tarballs"], "ok", totals["tarballs_ok"],
        "regular_bytes", totals["regular_bytes"], flush=True,
    )


if __name__ == "__main__":
    main()
