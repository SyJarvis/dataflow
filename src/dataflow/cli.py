"""dataflow CLI: run / report."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def main(argv=None):
    p = argparse.ArgumentParser(prog="dataflow",
                                description="Stack v3 style code cleaning pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="run the pipeline on a config")
    run_p.add_argument("config", type=Path)
    run_p.add_argument("--resume", action="store_true",
                       help="skip tarballs already recorded in manifest.json")

    rep_p = sub.add_parser("report", help="pretty-print a run report")
    rep_p.add_argument("path", type=Path, help="runs/<name>/report.json")

    validation = sub.add_parser("validate-input", help="read-only gzip/tar acceptance inventory")
    validation.add_argument("input_dir", type=Path)
    validation.add_argument("--output-dir", type=Path, required=True)
    validation.add_argument("--workers", type=int, default=4)
    validation.add_argument("--timeout-sec", type=float, default=1800)
    validation.add_argument("--max-expanded-bytes", type=int, default=256 * 1024**3)
    validation.add_argument("--resume", action="store_true")
    validation.add_argument("--headers-only", action="store_true")

    args = p.parse_args(argv)

    if args.cmd == "validate-input":
        from .inventory import validate_inputs
        validate_inputs(args.input_dir, args.output_dir, workers=args.workers,
                        timeout_sec=args.timeout_sec,
                        max_expanded_bytes=args.max_expanded_bytes,
                        resume=args.resume, headers_only=args.headers_only)
        return 0

    if args.cmd == "run":
        sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
        from .runner import run_pipeline
        report = run_pipeline(args.config, resume=args.resume)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0

    if args.cmd == "report":
        print(json.dumps(json.loads(args.path.read_text()), indent=2, ensure_ascii=False))
        return 0
    return 1
