#!/usr/bin/env python3
import argparse
import glob
import os
import re
from datetime import datetime


CASE_LINE_RE = re.compile(r"^=== CASE: (.+) ===$")
PASS_LINE_RE = re.compile(r"^PASS: ([^(]+)\s+\((.+)\)$")
FAIL_LINE_RE = re.compile(r"^FAIL: ([^(]+)\s+\((.+)\)$")
SUMMARY_RE = re.compile(r"^SUMMARY:\s+pass=(\d+),\s+fail=(\d+),\s+total=(\d+)$")


def parse_report(path):
    result = {
        "path": path,
        "mtime": os.path.getmtime(path),
        "cases": [],
        "pass": 0,
        "fail": 0,
        "total": 0,
        "has_summary": False,
    }
    current_case = None
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        for raw in f:
            line = raw.strip()
            if not line:
                continue
            m = CASE_LINE_RE.match(line)
            if m:
                current_case = m.group(1).strip()
                continue
            m = PASS_LINE_RE.match(line)
            if m:
                case_name = m.group(1).strip()
                result["cases"].append(
                    {"name": case_name, "status": "PASS", "message": m.group(2).strip()}
                )
                continue
            m = FAIL_LINE_RE.match(line)
            if m:
                case_name = m.group(1).strip()
                result["cases"].append(
                    {"name": case_name, "status": "FAIL", "message": m.group(2).strip()}
                )
                continue
            m = SUMMARY_RE.match(line)
            if m:
                result["pass"] = int(m.group(1))
                result["fail"] = int(m.group(2))
                result["total"] = int(m.group(3))
                result["has_summary"] = True
                continue
            if current_case and result["cases"]:
                # Keep context simple: ignore non PASS/FAIL lines.
                pass

    if not result["has_summary"]:
        passed = sum(1 for c in result["cases"] if c["status"] == "PASS")
        failed = sum(1 for c in result["cases"] if c["status"] == "FAIL")
        result["pass"] = passed
        result["fail"] = failed
        result["total"] = len(result["cases"])
    return result


def build_markdown(reports):
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = []
    lines.append("# Gray Validation Summary")
    lines.append("")
    lines.append(f"- Generated at: {now_str}")
    lines.append(f"- Report files: {len(reports)}")
    lines.append("")

    agg_pass = sum(r["pass"] for r in reports)
    agg_fail = sum(r["fail"] for r in reports)
    agg_total = sum(r["total"] for r in reports)
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- Total cases: {agg_total}")
    lines.append(f"- Passed: {agg_pass}")
    lines.append(f"- Failed: {agg_fail}")
    lines.append("")

    lines.append("## Runs")
    lines.append("")
    lines.append("| Time | File | Pass | Fail | Total |")
    lines.append("| --- | --- | ---: | ---: | ---: |")
    for report in reports:
        time_str = datetime.fromtimestamp(report["mtime"]).strftime("%Y-%m-%d %H:%M:%S")
        file_name = os.path.basename(report["path"])
        lines.append(
            f"| {time_str} | `{file_name}` | {report['pass']} | {report['fail']} | {report['total']} |"
        )
    lines.append("")

    lines.append("## Failed Cases")
    lines.append("")
    any_fail = False
    for report in reports:
        failed = [c for c in report["cases"] if c["status"] == "FAIL"]
        if not failed:
            continue
        any_fail = True
        lines.append(f"### `{os.path.basename(report['path'])}`")
        lines.append("")
        for case in failed:
            lines.append(f"- `{case['name']}`: {case['message']}")
        lines.append("")
    if not any_fail:
        lines.append("- No failed cases found in selected logs.")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Summarize gray validation logs into a markdown report."
    )
    parser.add_argument(
        "--input-glob",
        default="scripts/reports/gray_validation_*.log",
        help="glob pattern for gray validation log files",
    )
    parser.add_argument(
        "--output",
        default="scripts/reports/gray_validation_summary.md",
        help="output markdown file path",
    )
    parser.add_argument(
        "--latest",
        type=int,
        default=0,
        help="only include latest N logs (0 means all matched logs)",
    )
    args = parser.parse_args()

    paths = sorted(glob.glob(args.input_glob), key=lambda p: os.path.getmtime(p), reverse=True)
    if args.latest > 0:
        paths = paths[: args.latest]
    if len(paths) == 0:
        raise SystemExit(f"error: no logs matched input glob: {args.input_glob}")

    reports = [parse_report(p) for p in paths]
    md = build_markdown(reports)

    out_dir = os.path.dirname(args.output) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(md)
        f.write("\n")

    print(f"summary generated: {args.output}")


if __name__ == "__main__":
    main()
