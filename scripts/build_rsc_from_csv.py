#!/usr/bin/env python3
"""
build_rsc_from_csv.py

Build a RouterOS .rsc from a template + CSV, grouped by section.

Behavior:
- The template is split into sections starting with lines that begin with "/" (e.g. "/ip pool").
- For EACH section:
    - print the section header once
    - then loop ALL CSV rows and emit that section’s command lines for each row
    - then move to next section

Template placeholders use Python format braces, e.g. {identity}, {vlan}, {network}, {prefix}, {gateway}, {pool}, {speed}.

Defaults:
- If CSV lacks 'speed' or it's blank, speed defaults to --default-speed (250 by default).
"""

from __future__ import annotations

import argparse
import csv
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List


@dataclass
class SectionBlock:
    header: str               # e.g. "/interface vlan"
    lines: List[str]          # command lines under that header


def parse_template_sections(template_text: str) -> List[SectionBlock]:
    """
    Parse a RouterOS .rsc template where each section begins with a line that starts with "/".
    Everything after that (until the next "/") is treated as that section's command lines.

    Notes:
    - Blank lines inside sections are preserved.
    - If the template begins with non-section lines, they are treated as a headerless section.
    """
    blocks: List[SectionBlock] = []
    current: SectionBlock | None = None

    for raw in template_text.splitlines():
        line = raw.rstrip("\n")

        # Preserve blank lines inside a section
        if not line.strip():
            if current is not None:
                current.lines.append(line)
            continue

        # New section header
        if line.lstrip().startswith("/"):
            if current is not None:
                blocks.append(current)
            current = SectionBlock(header=line.strip(), lines=[])
            continue

        # Regular line
        if current is None:
            current = SectionBlock(header="", lines=[])
        current.lines.append(line)

    if current is not None:
        blocks.append(current)

    return blocks


def load_csv_rows(csv_path: Path) -> List[Dict[str, str]]:
    with csv_path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows = [dict(r) for r in reader]

    if not rows:
        raise SystemExit(f"No rows found in CSV: {csv_path}")

    if reader.fieldnames is None:
        raise SystemExit(f"CSV has no header row: {csv_path}")

    return rows


def apply_defaults(rows: List[Dict[str, str]], default_speed_m: int) -> None:
    """
    Ensure 'speed' exists for queue template usage.
    Your template uses {speed}M/{speed}M, so speed should be numeric (e.g. 250).
    """
    for r in rows:
        # Treat missing or blank as needing default
        if "speed" not in r or not str(r.get("speed", "")).strip():
            r["speed"] = str(default_speed_m)


def format_line(line: str, row: Dict[str, str]) -> str:
    """
    Format a template line using Python's str.format with CSV headers as keys.
    Raises KeyError if a placeholder is missing from the row.
    """
    return line.format(**row)


def build_grouped_rsc(blocks: List[SectionBlock], rows: List[Dict[str, str]]) -> str:
    """
    Emit: section header -> all rows -> next section header -> all rows -> ...
    """
    out_lines: List[str] = []

    for blk in blocks:
        if blk.header:
            out_lines.append(blk.header)

        for row in rows:
            for ln in blk.lines:
                if not ln.strip():
                    out_lines.append(ln)
                    continue
                out_lines.append(format_line(ln, row))

    # Ensure trailing newline (RouterOS import is happier)
    return "\n".join(out_lines).rstrip() + "\n"


def main(argv: List[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        description="Build a RouterOS .rsc from a template and CSV, grouped by section."
    )
    p.add_argument("--template", required=True, help="Path to the .rsc template with {placeholders}")
    p.add_argument("--csv", required=True, help="Path to CSV containing columns matching placeholders")
    p.add_argument("--out", default="-", help='Output .rsc path, or "-" for stdout (default)')
    p.add_argument("--default-speed", type=int, default=250, help="Default queue speed (Mbps) if CSV lacks 'speed'")
    args = p.parse_args(argv)

    template_path = Path(args.template)
    csv_path = Path(args.csv)

    if not template_path.exists():
        raise SystemExit(f"Template not found: {template_path}")
    if not csv_path.exists():
        raise SystemExit(f"CSV not found: {csv_path}")

    template_text = template_path.read_text(errors="replace")
    blocks = parse_template_sections(template_text)

    rows = load_csv_rows(csv_path)
    apply_defaults(rows, default_speed_m=args.default_speed)

    try:
        rsc_text = build_grouped_rsc(blocks, rows)
    except KeyError as e:
        missing = str(e).strip("'")
        cols = sorted(rows[0].keys())
        raise SystemExit(
            f"Template placeholder {{{missing}}} not found in CSV columns.\n"
            f"CSV columns: {cols}"
        )

    if args.out == "-":
        sys.stdout.write(rsc_text)
    else:
        out_path = Path(args.out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(rsc_text)
        print(f"Wrote RSC: {out_path} ({len(rows)} rows)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
