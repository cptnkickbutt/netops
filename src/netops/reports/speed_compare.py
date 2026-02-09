#!/usr/bin/env python3
"""
Speed audit comparison tool.

Compares two speed audit runs (CSV or XLSX) and produces an Excel workbook
with a Summary sheet and a Changes sheet.

Expected columns in each run (minimum):
    - Property
    - Identity
    - Mac/Serial
    - Speed
    - Status

You can:
    - Pass explicit paths to "previous" and "current" runs, OR
    - Use --prev-date / --curr-date together with --snapshot-dir
      to auto-build paths like: {snapshot_dir}/speed_snapshot_{date}.csv

Example usage:

    # Explicit files:
    python speed_compare.py \
        --prev ./snapshots/speed_snapshot_2025-10-01.csv \
        --curr ./snapshots/speed_snapshot_2025-11-01.csv \
        --output ./reports/Report_Speed_Comparison_2025-11-01.xlsx

    # Date-based, using the default snapshot filename pattern:
    python speed_compare.py \
        --snapshot-dir ./snapshots \
        --prev-date 2025-10-01 \
        --curr-date 2025-11-01
"""

import argparse
import os
import re
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any

import numpy as np
import pandas as pd


# --------------------------------------------------------------------------------------
# Loading helpers
# --------------------------------------------------------------------------------------

REQUIRED_COLUMNS = ["Property", "Identity", "Mac/Serial", "Speed", "Status"]


def load_run(path: Path) -> pd.DataFrame:
    """
    Load a speed-audit run from a CSV or XLSX.

    - If CSV: read directly.
    - If XLSX: assume each property has its own sheet.
      We read all sheets except ones named like "TOC" or starting with "_",
      and concatenate them, adding a Property column if missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"Run file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(path)
        # Ensure Property column exists; for CSV we assume it's already there,
        # but if not, set to 'Unknown'.
        if "Property" not in df.columns:
            df["Property"] = "Unknown"
        return df

    elif suffix in (".xlsx", ".xlsm", ".xlsb"):
        # Read all property sheets
        xls = pd.ExcelFile(path)
        frames = []
        for sheet_name in xls.sheet_names:
            n = sheet_name.strip().lower()
            if n in {"toc", "summary"} or n.startswith("_"):
                continue  # skip non-property sheets

            sheet_df = xls.parse(sheet_name)
            if sheet_df.empty:
                continue

            if "Property" not in sheet_df.columns:
                sheet_df["Property"] = sheet_name

            frames.append(sheet_df)

        if not frames:
            raise ValueError(f"No usable sheets found in workbook: {path}")

        df = pd.concat(frames, ignore_index=True)
        return df

    else:
        raise ValueError(f"Unsupported file type: {path}")


def ensure_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure all required columns exist, filling with defaults if missing.
    """
    df = df.copy()
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            if col == "Property":
                df[col] = "Unknown"
            else:
                df[col] = np.nan
    # Enforce string-ish types where appropriate
    for col in ["Property", "Identity", "Mac/Serial", "Status"]:
        df[col] = df[col].astype(str).fillna("")
    # Speed stays as is; we'll handle conversion separately
    return df


def load_run(path: Path, debug: bool = False) -> pd.DataFrame:
    """
    Load a speed-audit run from a CSV or XLSX.

    - If CSV: read directly.
    - If XLSX: assume each property has its own sheet.
      We read all sheets except ones named like "TOC" or starting with "_",
      and concatenate them, adding a Property column if missing.
    """
    if not path.exists():
        raise FileNotFoundError(f"Run file not found: {path}")

    suffix = path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(path)
        if "Property" not in df.columns:
            df["Property"] = "Unknown"
        if debug:
            print(f"[DEBUG] Loaded CSV {path}")
            print(f"[DEBUG] Rows: {len(df)}, Columns: {list(df.columns)}")
            print(f"[DEBUG] Unique properties: {df['Property'].nunique()}")
        return df

    elif suffix in (".xlsx", ".xlsm", ".xlsb"):
        xls = pd.ExcelFile(path)
        frames = []
        if debug:
            print(f"[DEBUG] Loading XLSX {path}")
            print(f"[DEBUG] Sheets: {xls.sheet_names}")

        for sheet_name in xls.sheet_names:
            n = sheet_name.strip().lower()
            if n in {"toc", "summary"} or n.startswith("_"):
                continue

            sheet_df = xls.parse(sheet_name)
            if sheet_df.empty:
                continue

            if "Property" not in sheet_df.columns:
                sheet_df["Property"] = sheet_name

            frames.append(sheet_df)

        if not frames:
            raise ValueError(f"No usable sheets found in workbook: {path}")

        df = pd.concat(frames, ignore_index=True)

        if debug:
            print(f"[DEBUG] Combined rows from property sheets: {len(df)}")
            print(f"[DEBUG] Columns: {list(df.columns)}")
            print(f"[DEBUG] Unique properties: {df['Property'].nunique()}")

        return df

    else:
        raise ValueError(f"Unsupported file type: {path}")


# --------------------------------------------------------------------------------------
# Comparison logic
# --------------------------------------------------------------------------------------

def to_numeric_speed(series: pd.Series) -> pd.Series:
    """
    Best-effort conversion of Speed column to numeric for comparisons.

    Handles things like:
        "50", "50M", "50 Mbps", "100/20", "No Data"
    by extracting the first numeric chunk it can find.
    """
    # Convert to string and extract first number-like pattern
    s = series.astype(str).str.extract(r"([0-9]+\.?[0-9]*)", expand=False)
    return pd.to_numeric(s, errors="coerce")


def compare_runs(
    prev_df: pd.DataFrame,
    curr_df: pd.DataFrame,
    debug: bool = False,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Core comparison logic.

    Returns:
        changes_df: DataFrame with one row per identity that changed.
        summary: dict with aggregated stats for summary sheet.

    changes_df columns:
        - Property
        - Identity
        - Change
        - Prev Speed
        - Curr Speed
        - Prev Speed Num
        - Curr Speed Num
        - Prev Mac/Serial
        - Curr Mac/Serial
        - Prev Status
        - Curr Status
    """

    prev_df = ensure_columns(prev_df)
    curr_df = ensure_columns(curr_df)

    # Normalize key fields
    for col in ["Property", "Identity"]:
        prev_df[col] = prev_df[col].astype(str).str.strip()
        curr_df[col] = curr_df[col].astype(str).str.strip()

    # Outer merge on Property + Identity
    merged = prev_df.merge(
        curr_df,
        on=["Property", "Identity"],
        how="outer",
        suffixes=("_prev", "_curr"),
        indicator=True,
    )
    
    if debug:
        print("[DEBUG] After merge on Property + Identity:")
        print(merged["_merge"].value_counts())
        print(f"[DEBUG] Total merged rows: {len(merged)}")

        # Show a few examples of left_only (removed) and right_only (new)
        left_only = merged[merged["_merge"] == "left_only"].head(5)
        right_only = merged[merged["_merge"] == "right_only"].head(5)

        if not left_only.empty:
            print("\n[DEBUG] Sample removed entries (in previous only):")
            print(left_only[["Property", "Identity", "Speed_prev", "Status_prev"]])

        if not right_only.empty:
            print("\n[DEBUG] Sample new entries (in current only):")
            print(right_only[["Property", "Identity", "Speed_curr", "Status_curr"]])
    # Numeric speeds for direction
    merged["Speed_num_prev"] = to_numeric_speed(merged["Speed_prev"])
    merged["Speed_num_curr"] = to_numeric_speed(merged["Speed_curr"])

    # Prepare storage for the change rows
    records: List[Dict[str, Any]] = []

    for idx, row in merged.iterrows():
        change_flags: List[str] = []

        merge_flag = row["_merge"]

        property_name = row["Property"]
        identity = row["Identity"]

        prev_speed = row.get("Speed_prev", np.nan)
        curr_speed = row.get("Speed_curr", np.nan)
        prev_speed_num = row.get("Speed_num_prev", np.nan)
        curr_speed_num = row.get("Speed_num_curr", np.nan)

        prev_mac = row.get("Mac/Serial_prev", "")
        curr_mac = row.get("Mac/Serial_curr", "")

        prev_status = row.get("Status_prev", "")
        curr_status = row.get("Status_curr", "")

        # --- New vs Removed entries ---
        if merge_flag == "left_only":
            # Exists only in previous run -> removed
            change_flags.append("Removed Entry")

        elif merge_flag == "right_only":
            # Exists only in current run -> new
            change_flags.append("New Entry")

        else:  # "both"
            # Speed changes (prefer numeric comparison; only fall back to string if needed)
            if pd.isna(prev_speed) and pd.notna(curr_speed):
                change_flags.append("Speed Changed")
            elif pd.notna(prev_speed) and pd.isna(curr_speed):
                change_flags.append("Speed Changed")
            elif pd.notna(prev_speed) and pd.notna(curr_speed):
                if pd.notna(prev_speed_num) and pd.notna(curr_speed_num):
                    # Numeric comparison
                    if curr_speed_num > prev_speed_num:
                        change_flags.append("Speed Increased")
                    elif curr_speed_num < prev_speed_num:
                        change_flags.append("Speed Decreased")
                    else:
                        # Same numeric speed -> no speed change, even if strings differ
                        pass
                else:
                    # Fallback to string comparison if numeric parse failed
                    ps = str(prev_speed).strip().lower()
                    cs = str(curr_speed).strip().lower()
                    if ps != cs:
                        change_flags.append("Speed Changed")

            # Equipment (Mac/Serial) changes
            if prev_mac and curr_mac and prev_mac != curr_mac:
                change_flags.append("Equipment Changed")

            # Status changes
            if prev_status and curr_status and prev_status != curr_status:
                change_flags.append(f"Status Changed ({prev_status} → {curr_status})")

        if not change_flags:
            # No changes worth recording
            continue

        record = {
            "Property": property_name,
            "Identity": identity,
            "Change": "; ".join(change_flags),
            "Prev Speed": prev_speed,
            "Curr Speed": curr_speed,
            "Prev Speed Num": prev_speed_num,
            "Curr Speed Num": curr_speed_num,
            "Prev Mac/Serial": prev_mac,
            "Curr Mac/Serial": curr_mac,
            "Prev Status": prev_status,
            "Curr Status": curr_status,
        }
        records.append(record)

    changes_df = pd.DataFrame.from_records(records)
    
    if debug:
        print(f"\n[DEBUG] Total previous rows: {len(prev_df)}")
        print(f"[DEBUG] Total current rows:  {len(curr_df)}")
        print(f"[DEBUG] Total changed identities: {len(changes_df)}")
        if not changes_df.empty:
            print("[DEBUG] Change types breakdown:")
            print(changes_df["Change"].value_counts())

    # --- Build summary stats ---

    # Global totals
    total_active_curr = int(
        curr_df[curr_df["Status"].astype(str).str.lower().eq("active")].shape[0]
    )

    # Break down change types
    summary_counts_global = {
        "New Entries": int(changes_df["Change"].str.contains("New Entry").sum()),
        "Removed Entries": int(changes_df["Change"].str.contains("Removed Entry").sum()),
        "Speed Increased": int(changes_df["Change"].str.contains("Speed Increased").sum()),
        "Speed Decreased": int(changes_df["Change"].str.contains("Speed Decreased").sum()),
        "Speed Changed": int(
            changes_df["Change"].str.contains("Speed Changed").sum()
        ),
        "Equipment Changed": int(
            changes_df["Change"].str.contains("Equipment Changed").sum()
        ),
        "Status Changed": int(
            changes_df["Change"].str.contains("Status Changed").sum()
        ),
    }

    # Per-property summary
    per_property_records: List[Dict[str, Any]] = []
    if not changes_df.empty:
        for prop, sub in changes_df.groupby("Property"):
            rec = {
                "Property": prop,
                "New Entries": int(sub["Change"].str.contains("New Entry").sum()),
                "Removed Entries": int(sub["Change"].str.contains("Removed Entry").sum()),
                "Speed Increased": int(
                    sub["Change"].str.contains("Speed Increased").sum()
                ),
                "Speed Decreased": int(
                    sub["Change"].str.contains("Speed Decreased").sum()
                ),
                "Speed Changed": int(
                    sub["Change"].str.contains("Speed Changed").sum()
                ),
                "Equipment Changed": int(
                    sub["Change"].str.contains("Equipment Changed").sum()
                ),
                "Status Changed": int(
                    sub["Change"].str.contains("Status Changed").sum()
                ),
            }
            per_property_records.append(rec)

    per_property_df = pd.DataFrame.from_records(per_property_records).sort_values(
        "Property"
    )

    # Top 5 *properties* by total number of changes
    if not per_property_df.empty:
        per_property_df = per_property_df.copy()
        change_cols = [
            c for c in per_property_df.columns
            if c != "Property"
        ]
        per_property_df["Total Changes"] = per_property_df[change_cols].sum(axis=1)
        top5_props = per_property_df.sort_values(
            "Total Changes", ascending=False
        ).head(5)
    else:
        top5_props = per_property_df.copy()

    summary = {
        "total_active_curr": total_active_curr,
        "global_counts": summary_counts_global,
        "per_property": per_property_df,
        "top5_props": top5_props,
    }

    return changes_df, summary


# --------------------------------------------------------------------------------------
# Excel writing
# --------------------------------------------------------------------------------------

def write_comparison_workbook(
    changes_df: pd.DataFrame,
    summary: Dict[str, Any],
    output_path: Path,
    prev_label: str,
    curr_label: str,
) -> None:
    """
    Write the comparison workbook with:
      - Summary sheet
      - All Changes sheet
      - One sheet per Property with that property's changes
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Precompute sheet names for per-property tabs
    prop_sheet_names: Dict[str, str] = {}
    if not changes_df.empty:
        existing_sheet_names = {"Summary", "All Changes"}
        for prop in sorted(changes_df["Property"].dropna().unique()):
            base = str(prop).strip() or "Unknown"
            # sanitize invalid characters for Excel sheet names
            safe = re.sub(r"[:\\/?*\[\]]", "_", base)
            if len(safe) > 31:
                safe = safe[:31]
            original = safe
            i = 1
            while safe in existing_sheet_names:
                suffix = f"_{i}"
                safe = (original[: 31 - len(suffix)]) + suffix
                i += 1
            existing_sheet_names.add(safe)
            prop_sheet_names[prop] = safe

    with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
        workbook = writer.book

        bold = workbook.add_format({"bold": True})
        bold_center = workbook.add_format({"bold": True, "align": "center"})
        integer_fmt = workbook.add_format({"num_format": "0"})
        italic_fmt = workbook.add_format({"italic": True})
        link_fmt = workbook.add_format({"font_color": "blue", "underline": 1})

        # -------------------------
        # 1) Summary sheet
        # -------------------------
        summary_sheet_name = "Summary"
        ws_summary = workbook.add_worksheet(summary_sheet_name)
        writer.sheets[summary_sheet_name] = ws_summary

        row = 0

        # Title
        ws_summary.write(row, 0, "Speed Audit Comparison Summary", bold)
        row += 2

        # Run labels
        ws_summary.write(row, 0, "Previous Run:", bold)
        ws_summary.write(row, 1, prev_label)
        row += 1
        ws_summary.write(row, 0, "Current Run:", bold)
        ws_summary.write(row, 1, curr_label)
        row += 2

        # Global stats
        ws_summary.write(row, 0, "Global Stats", bold)
        row += 1

        ws_summary.write(row, 0, "Metric", bold_center)
        ws_summary.write(row, 1, "Count", bold_center)
        row += 1

        ws_summary.write(row, 0, "Total Active Identities (Current)")
        ws_summary.write_number(row, 1, summary["total_active_curr"], integer_fmt)
        row += 1

        for metric, count in summary["global_counts"].items():
            ws_summary.write(row, 0, metric)
            ws_summary.write_number(row, 1, count, integer_fmt)
            row += 1

        row += 2
        
        # Rough auto-width for global stats columns
        ws_summary.set_column(0, 0, 40)  # metric names
        ws_summary.set_column(1, 1, 15)  # counts

        row += 2
        # Per-property summary table
        per_prop_df: pd.DataFrame = summary["per_property"]

        ws_summary.write(row, 0, "Per-Property Changes", bold)
        row += 1

        if not per_prop_df.empty:
            start_row = row
            per_prop_df.to_excel(
                writer,
                sheet_name=summary_sheet_name,
                startrow=start_row,
                startcol=0,
                index=False,
            )
            # Header row bold
            ws_summary.set_row(start_row, None, bold)

            # Autofit-ish
            for col_idx, col in enumerate(per_prop_df.columns):
                max_len = max(
                    [len(str(col))]
                    + [len(str(v)) for v in per_prop_df[col].tolist()]
                )
                ws_summary.set_column(col_idx, col_idx, max(10, min(max_len + 2, 40)))

            # Add hyperlinks from Property column to per-property sheets
            if "Property" in per_prop_df.columns and prop_sheet_names:
                prop_col_idx = per_prop_df.columns.get_loc("Property")
                for i, prop in enumerate(per_prop_df["Property"]):
                    sheet_name = prop_sheet_names.get(prop)
                    if not sheet_name:
                        continue
                    excel_row = start_row + 1 + i  # +1 for header row
                    formula = f"=HYPERLINK(\"#'{sheet_name}'!A1\",\"{prop}\")"
                    ws_summary.write_formula(excel_row, prop_col_idx, formula, link_fmt)

            row = start_row + len(per_prop_df.index) + 2
        else:
            ws_summary.write(row, 0, "No changes detected.", italic_fmt)
            row += 2

        # Top 5 changes table
        # Top 5 properties by total changes
        top5_props: pd.DataFrame = summary["top5_props"]
        ws_summary.write(row, 0, "Top 5 Properties by Total Changes", bold)
        row += 1

        if not top5_props.empty:
            cols_to_show = [
                "Property",
                "Total Changes",
                "New Entries",
                "Removed Entries",
                "Speed Increased",
                "Speed Decreased",
                "Speed Changed",
                "Equipment Changed",
                "Status Changed",
            ]
            # Some columns might theoretically be missing, so guard it
            cols_to_show = [c for c in cols_to_show if c in top5_props.columns]

            top5_display = top5_props[cols_to_show].reset_index(drop=True)

            start_row = row
            top5_display.to_excel(
                writer,
                sheet_name=summary_sheet_name,
                startrow=start_row,
                startcol=0,
                index=False,
            )
            ws_summary.set_row(start_row, None, bold)
            for col_idx, col in enumerate(cols_to_show):
                max_len = max(
                    [len(str(col))]
                    + [len(str(v)) for v in top5_display[col].tolist()]
                )
                ws_summary.set_column(col_idx, col_idx, max(10, min(max_len + 2, 40)))
            row = start_row + len(top5_display.index) + 2
        else:
            ws_summary.write(row, 0, "No changes to highlight.", italic_fmt)
            row += 2


        ws_summary.freeze_panes(5, 0)

        # -------------------------
        # 2) All Changes sheet
        # -------------------------
        all_changes_sheet_name = "All Changes"
        if changes_df.empty:
            ws_changes = workbook.add_worksheet(all_changes_sheet_name)
            writer.sheets[all_changes_sheet_name] = ws_changes
            ws_changes.write(0, 0, "No changes detected between runs.", bold)
            return

        display_cols = [
            "Property",
            "Identity",
            "Change",
            "Prev Speed",
            "Curr Speed",
            "Prev Mac/Serial",
            "Curr Mac/Serial",
            "Prev Status",
            "Curr Status",
        ]
        for col in display_cols:
            if col not in changes_df.columns:
                changes_df[col] = np.nan

        changes_display = changes_df[display_cols].copy()

        changes_display.to_excel(
            writer,
            sheet_name=all_changes_sheet_name,
            startrow=0,
            startcol=0,
            index=False,
        )
        ws_changes = writer.sheets[all_changes_sheet_name]

        header_format = workbook.add_format(
            {"bold": True, "bg_color": "#DDDDDD", "border": 1}
        )
        for col_idx, col in enumerate(display_cols):
            ws_changes.write(0, col_idx, display_cols[col_idx], header_format)

        ws_changes.autofilter(0, 0, len(changes_display.index), len(display_cols) - 1)
        ws_changes.freeze_panes(1, 0)

        for col_idx, col in enumerate(display_cols):
            max_len = max(
                [len(str(col))]
                + [len(str(v)) for v in changes_display[col].tolist()]
            )
            ws_changes.set_column(col_idx, col_idx, max(10, min(max_len + 2, 50)))

        # Conditional formats on Change column (col index 2)
        green_fmt = workbook.add_format({"bg_color": "#C6EFCE"})
        red_fmt = workbook.add_format({"bg_color": "#FFC7CE"})
        yellow_fmt = workbook.add_format({"bg_color": "#FFEB9C"})
        blue_fmt = workbook.add_format({"bg_color": "#C9DAF8"})
        gray_fmt = workbook.add_format({"bg_color": "#E6E6E6"})

        last_row = len(changes_display.index)

        ws_changes.conditional_format(
            1,
            2,
            last_row,
            2,
            {
                "type": "text",
                "criteria": "containing",
                "value": "New Entry",
                "format": green_fmt,
            },
        )
        ws_changes.conditional_format(
            1,
            2,
            last_row,
            2,
            {
                "type": "text",
                "criteria": "containing",
                "value": "Removed Entry",
                "format": red_fmt,
            },
        )
        ws_changes.conditional_format(
            1,
            2,
            last_row,
            2,
            {
                "type": "text",
                "criteria": "containing",
                "value": "Speed Increased",
                "format": green_fmt,
            },
        )
        ws_changes.conditional_format(
            1,
            2,
            last_row,
            2,
            {
                "type": "text",
                "criteria": "containing",
                "value": "Speed Decreased",
                "format": yellow_fmt,
            },
        )
        ws_changes.conditional_format(
            1,
            2,
            last_row,
            2,
            {
                "type": "text",
                "criteria": "containing",
                "value": "Equipment Changed",
                "format": blue_fmt,
            },
        )
        ws_changes.conditional_format(
            1,
            2,
            last_row,
            2,
            {
                "type": "text",
                "criteria": "containing",
                "value": "Status Changed",
                "format": gray_fmt,
            },
        )

        # -------------------------
        # 3) Per-property sheets
        # -------------------------
        if not changes_df.empty and prop_sheet_names:
            for prop, sheet_name in prop_sheet_names.items():
                prop_df = changes_display[changes_display["Property"] == prop]
                if prop_df.empty:
                    continue

                prop_df_reset = prop_df.reset_index(drop=True)
                prop_df_reset.to_excel(
                    writer,
                    sheet_name=sheet_name,
                    startrow=0,
                    startcol=0,
                    index=False,
                )
                ws_prop = writer.sheets[sheet_name]

                for col_idx, col in enumerate(display_cols):
                    ws_prop.write(0, col_idx, display_cols[col_idx], header_format)

                ws_prop.autofilter(0, 0, len(prop_df_reset.index), len(display_cols) - 1)
                ws_prop.freeze_panes(1, 0)

                for col_idx, col in enumerate(display_cols):
                    max_len = max(
                        [len(str(col))]
                        + [len(str(v)) for v in prop_df_reset[col].tolist()]
                    )
                    ws_prop.set_column(col_idx, col_idx, max(10, min(max_len + 2, 50)))



# --------------------------------------------------------------------------------------
# CLI handling
# --------------------------------------------------------------------------------------

def build_snapshot_path(snapshot_dir: Path, date_str: str) -> Path:
    """
    Build a snapshot path from a date, using a fixed pattern.

    You can adjust the pattern here to match however we decide to name
    the archived CSVs from the speed-audit pipeline.
    """
    # Example pattern: speed_snapshot_YYYY-MM-DD.csv
    filename = f"speed_snapshot_{date_str}.csv"
    return snapshot_dir / filename


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compare two speed audit runs and produce a comparison workbook."
    )

    parser.add_argument(
        "--prev",
        type=str,
        help="Path to previous run file (CSV or XLSX).",
    )
    parser.add_argument(
        "--curr",
        type=str,
        help="Path to current run file (CSV or XLSX).",
    )

    parser.add_argument(
        "--snapshot-dir",
        type=str,
        default=None,
        help=(
            "Directory containing archived snapshots (CSV). "
            "Used with --prev-date and --curr-date if --prev/--curr are not provided."
        ),
    )
    parser.add_argument(
        "--prev-date",
        type=str,
        help="Date of previous run (YYYY-MM-DD) to build snapshot path if --prev not set.",
    )
    parser.add_argument(
        "--curr-date",
        type=str,
        help="Date of current run (YYYY-MM-DD) to build snapshot path if --curr not set.",
    )

    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help="Output Excel filename for comparison report.",
    )
    
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug information about loads and comparison.",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    debug = args.debug
    
    # Resolve previous and current paths
    prev_path: Optional[Path] = Path(args.prev) if args.prev else None
    curr_path: Optional[Path] = Path(args.curr) if args.curr else None

    snapshot_dir: Optional[Path] = Path(args.snapshot_dir) if args.snapshot_dir else None

    if prev_path is None or curr_path is None:
        # Use snapshot-dir + dates if provided
        if not snapshot_dir or not args.prev_date or not args.curr_date:
            raise SystemExit(
                "You must specify either --prev and --curr, or "
                "--snapshot-dir, --prev-date, and --curr-date."
            )
        if prev_path is None:
            prev_path = build_snapshot_path(snapshot_dir, args.prev_date)
        if curr_path is None:
            curr_path = build_snapshot_path(snapshot_dir, args.curr_date)

    prev_path = prev_path.resolve()
    curr_path = curr_path.resolve()

    # Decide output filename
    if args.output:
        output_path = Path(args.output)
    else:
        # Default: same directory as current run
        curr_stem = curr_path.stem
        parent = curr_path.parent
        output_path = parent / f"Report_Speed_Comparison_{curr_stem}.xlsx"

    print(f"Loading previous run: {prev_path}")
    prev_df = load_run(prev_path, debug=debug)

    print(f"Loading current run:  {curr_path}")
    curr_df = load_run(curr_path, debug=debug)

    print("Comparing runs...")
    changes_df, summary = compare_runs(prev_df, curr_df, debug=debug)

    # Derive labels for summary sheet
    prev_label = prev_path.name
    curr_label = curr_path.name

    print(f"Writing comparison workbook: {output_path}")
    write_comparison_workbook(changes_df, summary, output_path, prev_label, curr_label)

    print("Done.")


if __name__ == "__main__":
    main()
