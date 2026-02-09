#!/usr/bin/env python3
"""
One-off helper to extract a machine-friendly snapshot CSV
from an existing speed audit Excel workbook.

Usage example:

    python extract_snapshot_from_audit_xlsx.py ^
        --input  "C:\\Users\\eshor\\Downloads\\netops_package\\2025_10_12_Speed_Audit.xlsx" ^
        --output "C:\\Users\\eshor\\Downloads\\netops_package\\speed_snapshot_2025-10-12.csv"

    python extract_snapshot_from_audit_xlsx.py ^
        --input  "C:\\Users\\eshor\\Downloads\\netops_package\\2025_11_12_Speed_Audit.xlsx" ^
        --output "C:\\Users\\eshor\\Downloads\\netops_package\\speed_snapshot_2025-11-12.csv"
"""

import argparse
import re
from pathlib import Path
from typing import List

import numpy as np
import pandas as pd

REQUIRED_COLUMNS = ["Property", "Identity", "Mac/Serial", "Speed", "Status"]


def normalize_colname(col: str) -> str:
    """Normalize a column name to a simple token for matching."""
    return (
        str(col)
        .strip()
        .lower()
        .replace(" ", "")
        .replace("/", "")
        .replace("_", "")
    )


CANONICAL_MAP = {
    "property": "Property",
    "identity": "Identity",
    "macserial": "Mac/Serial",   # Mac/Serial or Serial/Mac
    "serialmac": "Mac/Serial",
    "speed": "Speed",
    "status": "Status",
}


def normalize_speed_text(val):
    """Normalize speed text to '<number> Mbps' where possible."""
    if pd.isna(val):
        return val
    text = str(val).strip()
    # Extract first numeric chunk
    m = re.search(r"([0-9]+\.?[0-9]*)", text)
    if not m:
        return text  # leave as-is if no number found
    num = m.group(1)
    return f"{num} Mbps"


def find_header_row(df: pd.DataFrame) -> int:
    """
    Try to find the row index that contains the 'Identity' column header.
    We assume there is a row where one of the cells == 'Identity' (case-insensitive).
    """
    for idx in df.index:
        row = df.loc[idx]
        if any(str(v).strip().lower() == "identity" for v in row):
            return idx
    raise ValueError("Could not find a header row containing 'Identity'.")


def extract_snapshot_from_workbook(xlsx_path: Path) -> pd.DataFrame:
    """
    Extract a combined snapshot DataFrame from the given audit XLSX.

    For each property sheet:
      - find header row containing "Identity"
      - use that as header
      - normalize column names (Mac/Serial vs Serial/Mac, etc.)
      - keep required columns (Property, Identity, Mac/Serial, Speed, Status)
      - normalize Speed text
    """
    if not xlsx_path.exists():
        raise FileNotFoundError(f"Input workbook not found: {xlsx_path}")

    xls = pd.ExcelFile(xlsx_path)
    print(f"[INFO] Loading workbook: {xlsx_path}")
    print(f"[INFO] Sheets: {xls.sheet_names}")

    frames: List[pd.DataFrame] = []

    for sheet_name in xls.sheet_names:
        sheet_name_stripped = sheet_name.strip()
        lower = sheet_name_stripped.lower()
        if lower in {"table_of_contents", "table of contents", "toc", "summary"}:
            print(f"[INFO] Skipping sheet {sheet_name!r} (TOC/summary)")
            continue

        print(f"[INFO] Processing sheet: {sheet_name!r}")

        df_raw = xls.parse(sheet_name, header=None)

        df_raw = df_raw.dropna(how="all")
        df_raw = df_raw.dropna(axis=1, how="all")

        if df_raw.empty:
            print(f"[WARN] Sheet {sheet_name!r} is empty after cleaning, skipping.")
            continue

        try:
            header_row = find_header_row(df_raw)
        except ValueError as e:
            print(f"[WARN] {e} in sheet {sheet_name!r}, skipping this sheet.")
            continue

        header_values = df_raw.loc[header_row].tolist()
        df_data = df_raw.loc[header_row + 1 :].copy()
        df_data.columns = header_values

        # Normalize column names to canonical names
        original_cols = list(df_data.columns)
        col_rename = {}
        for col in original_cols:
            norm = normalize_colname(col)
            if norm in CANONICAL_MAP:
                col_rename[col] = CANONICAL_MAP[norm]
        if col_rename:
            df_data = df_data.rename(columns=col_rename)

        df_data.columns = [str(c).strip() for c in df_data.columns]
        df_data = df_data.dropna(how="all")

        if df_data.empty:
            print(f"[WARN] Sheet {sheet_name!r} has no data rows under the header, skipping.")
            continue

        if "Property" not in df_data.columns:
            df_data["Property"] = sheet_name_stripped
        else:
            df_data["Property"] = df_data["Property"].fillna(sheet_name_stripped)

        # Keep only required columns if they exist
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in df_data.columns]
        if missing_cols:
            print(
                f"[WARN] Sheet {sheet_name!r} is missing required columns {missing_cols}, "
                f"attempting to continue with available ones."
            )

        keep_cols = [c for c in REQUIRED_COLUMNS if c in df_data.columns]
        df_subset = df_data[keep_cols].copy()

        for col in REQUIRED_COLUMNS:
            if col not in df_subset.columns:
                df_subset[col] = np.nan

        df_subset = df_subset[REQUIRED_COLUMNS]

        for col in ["Property", "Identity", "Mac/Serial", "Status"]:
            df_subset[col] = df_subset[col].astype(str).str.strip()

        # Normalize Speed text for consistency (e.g. 0 -> 0 Mbps)
        df_subset["Speed"] = df_subset["Speed"].apply(normalize_speed_text)

        frames.append(df_subset)

    if not frames:
        raise RuntimeError("No usable property sheets were found. Nothing to export.")

    combined = pd.concat(frames, ignore_index=True)
    print(f"[INFO] Combined snapshot rows: {len(combined)}")
    print(f"[INFO] Columns: {list(combined.columns)}")
    print(f"[INFO] Unique properties in snapshot: {combined['Property'].nunique()}")

    return combined


def main():
    parser = argparse.ArgumentParser(
        description="Extract a snapshot CSV from a speed audit XLSX."
    )
    parser.add_argument(
        "--input", "-i", required=True, help="Path to the Speed_Audit.xlsx file."
    )
    parser.add_argument(
        "--output", "-o", required=True, help="Path to output snapshot CSV."
    )
    args = parser.parse_args()

    xlsx_path = Path(args.input).resolve()
    out_path = Path(args.output).resolve()

    snapshot_df = extract_snapshot_from_workbook(xlsx_path)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    snapshot_df.to_csv(out_path, index=False, encoding="utf-8")
    print(f"[INFO] Snapshot CSV written to: {out_path}")


if __name__ == "__main__":
    main()
