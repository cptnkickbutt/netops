from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
import re
from typing import Mapping

import pandas as pd

from ..excel import safe_sheet_name


HASHLOG_COLUMNS = [
    "Site",
    "Source File",
    "Line Number",
    "Timestamp",
    "Topics",
    "Event",
    "Chain",
    "In Interface",
    "Out Interface",
    "Source MAC",
    "Protocol",
    "Protocol Flags",
    "Source IP",
    "Source Port",
    "Destination IP",
    "Destination Port",
    "NAT Original Source IP",
    "NAT Original Source Port",
    "NAT Translated Source IP",
    "NAT Translated Source Port",
    "NAT Destination IP",
    "NAT Destination Port",
    "Length",
    "Parse Status",
    "Raw Line",
]

SUMMARY_COLUMNS = [
    "Site",
    "Hashlog Files",
    "Total Lines",
    "Parsed Lines",
    "Parse Errors",
    "First Seen",
    "Last Seen",
    "Unique Source IPs",
    "Unique Source MACs",
    "Unique Destination IPs",
    "Top Source IP",
    "Top Destination IP",
    "Top Destination Port",
    "Top Protocol",
]

_TS_RE = re.compile(
    r"^(?P<date>[A-Za-z]{3}/\d{1,2}/\d{4})\s+"
    r"(?P<time>\d{1,2}:\d{2}:\d{2})\s+"
    r"(?P<body>.*)$"
)
_BODY_RE = re.compile(r"^(?P<topics>[^:]+):\s*(?P<event>[^:]+):\s*(?P<details>.*)$")
_IPV4 = r"(?:\d{1,3}\.){3}\d{1,3}"
_FLOW_RE = re.compile(
    rf"(?P<src_ip>{_IPV4})(?::(?P<src_port>\d+))?"
    rf"->(?P<dst_ip>{_IPV4})(?::(?P<dst_port>\d+))?"
)
_NAT_RE = re.compile(
    rf"NAT\s+\((?P<orig_ip>{_IPV4})(?::(?P<orig_port>\d+))?"
    rf"->(?P<translated_ip>{_IPV4})(?::(?P<translated_port>\d+))?\)"
    rf"->(?P<dst_ip>{_IPV4})(?::(?P<dst_port>\d+))?",
    flags=re.I,
)


@dataclass(frozen=True)
class SiteHashlogReport:
    site: str
    log_file_count: int
    rows: list[dict[str, object]]
    summary: dict[str, object]
    combined_text: str
    csv_bytes: bytes


def parse_hashlog_line(line: str, *, site: str, source_file: str, line_number: int) -> dict[str, object]:
    row: dict[str, object] = {col: "" for col in HASHLOG_COLUMNS}
    row.update(
        {
            "Site": site,
            "Source File": source_file,
            "Line Number": line_number,
            "Raw Line": line.rstrip("\r\n"),
            "Parse Status": "parsed",
        }
    )

    ts_match = _TS_RE.match(line.strip())
    if not ts_match:
        row["Parse Status"] = "unparsed"
        return row

    raw_timestamp = f"{ts_match.group('date')} {ts_match.group('time')}"
    row["Timestamp"] = _normalize_timestamp(raw_timestamp)

    body = ts_match.group("body")
    body_match = _BODY_RE.match(body)
    if not body_match:
        row["Parse Status"] = "unparsed"
        return row

    topics = body_match.group("topics").strip()
    event = body_match.group("event").strip()
    details = body_match.group("details").strip()

    row["Topics"] = topics
    row["Event"] = event
    if event:
        row["Chain"] = event.split()[-1]

    row["In Interface"] = _find_colon_value(details, "in")
    row["Out Interface"] = _find_colon_value(details, "out")
    row["Source MAC"] = _find_space_value(details, "src-mac")

    proto_match = re.search(r"(?:^|,\s*)proto\s+(?P<proto>\S+)(?:\s+\((?P<flags>[^)]*)\))?", details, flags=re.I)
    if proto_match:
        row["Protocol"] = proto_match.group("proto") or ""
        row["Protocol Flags"] = proto_match.group("flags") or ""

    flow_match = _FLOW_RE.search(details)
    if flow_match:
        row["Source IP"] = flow_match.group("src_ip") or ""
        row["Source Port"] = flow_match.group("src_port") or ""
        row["Destination IP"] = flow_match.group("dst_ip") or ""
        row["Destination Port"] = flow_match.group("dst_port") or ""

    nat_match = _NAT_RE.search(details)
    if nat_match:
        row["NAT Original Source IP"] = nat_match.group("orig_ip") or ""
        row["NAT Original Source Port"] = nat_match.group("orig_port") or ""
        row["NAT Translated Source IP"] = nat_match.group("translated_ip") or ""
        row["NAT Translated Source Port"] = nat_match.group("translated_port") or ""
        row["NAT Destination IP"] = nat_match.group("dst_ip") or ""
        row["NAT Destination Port"] = nat_match.group("dst_port") or ""

    len_match = re.search(r"(?:^|,\s*)len\s+(?P<length>\d+)", details, flags=re.I)
    if len_match:
        row["Length"] = int(len_match.group("length"))

    return row


def build_site_hashlog_report(site: str, hash_logs: Mapping[str, bytes]) -> SiteHashlogReport:
    rows: list[dict[str, object]] = []
    combined_parts: list[str] = []

    for source_file, raw_data in sorted(hash_logs.items(), key=lambda item: _hashlog_sort_key(item[0])):
        text = raw_data.decode("utf-8-sig", "replace")
        combined_parts.append(f"===== {source_file} =====\n{text.rstrip()}\n")

        for line_number, line in enumerate(text.splitlines(), start=1):
            if not line.strip():
                continue
            rows.append(
                parse_hashlog_line(
                    line,
                    site=site,
                    source_file=source_file,
                    line_number=line_number,
                )
            )

    combined_text = "\n".join(combined_parts).rstrip()
    if combined_text:
        combined_text += "\n"

    df = pd.DataFrame(rows, columns=HASHLOG_COLUMNS)
    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")

    return SiteHashlogReport(
        site=site,
        log_file_count=len(hash_logs),
        rows=rows,
        summary=_build_summary(site, len(hash_logs), rows),
        combined_text=combined_text,
        csv_bytes=csv_bytes,
    )


def build_hashlog_workbook_bytes(reports: list[SiteHashlogReport], *, day: str) -> bytes:
    output = BytesIO()
    used_sheet_names = {"summary"}

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        workbook = writer.book
        title_fmt = workbook.add_format({"bold": True, "font_size": 14})
        note_fmt = workbook.add_format({"italic": True, "font_color": "#666666"})

        summary_df = pd.DataFrame([report.summary for report in reports], columns=SUMMARY_COLUMNS)
        summary_df.to_excel(writer, sheet_name="Summary", startrow=2, index=False)
        summary_ws = writer.sheets["Summary"]
        summary_ws.write(0, 0, f"{day} Hashlog Summary", title_fmt)
        summary_ws.write(1, 0, "Per-site hashlog counts, parse quality, and top talkers.", note_fmt)
        _format_worksheet(summary_ws, summary_df, startrow=2)

        all_rows = [row for report in reports for row in report.rows]
        all_df = pd.DataFrame(all_rows, columns=HASHLOG_COLUMNS)
        _write_dataframe_sheet(writer, "All_Hashlogs", all_df, used_sheet_names)

        for report in reports:
            if not report.rows:
                continue
            sheet_name = _unique_sheet_name(report.site, used_sheet_names)
            site_df = pd.DataFrame(report.rows, columns=HASHLOG_COLUMNS)
            _write_dataframe_sheet(writer, sheet_name, site_df, used_sheet_names)

    return output.getvalue()


def _normalize_timestamp(value: str) -> str:
    try:
        parsed = datetime.strptime(value, "%b/%d/%Y %H:%M:%S")
        return parsed.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value


def _find_colon_value(text: str, key: str) -> str:
    match = re.search(rf"(?<!\S){re.escape(key)}:(?P<value>[^,\s]+)", text, flags=re.I)
    return match.group("value") if match else ""


def _find_space_value(text: str, key: str) -> str:
    match = re.search(rf"(?:^|,\s*){re.escape(key)}\s+(?P<value>[^,\s]+)", text, flags=re.I)
    return match.group("value") if match else ""


def _hashlog_sort_key(name: str) -> tuple[int, int, str]:
    match = re.search(r"(?i)(hashlog|log)\.(\d+)\.txt$", name)
    if not match:
        return (10_000, 99, name.lower())
    prefix_order = 0 if match.group(1).lower() == "hashlog" else 1
    return (int(match.group(2)), prefix_order, name.lower())


def _build_summary(site: str, log_file_count: int, rows: list[dict[str, object]]) -> dict[str, object]:
    parsed_rows = [row for row in rows if row.get("Parse Status") == "parsed"]
    timestamps = sorted(str(row.get("Timestamp") or "") for row in parsed_rows if row.get("Timestamp"))

    return {
        "Site": site,
        "Hashlog Files": log_file_count,
        "Total Lines": len(rows),
        "Parsed Lines": len(parsed_rows),
        "Parse Errors": len(rows) - len(parsed_rows),
        "First Seen": timestamps[0] if timestamps else "",
        "Last Seen": timestamps[-1] if timestamps else "",
        "Unique Source IPs": _unique_count(parsed_rows, "Source IP"),
        "Unique Source MACs": _unique_count(parsed_rows, "Source MAC"),
        "Unique Destination IPs": _unique_count(parsed_rows, "Destination IP"),
        "Top Source IP": _top_value(parsed_rows, "Source IP"),
        "Top Destination IP": _top_value(parsed_rows, "Destination IP"),
        "Top Destination Port": _top_value(parsed_rows, "Destination Port"),
        "Top Protocol": _top_value(parsed_rows, "Protocol"),
    }


def _unique_count(rows: list[dict[str, object]], column: str) -> int:
    return len({str(row.get(column) or "").strip() for row in rows if str(row.get(column) or "").strip()})


def _top_value(rows: list[dict[str, object]], column: str) -> str:
    values = [str(row.get(column) or "").strip() for row in rows if str(row.get(column) or "").strip()]
    if not values:
        return ""
    value, count = Counter(values).most_common(1)[0]
    return f"{value} ({count})"


def _write_dataframe_sheet(
    writer: pd.ExcelWriter,
    sheet_name: str,
    df: pd.DataFrame,
    used_sheet_names: set[str],
) -> None:
    display_df = _excel_safe_frame(df)
    display_df.to_excel(writer, sheet_name=sheet_name, index=False)
    used_sheet_names.add(sheet_name.lower())
    ws = writer.sheets[sheet_name]
    _format_worksheet(ws, display_df)

    if len(df) > len(display_df):
        ws.write(len(display_df) + 2, 0, f"Workbook sheet truncated at {len(display_df):,} rows; use the per-site CSV for full data.")


def _excel_safe_frame(df: pd.DataFrame) -> pd.DataFrame:
    max_data_rows = 1_048_575
    if len(df) <= max_data_rows:
        return df
    return df.iloc[:max_data_rows].copy()


def _format_worksheet(ws, df: pd.DataFrame, *, startrow: int = 0) -> None:
    header_row = startrow
    first_data_row = header_row + 1
    last_row = header_row + max(len(df), 1)
    last_col = max(len(df.columns) - 1, 0)

    ws.freeze_panes(first_data_row, 0)
    ws.autofilter(header_row, 0, last_row, last_col)

    for col_idx, column in enumerate(df.columns):
        width = len(str(column)) + 2
        if not df.empty:
            try:
                width = max(width, int(df[column].astype(str).map(len).max()) + 2)
            except Exception:
                pass
        if column == "Raw Line":
            width = min(max(width, 40), 120)
        else:
            width = min(max(width, 10), 42)
        ws.set_column(col_idx, col_idx, width)


def _unique_sheet_name(name: str, used_sheet_names: set[str]) -> str:
    base = safe_sheet_name(name).strip() or "Site"
    candidate = base
    suffix = 2

    while candidate.lower() in used_sheet_names:
        suffix_text = f"_{suffix}"
        candidate = f"{base[:31 - len(suffix_text)]}{suffix_text}"
        suffix += 1

    return candidate
