
import re, pandas as pd

def safe_sheet_name(name: str) -> str:
    bad = r'[:\\/*?[\]]'
    s = re.sub(bad, "_", name)
    return s[:31] if len(s) > 31 else s

def write_workbook(filename: str, results: list[tuple[str,str,pd.DataFrame]]) -> None:
    with pd.ExcelWriter(filename, engine="xlsxwriter") as writer:
        wb = writer.book
        toc = wb.add_worksheet("Table_of_Contents")
        writer.sheets["Table_of_Contents"] = toc
        toc.set_column("A:A", len("Table of Contents")+4)
        header_fmt = wb.add_format({"bold": True, "font_color": "blue", "font_size": 14})
        link_fmt = wb.add_format({"font_color": "blue", "underline": 1})
        toc.write("A1", "Table of Contents", header_fmt)
        toc.write("A2", "Property"); toc.write("B2", "System")
        toc_row = 3
        for prop, system, df in results:
            sheet = safe_sheet_name(prop)
            df.to_excel(writer, sheet_name=sheet, startrow=2, index=False)
            ws = writer.sheets[sheet]
            for i, col in enumerate(df.columns):
                try:
                    max_len = max(df[col].astype(str).map(len).max(), len(str(col)))
                except Exception:
                    max_len = len(str(col))
                ws.set_column(i, i, max_len + 2)
            ws.write_url("A1", "internal:'Table_of_Contents'!A1", link_fmt, "← Back to TOC")
            toc.write_url(f"A{toc_row}", f"internal:'{sheet}'!A1", link_fmt, prop)
            toc.write(f"B{toc_row}", system)
            toc_row += 1
