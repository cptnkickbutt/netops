from __future__ import annotations

import unittest

from netops.reports.hashlog import (
    build_hashlog_workbook_bytes,
    build_site_hashlog_report,
    parse_hashlog_line,
)


SAMPLE_LINE = (
    "Jun/18/2026 12:13:59 firewall,info hash: hash forward: "
    "in:Bridge_INT out:COLO, src-mac dc:2c:6e:7a:9c:a7, "
    "proto TCP (ACK,PSH), 10.201.73.239:35582->172.66.147.243:80, "
    "NAT (10.201.73.239:35582->199.115.144.222:35582)->172.66.147.243:80, len 167"
)


class HashlogReportTests(unittest.TestCase):
    def test_parse_standard_hashlog_line(self) -> None:
        row = parse_hashlog_line(SAMPLE_LINE, site="Cedars", source_file="Hashlog.0.txt", line_number=1)

        self.assertEqual(row["Parse Status"], "parsed")
        self.assertEqual(row["Timestamp"], "2026-06-18 12:13:59")
        self.assertEqual(row["Site"], "Cedars")
        self.assertEqual(row["Chain"], "forward")
        self.assertEqual(row["In Interface"], "Bridge_INT")
        self.assertEqual(row["Out Interface"], "COLO")
        self.assertEqual(row["Source MAC"], "dc:2c:6e:7a:9c:a7")
        self.assertEqual(row["Protocol"], "TCP")
        self.assertEqual(row["Protocol Flags"], "ACK,PSH")
        self.assertEqual(row["Source IP"], "10.201.73.239")
        self.assertEqual(row["Source Port"], "35582")
        self.assertEqual(row["Destination IP"], "172.66.147.243")
        self.assertEqual(row["Destination Port"], "80")
        self.assertEqual(row["NAT Translated Source IP"], "199.115.144.222")
        self.assertEqual(row["Length"], 167)

    def test_build_site_report_combines_csv_and_summary(self) -> None:
        report = build_site_hashlog_report(
            "Cedars",
            {
                "Hashlog.1.txt": SAMPLE_LINE.replace("12:13:59", "12:14:00").encode(),
                "Hashlog.0.txt": SAMPLE_LINE.encode(),
            },
        )

        self.assertIn("===== Hashlog.0.txt =====", report.combined_text)
        self.assertIn("===== Hashlog.1.txt =====", report.combined_text)
        self.assertEqual(report.log_file_count, 2)
        self.assertEqual(report.summary["Total Lines"], 2)
        self.assertEqual(report.summary["Parsed Lines"], 2)
        self.assertEqual(report.summary["Parse Errors"], 0)
        self.assertEqual(report.summary["First Seen"], "2026-06-18 12:13:59")
        self.assertEqual(report.summary["Last Seen"], "2026-06-18 12:14:00")
        self.assertIn(b"Destination IP", report.csv_bytes)

    def test_build_workbook_bytes(self) -> None:
        report = build_site_hashlog_report("Cedars", {"Hashlog.0.txt": SAMPLE_LINE.encode()})
        workbook = build_hashlog_workbook_bytes([report], day="2026-06-18")

        self.assertTrue(workbook.startswith(b"PK"))
        self.assertGreater(len(workbook), 1000)


if __name__ == "__main__":
    unittest.main()
