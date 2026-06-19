from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import unittest

from netops.config import FileSvrCfg
from netops import uploader


@dataclass
class _Entry:
    filename: str


class _FakeSftp:
    def __init__(self, names: list[str]) -> None:
        self.names = names
        self.removed: list[str] = []

    def listdir_attr(self, remote_dir: str) -> list[_Entry]:
        return [_Entry(name) for name in self.names]

    def remove(self, remote_file: str) -> None:
        self.removed.append(remote_file)

    def close(self) -> None:
        pass


class _FakeSsh:
    def __init__(self, sftp: _FakeSftp) -> None:
        self.sftp = sftp

    def open_sftp(self) -> _FakeSftp:
        return self.sftp

    def close(self) -> None:
        pass


class DailyExportCleanupTests(unittest.TestCase):
    def setUp(self) -> None:
        today = datetime.now().date()
        self.old_name = f"{today - timedelta(days=181):%Y-%m-%d}_Daily_Exports.zip"
        self.old_safe_name = f"{today - timedelta(days=240):%Y-%m-%d}_Daily_Exports_SAFE.zip"
        self.recent_name = f"{today - timedelta(days=10):%Y-%m-%d}_Daily_Exports.zip"
        self.other_name = f"{today - timedelta(days=365):%Y-%m-%d}_Other_Report.zip"
        self.cfg = FileSvrCfg(
            host="files.example",
            port=22,
            user="tester",
            password="secret",
            base_dir="/base/",
        )

    def test_cleanup_dry_run_selects_only_expired_daily_export_zips(self) -> None:
        sftp = _FakeSftp([self.old_name, self.old_safe_name, self.recent_name, self.other_name])
        original = uploader.make_ssh_client
        uploader.make_ssh_client = lambda *args, **kwargs: _FakeSsh(sftp)
        try:
            result = uploader.cleanup_old_daily_exports(
                self.cfg,
                subdir="Daily_Exports_and_Hash_Logs",
                retention_days=180,
                dry_run=True,
            )
        finally:
            uploader.make_ssh_client = original

        self.assertEqual(result.scanned, 4)
        self.assertEqual(result.matched, 3)
        self.assertEqual(len(result.expired), 2)
        self.assertEqual(result.deleted, [])
        self.assertEqual(sftp.removed, [])
        self.assertTrue(any(path.endswith(self.old_name) for path in result.expired))
        self.assertTrue(any(path.endswith(self.old_safe_name) for path in result.expired))
        self.assertFalse(any(path.endswith(self.recent_name) for path in result.expired))
        self.assertFalse(any(path.endswith(self.other_name) for path in result.expired))

    def test_cleanup_live_deletes_expired_daily_export_zips(self) -> None:
        sftp = _FakeSftp([self.old_name, self.recent_name])
        original = uploader.make_ssh_client
        uploader.make_ssh_client = lambda *args, **kwargs: _FakeSsh(sftp)
        try:
            result = uploader.cleanup_old_daily_exports(
                self.cfg,
                subdir="Daily_Exports_and_Hash_Logs",
                retention_days=180,
                dry_run=False,
            )
        finally:
            uploader.make_ssh_client = original

        self.assertEqual(len(result.expired), 1)
        self.assertEqual(result.deleted, result.expired)
        self.assertEqual(sftp.removed, result.expired)


if __name__ == "__main__":
    unittest.main()
