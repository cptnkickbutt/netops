# 🧾 Changelog

## [v0.2.0] - 2025-11-11  
**Unified & Stable Release**

### Highlights
- **Unified Inventory**
  - Replaced legacy CSVs with a single `inventory.csv`.
  - Added flexible role tagging (`firewall`, `export`, `backup`, `web-system`).
  - CLI now supports `--roles`, `--exclude-roles`, and `--dry-list` for precise targeting.

- **ETTP Logic Restored**
  - Reinstated working RouterOS script (`getNeighbors2.rsc`) as primary neighbor source.
  - Automatic filtering of `_AP` interfaces and quote cleanup for identity names.
  - `/ip neighbor` detail retained as fallback only when the script returns no data.

- **CLI & Workflow Improvements**
  - `speed-audit` and `daily-export` now fully Click-based and share unified config.
  - Both commands use the same `.env` credentials and File Server config.
  - Default file server base path: `/mnt/TelcomFS/`
    - `Daily_Exports` for daily-export
    - `Speed_Audit` for speed-audit
  - Improved concurrency handling and progress visualization.

- **Email & Upload Enhancements**
  - Gmail-safe attachment filtering (skips hotspot/executable files if needed).
  - Reliable uploads via SFTP with automatic directory creation.
  - Unified, robust error logging and retry handling.

### Verification
- ✅ All ETTP, DSL, GPON, and CMTS systems process without error.  
- ✅ Email and upload tests confirmed successful for both pipelines.  
- ✅ Roles accurately determine inclusion/exclusion in audits and exports.

### Next Steps
- Add per-row `NeighborSource` override in `inventory.csv` (script vs. ip-neighbor).
- Extend “backup” role automation to verify hotspot and log archives.
- Dynamic concurrency tuning based on system type.

---

**Tag:** `v0.2.0`  
**Date:** 2025-11-11  
**Status:** 🟢 Stable baseline for future development.


# netops 0.2.0 – 2025-11-06

### Added
- New async Telnet transport (`netops.transports.telnet_async`)
- Sync Telnet wrapper for Python 3.13 (uses telnetlib3)
- Modularized transport layer (`ssh`, `sftp`, `telnet`, `base`)

### Changed
- `telnet.py` no longer uses stdlib telnetlib (removed in Py 3.13)
- Updated `__init__.py` re-exports for both sync/async APIs
- Bumped dependency: added `telnetlib3`


## v0.4.0 - 2026-02-13
- Many changes that I failed to keep track of
- Optimizations to existing cli commands
- creation of mass-config that works to a degree, but still needs a lot of work

## v0.4.1 - 2026-02-17

### ✨ Added
- New `pw-gen` CLI command for password generation.
- Supports random policy OR token-based formats (u/l/n/s).
- Generate multiple passwords with configurable length and count.
- Output to screen, TXT, CSV, or XLSX.
- XLSX export now uses the shared Excel helper (TOC + auto-fit columns).
- Ability to append/fill passwords into existing CSV or XLSX files.

### ♻️ Internal
- Reused `security.passwords` helper as standalone CLI utility.
- Integrated Excel helper for consistent workbook formatting.
