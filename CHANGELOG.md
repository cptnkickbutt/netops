# netops 0.2.0 – 2025-11-06

### Added
- New async Telnet transport (`netops.transports.telnet_async`)
- Sync Telnet wrapper for Python 3.13 (uses telnetlib3)
- Modularized transport layer (`ssh`, `sftp`, `telnet`, `base`)

### Changed
- `telnet.py` no longer uses stdlib telnetlib (removed in Py 3.13)
- Updated `__init__.py` re-exports for both sync/async APIs
- Bumped dependency: added `telnetlib3`
