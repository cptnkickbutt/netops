# netops

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/cptnkickbutt/netops?sort=semver&display_name=tag)](https://github.com/cptnkickbutt/netops/releases/latest)
![Release date](https://img.shields.io/github/release-date/cptnkickbutt/netops)
![Commits since latest release](https://img.shields.io/github/commits-since/cptnkickbutt/netops/latest)
[![semantic-release: automated](https://img.shields.io/badge/semantic--release-automated-brightgreen.svg)](https://github.com/semantic-release/semantic-release)
![Build status](https://github.com/cptnkickbutt/netops/actions/workflows/semantic-release.yml/badge.svg)
![License](https://img.shields.io/github/license/cptnkickbutt/netops)

`netops` is a modular Python toolkit for network operations automation.

It powers scripted and CLI workflows for tasks like:

- parallel SSH/Telnet polling of routers, DSLAMs, and other network devices
- VLAN and configuration audits
- automated exports, uploads, and reporting, including `daily-export` and `speed-audit`
- integration with FreeRADIUS, Omada SDN, and other management systems

Built for performance and clarity, the async transports allow hundreds of concurrent sessions while the CLI provides single-command access for daily operations.

## Quick Install

```bash
git clone https://github.com/cptnkickbutt/netops.git
cd netops
pip install -e .
```

## Generated Files

Generated reports, zips, snippets, and helper outputs default to the local `files/` directory. Explicit paths still win: if you pass `--output`, `--out`, `--core-peers-file`, or another output option, the command writes to the path you provided.

Set `NETOPS_FILES_DIR` to use a different default generated-files directory.

## Commands

### `netops daily-export`

Collect RouterOS exports, backup logs, and hotspot assets for inventory rows matching the selected roles.

Typical usage:

```bash
netops daily-export
netops daily-export --testing
netops daily-export --single
```

Notes:

- Hashlog reporting is enabled by default, including in `--testing` mode.
- The export zip includes original per-site hashlog files, plus `combined_hashlog.txt` and `hashlog.csv` under each site's `hash-log/` folder when hashlogs are present.
- The export zip also includes a root-level `{date}_Hashlog_Report.xlsx` workbook with a summary, an all-sites sheet, and one sheet per site with parsed hashlog rows.
- Use `--no-hashlog-report` to skip the combined text, CSV, and XLSX hashlog report artifacts.
- Remote daily export cleanup is enabled by default after upload. It keeps 180 days of `*_Daily_Exports.zip` files and only deletes filenames matching the daily export pattern.
- In `--testing` mode, remote cleanup runs as a dry run and logs what it would delete. Use `--cleanup-days` to change the retention window or `--no-cleanup` to skip it.

### `netops wireless-info`

Pull Wi-Fi/password/queue information from MikroTik neighbor devices for one `System=ETTP` inventory row.

Typical usage:

```bash
netops wireless-info --list-sites
netops wireless-info --site "525" --format xlsx
netops wireless-info --site "525" --format csv
```

Notes:

- `--site` matches the `Site` column, and also matches an optional `Abbr` column when present.
- Output defaults to XLSX under `files/`. Use `--format csv` for CSV.
- XLSX output includes `Wireless_Info` as the merged/customer-facing sheet and `Raw_Devices` as a troubleshooting sheet with every polled neighbor device.
- CSV output contains only the merged/customer-facing `Wireless_Info` rows.
- The legacy `vlan-id` column is not included by default. Add it only when needed with `--include-vlan-id`.
- Neighbor collection defaults to `getNeighbors2.rsc` when present, with an embedded equivalent as fallback.
- `--interface-filter Modem` is applied by default. This captures both modem/router interfaces and AP modem interfaces. Use `--all-neighbors` for troubleshooting.
- Merging is done after polling: `AP_Modem`-style interfaces supply wireless data, plain `Modem` interfaces supply queue/speed data, and matching is attempted by normalized identity first (`TC_4B_424_AP` and `TC_4B_AP_424` both match `TC_4B_424`) with normalized interface as a fallback.
- hAP-style single-device rows can provide both queue and wireless data when no paired AP is found.
