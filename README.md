# 🧠 netops

[![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/cptnkickbutt/netops?sort=semver&display_name=tag)](https://github.com/cptnkickbutt/netops/releases/latest)
![Release date](https://img.shields.io/github/release-date/cptnkickbutt/netops)
![Commits since latest release](https://img.shields.io/github/commits-since/cptnkickbutt/netops/latest)
[![semantic-release: automated](https://img.shields.io/badge/semantic--release-automated-brightgreen.svg)](https://github.com/semantic-release/semantic-release)
![Build status](https://github.com/cptnkickbutt/netops/actions/workflows/semantic-release.yml/badge.svg)
![License](https://img.shields.io/github/license/cptnkickbutt/netops)

---

**`netops`** is a modular Python toolkit for network operations automation.  
It powers scripted and CLI workflows for tasks like:  
- parallel SSH/Telnet polling of routers, DSLAMs, and other network devices  
- VLAN and configuration audits  
- automated exports, uploads, and reporting (e.g., *daily_export*, *speed_audit*)  
- integration with FreeRADIUS, Omada SDN, and other management systems  

Built for performance and clarity — the async transports allow hundreds of concurrent sessions,  
while the CLI provides single-command access for your daily operations.

---

## 🚀 Quick install

```bash
git clone https://github.com/cptnkickbutt/netops.git
cd netops
pip install -e .
