# NetOps Runner Scripts

This folder contains standalone operational scripts that run via cron
on the netops-runner container. These are **not** part of the packaged
`netops` CLI, but live alongside the repo for versioning and reuse.

Typical use cases:
- File server automation
- Backups / transfers
- SFTP jobs
- One-off operational tooling

If a script becomes broadly useful, it may later be promoted into
`src/netops/` as an official CLI command.
