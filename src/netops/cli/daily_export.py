# netops/tasks/daily_export.py
import csv, time, socket
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Tuple
from netops import logging as nlog
from netops.config import MailCfg, FileSvrCfg, resolve_env_or_literal
from netops.transports import make_ssh_client, ssh_exec, sftp_listdir, ensure_dir_over_ssh
from netops.storage import zip_path, safe_rmtree, safe_unlink
from netops.emailer import send_email_with_attachment

def _read_properties(csv_path: Path) -> List[Tuple[str,str,str,str]]:
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        rd = csv.reader(f); next(rd, None)
        return [(r[0].strip(), r[1].strip(), r[2].strip(), r[3].strip()) for r in rd if len(r) >= 4]

def _fetch_one(out_dir: Path, date_str: str, location: str, ip: str, user_env: str, pw_env: str, log, retries=2):
    user = resolve_env_or_literal(user_env)
    pw   = resolve_env_or_literal(pw_env)
    for attempt in range(1, retries+2):
        try:
            log.info("(%s) connect %s@%s attempt %d", location, user, ip, attempt)
            c = make_ssh_client(ip, 22, user, pw, timeout=10)

            out, err, rc = ssh_exec(c, "/export")
            (out_dir / f"{location}_export_{date_str}.txt").write_text(out, encoding="utf-8")

            s = c.open_sftp()
            names = sftp_listdir(s, ".")
            idxs = sorted(int(n[4:-4]) for n in names if n.startswith("log.") and n.endswith(".txt") and n[4:-4].isdigit())
            for i in idxs:
                remote = f"log.{i}.txt"
                local  = out_dir / f"{location}_hash_log_{i}_{date_str}.csv"
                s.get(remote, str(local))
                try: ssh_exec(c, f"/file remove [ find name={remote} ]")
                except Exception: pass
            s.close(); c.close()
            return True, "ok"
        except (socket.error, TimeoutError) as e:
            log.warning("(%s) ssh error: %s", location, e)
            if attempt <= retries: time.sleep(attempt*2); continue
            (out_dir / f"{location}_export_{date_str}_ERROR.txt").write_text(f"SSH failed: {e}", encoding="utf-8")
            return False, f"SSH failed: {e}"
        except Exception as e:
            log.error("(%s) unexpected: %s", location, e)
            (out_dir / f"{location}_export_{date_str}_ERROR.txt").write_text(f"Unexpected: {e}", encoding="utf-8")
            return False, f"unexpected: {e}"

def _upload_zip(zip_path: Path, cfg: FileSvrCfg, log):
    pw = cfg.pw_value or resolve_env_or_literal(cfg.pw_key)
    c = make_ssh_client(cfg.host, cfg.port, cfg.user, pw, timeout=10)
    remote_dir = "/mnt/TelcomFS/Daily_Export_and_Hash_Logs/"
    ensure_dir_over_ssh(c, remote_dir)
    s = c.open_sftp()
    s.put(str(zip_path), f"{remote_dir}{zip_path.name}")
    s.close(); c.close()
    log.info("Uploaded %s", zip_path.name)

def run_daily_export(args) -> int:
    log = nlog.setup(args.log_level)
    today = datetime.now().strftime("%Y-%m-%d")
    out_dir = Path(f"{today}_Daily_Exports"); out_dir.mkdir(parents=True, exist_ok=True)

    props = _read_properties(Path(args.properties))
    if args.only:
        allow = {p.strip() for p in args.only.split(",") if p.strip()}
        props = [p for p in props if p[0] in allow]
    if not props:
        log.error("No properties found."); return 2

    workers = max(1, int(args.workers or 1))
    results = []
    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = [ex.submit(_fetch_one, out_dir, today, n, ip, ue, pe, log) for (n, ip, ue, pe) in props]
            for f in as_completed(futs): results.append(f.result())
    else:
        for (n, ip, ue, pe) in props: results.append(_fetch_one(out_dir, today, n, ip, ue, pe, log))

    ok = sum(1 for s,_ in results if s)
    log.info("Completed: %d ok / %d total", ok, len(results))

    zip_p = zip_path(out_dir)

    if not args.no_upload:
        try: _upload_zip(zip_p, FileSvrCfg(), log)
        except Exception as e: log.error("Upload failed: %s", e)

    if not args.no_email:
        rcpts = [e.strip() for e in (args.recipients or "").split(",") if e.strip()] or \
                ["eshortt@telcomsys.net", "jedwards@ripheat.com", "rkammerman@ripheat.com"]
        m = MailCfg()
        try:
            send_email_with_attachment(m.sender_email, m.sender_password, m.host, m.port,
                                       rcpts, f"{today} Daily Exports",
                                       f"Attached firewall exports for {today}", zip_p)
        except Exception as e:
            log.error("Email failed: %s", e)

    if not args.keep:
        from netops.storage import safe_rmtree, safe_unlink
        safe_rmtree(out_dir); safe_unlink(zip_p)
    return 0
