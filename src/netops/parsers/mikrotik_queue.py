
import shlex, re

def parse_queue_export_verbose(text: str) -> list[dict]:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    items, cur = [], []
    for ln in lines:
        if ln.startswith('/'):
            continue
        if ln.startswith('add '):
            if cur:
                items.append(' '.join(cur)); cur = []
            cur.append(ln[4:].strip())
        else:
            if cur:
                cur.append(ln)
    if cur:
        items.append(' '.join(cur))

    out = []
    for rule in items:
        toks = shlex.split(rule)
        d = {}
        for t in toks:
            if '=' in t:
                k, v = t.split('=', 1)
                d[k] = v
            else:
                d[t] = True
        out.append(d)
    return out

def rate_from_rule(rule: dict) -> str | None:
    if str(rule.get('disabled','no')).lower() in {'yes','true','on','1'}:
        return None
    if (qf := rule.get('queue')):
        left = qf.split('/')[0]
        head = left.split('_')[0]
        m = re.fullmatch(r'(\d+)([MK])?', head, flags=re.I)
        if m:
            num, unit = m.groups()
            return f"{num} Mbps" if (not unit or unit.lower()=='m') else f"{num} Kbps"
    if (ml := rule.get('max-limit')):
        left = ml.split('/')[0]
        m = re.fullmatch(r'(\d+)([MK])', left, flags=re.I)
        if m:
            num, unit = m.groups()
            return f"{num} Mbps" if unit.lower()=='m' else f"{num} Kbps"
    return None
