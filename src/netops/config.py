
import os, pandas as pd
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

REQUIRED = ["Property","IP","User","PW","System","Access"]

def load_env() -> Optional[Path]:
    explicit = os.environ.get("NETOPS_DOTENV") or os.environ.get("SPEED_AUDIT_DOTENV")
    candidates = []
    if explicit:
        candidates.append(Path(explicit))
    here = Path.cwd()
    candidates += [here/".env", here.parent/".env"]
    for p in candidates:
        if p and p.exists():
            load_dotenv(p)
            return p
    return None

def load_inventory(csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    df.columns = df.columns.astype(str).str.replace("\ufeff","",regex=False).str.strip()
    missing = [c for c in REQUIRED if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    for c in df.columns:
        if df[c].dtype == object:
            df[c] = df[c].astype(str).str.strip().str.strip('"').str.strip("'")
    df["Property"] = df["Property"].astype(str)
    return df

def resolve_env(name: str) -> str:
    val = os.getenv(name.strip())
    if not val:
        raise RuntimeError(f"Missing env var {name!r}")
    return val
