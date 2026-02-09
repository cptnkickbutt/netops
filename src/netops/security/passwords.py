# src/netops/security/passwords.py

from __future__ import annotations
import secrets
import string
from dataclasses import dataclass
from typing import Optional, Sequence

DEFAULT_SYMBOLS = "@#$%&*+-=?^~"  # pick your org-safe defaults
DIGITS_0_9 = string.digits
DIGITS_1_9 = "123456789"

_TOKEN_MAP = {
    "u": string.ascii_uppercase,
    "l": string.ascii_lowercase,
    "n": DIGITS_0_9,   # can override with digits set
    "s": DEFAULT_SYMBOLS,  # can override with symbols set
}

@dataclass(frozen=True)
class PasswordPolicy:
    length: int = 16
    use_upper: bool = True
    use_lower: bool = True
    use_digits: bool = True
    use_symbols: bool = True
    symbols: str = DEFAULT_SYMBOLS
    digits: str = DIGITS_0_9

def parse_format(fmt: str) -> list[str]:
    """
    Accepts:
      "u,l,n,n,n,n,n,s" or "ulnnnnns" (commas/spaces ignored)
    Returns token list like ["u","l","n","n","n","n","n","s"].
    """
    cleaned = "".join(ch for ch in fmt.lower() if ch in "ulns")
    if not cleaned:
        raise ValueError("Format contained no valid tokens (u/l/n/s).")
    return list(cleaned)

def generate_password(
    *,
    policy: Optional[PasswordPolicy] = None,
    fmt: Optional[str] = None,
    symbols: Optional[str] = None,
    digits: Optional[str] = None,
) -> str:
    """
    If fmt is provided, generates exactly len(tokens) characters in that order.
    Otherwise generates a random password of policy.length containing at least
    one of each enabled class.
    """
    policy = policy or PasswordPolicy()
    sym = symbols if symbols is not None else policy.symbols
    dig = digits if digits is not None else policy.digits

    if fmt:
        tokens = parse_format(fmt)
        out = []
        for t in tokens:
            if t == "u":
                pool = string.ascii_uppercase
            elif t == "l":
                pool = string.ascii_lowercase
            elif t == "n":
                pool = dig
            elif t == "s":
                pool = sym
            else:
                raise ValueError(f"Unknown token: {t}")
            out.append(secrets.choice(pool))
        return "".join(out)

    pools = []
    if policy.use_upper:
        pools.append(string.ascii_uppercase)
    if policy.use_lower:
        pools.append(string.ascii_lowercase)
    if policy.use_digits:
        pools.append(dig)
    if policy.use_symbols:
        pools.append(sym)
    if not pools:
        raise ValueError("Policy enables no character classes.")

    # Ensure at least one char from each enabled class
    out = [secrets.choice(p) for p in pools]

    # Fill remainder from combined pool
    all_chars = "".join(pools)
    for _ in range(policy.length - len(out)):
        out.append(secrets.choice(all_chars))

    secrets.SystemRandom().shuffle(out)
    return "".join(out)
