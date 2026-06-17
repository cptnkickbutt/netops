# src/netops/security/passwords.py

from __future__ import annotations

import secrets
import string
from dataclasses import dataclass
from typing import Optional

DEFAULT_SYMBOLS = "@#$%&*+-=?^~"  # pick your org-safe defaults
DIGITS_0_9 = string.digits
DIGITS_1_9 = "123456789"

_TOKEN_MAP = {
    "u": string.ascii_uppercase,
    "l": string.ascii_lowercase,
    "n": DIGITS_0_9,   # can override with digits set
    "s": DEFAULT_SYMBOLS,  # can override with symbols set
}

# Intentionally short, plain, and non-offensive. These are building blocks for
# memorable shared secrets, not full passphrases by themselves.
DEFAULT_ADJECTIVES = (
    "amber", "brave", "bright", "calm", "clever", "copper", "crisp", "daring",
    "eager", "fancy", "fast", "fierce", "frosty", "gentle", "golden", "grand",
    "happy", "honest", "jolly", "kind", "lively", "lucky", "mighty", "modern",
    "noble", "proud", "quiet", "rapid", "royal", "sharp", "silver", "solid",
    "swift", "tidy", "vivid", "wild", "wise", "zesty",
)

DEFAULT_NOUNS = (
    "anchor", "badger", "beacon", "bison", "bridge", "canyon", "cedar", "comet",
    "copper", "falcon", "forest", "harbor", "horizon", "jaguar", "lantern", "maple",
    "meadow", "meteor", "monarch", "otter", "panther", "phoenix", "quartz", "raven",
    "river", "rocket", "summit", "thunder", "tiger", "valley", "voyager", "willow",
)

# Netgear-ish substitutions. Kept org-safe for CLIs, CSVs, and RADIUS shared secrets.
DEFAULT_SUBSTITUTIONS = {
    "a": "@",
    "e": "3",
    "i": "!",
    "o": "0",
    "s": "$",
    "t": "7",
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


@dataclass(frozen=True)
class MemorablePasswordPolicy:
    min_numbers: int = 3
    max_numbers: int = 5
    substitution_chance: float = 0.45
    uppercase_chance: float = 0.35
    separator: str = ""
    adjectives: tuple[str, ...] = DEFAULT_ADJECTIVES
    nouns: tuple[str, ...] = DEFAULT_NOUNS
    substitutions: dict[str, str] | None = None


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


def _validate_chance(name: str, value: float) -> None:
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be between 0 and 1.")


def _randomize_word(word: str, *, uppercase_chance: float, substitution_chance: float, substitutions: dict[str, str]) -> str:
    out: list[str] = []
    for ch in word.lower():
        mapped = substitutions.get(ch)
        if mapped and secrets.randbelow(10_000) / 10_000 < substitution_chance:
            out.append(mapped)
            continue
        if ch.isalpha() and secrets.randbelow(10_000) / 10_000 < uppercase_chance:
            out.append(ch.upper())
        else:
            out.append(ch)
    return "".join(out)


def generate_memorable_password(policy: Optional[MemorablePasswordPolicy] = None) -> str:
    """
    Generate a Netgear-ish memorable password:
      adjective + noun + random digits, with randomized capitalization and substitutions.

    Example shape, not exact output:
      Br@veR0cket4827
    """
    policy = policy or MemorablePasswordPolicy()
    substitutions = policy.substitutions or DEFAULT_SUBSTITUTIONS

    if not policy.adjectives:
        raise ValueError("Memorable policy has no adjectives.")
    if not policy.nouns:
        raise ValueError("Memorable policy has no nouns.")
    if policy.min_numbers < 0 or policy.max_numbers < 0:
        raise ValueError("Number counts must be >= 0.")
    if policy.max_numbers < policy.min_numbers:
        raise ValueError("max_numbers must be >= min_numbers.")
    _validate_chance("uppercase_chance", policy.uppercase_chance)
    _validate_chance("substitution_chance", policy.substitution_chance)

    adjective = secrets.choice(policy.adjectives)
    noun = secrets.choice(policy.nouns)
    num_count = policy.min_numbers + secrets.randbelow(policy.max_numbers - policy.min_numbers + 1)
    numbers = "".join(secrets.choice(DIGITS_0_9) for _ in range(num_count))

    left = _randomize_word(
        adjective,
        uppercase_chance=policy.uppercase_chance,
        substitution_chance=policy.substitution_chance,
        substitutions=substitutions,
    )
    right = _randomize_word(
        noun,
        uppercase_chance=policy.uppercase_chance,
        substitution_chance=policy.substitution_chance,
        substitutions=substitutions,
    )
    return f"{left}{policy.separator}{right}{numbers}"


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
