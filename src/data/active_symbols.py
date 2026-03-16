import os
import re
from functools import lru_cache


ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_ACTIVE_SYMBOLS_PATH = os.path.join(ROOT, "data", "active_symbols.txt")


def _resolve_path(file_path=None):
    return file_path or os.environ.get("ACTIVE_SYMBOLS_FILE") or DEFAULT_ACTIVE_SYMBOLS_PATH


def parse_active_symbols_text(raw_text: str):
    symbols = []
    seen = set()

    for line in raw_text.splitlines():
        line = line.strip()
        if not line:
            continue

        if "\t" in line:
            token = line.split("\t", 1)[0].strip()
        else:
            match = re.match(r"^([A-Z][A-Z0-9]{1,15})\b", line.upper())
            token = match.group(1) if match else ""

        token = token.upper()
        if token and token not in seen:
            seen.add(token)
            symbols.append(token)

    return symbols


@lru_cache(maxsize=8)
def load_active_symbols(file_path=None):
    path = _resolve_path(file_path)
    if not path or not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as handle:
        return parse_active_symbols_text(handle.read())


def get_active_symbol_set(file_path=None):
    return set(load_active_symbols(file_path))


def filter_active_symbols(symbols, file_path=None):
    active = get_active_symbol_set(file_path)
    cleaned = []
    seen = set()

    for symbol in symbols:
        upper = str(symbol).strip().upper()
        if not upper or upper in seen:
            continue
        if active and upper not in active:
            continue
        seen.add(upper)
        cleaned.append(upper)

    return cleaned