"""Các hàm làm sạch chữ dùng chung."""

import re
import unicodedata
from urllib.parse import urldefrag

_ZERO_WIDTH = re.compile(r"[​-‍﻿]")
_SPACES = re.compile(r"\s+")


def clean_text(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = " ".join(str(v) for v in value if v)
    value = unicodedata.normalize("NFC", str(value))
    value = _ZERO_WIDTH.sub("", value)
    value = _SPACES.sub(" ", value).strip()
    return value or None


def clean_multiline(value):
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        value = "\n".join(str(v) for v in value if v)
    value = unicodedata.normalize("NFC", str(value))
    value = _ZERO_WIDTH.sub("", value)
    lines = [_SPACES.sub(" ", line).strip() for line in value.splitlines()]
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(lines)).strip()
    return text or None


def clean_url(url):
    return urldefrag(url)[0] if url else None
