import re
from unicodedata import normalize


def normalizer_str(text):
    text = re.sub(
        r"([^n\u0300-\u036f]|n(?!\u0303(?![\u0300-\u036f])))[\u0300-\u036f]+", r"\1",
        normalize("NFD", str(text)), 0, re.I
    )
    return normalize("NFC", str(text))
