import re
from typing import List
from .models import ClaimSignal

DATE_RE = re.compile(r"\b(\d{4}-\d{2}-\d{2})\b")
QUANTITY_RE = re.compile(r"\b\d[\d\.,]*k?\b", re.I)
ENTITY_RE = re.compile(r"\b([A-Z][a-z]{2,}(?:\s+[A-Z][a-z]{2,})*)\b")
MODAL_RE = re.compile(r"\b(definitely|confirmed|proved|certainly|sure|guaranteed|reported|reportedly|according to)\b", re.I)


def _strip_url_numbers(text: str, quantities: list) -> list:
    # Remove numeric tokens that appear only as parts of URLs (query params, path IDs)
    urls = re.findall(r"https?://\S+", text)
    if not urls:
        return quantities
    url_nums = set()
    for u in urls:
        url_nums.update(re.findall(r"\d+", u))
    return [q for q in quantities if q not in url_nums]


def extract_claim_signals(text: str) -> ClaimSignal:
    # naive but deterministic heuristics
    dates = DATE_RE.findall(text)
    quantities = QUANTITY_RE.findall(text)
    # remove numbers that are only present in embedded URLs / params
    quantities = _strip_url_numbers(text, quantities)
    entities = []
    # capture capitalized tokens but avoid sentence starts that are common words
    for m in ENTITY_RE.findall(text):
        if len(m) > 1:
            entities.append(m)
    modal = MODAL_RE.findall(text)

    # spans: short snippets that look like claims
    # heuristic: sentences with numbers, dates or modals
    spans = []
    for s in re.split(r"(?<=[\.\?!])\s+", text):
        if DATE_RE.search(s) or QUANTITY_RE.search(s) or MODAL_RE.search(s):
            spans.append(s.strip())

    return ClaimSignal(spans=spans, dates=dates, quantities=quantities, entities=entities, modal=modal)
