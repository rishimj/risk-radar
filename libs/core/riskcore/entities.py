"""Mag 7 entity matching.

Trimmed from the old repo's company_database.py (which carried a sector-wide
supplier roster we don't need) but keeps its matching algorithm intact:
longest-keyword-first, word-boundary anchored, headline hit => role "primary".

The word-boundary anchor is load-bearing: without it "tesla" matches inside
"teslamotors" and "meta" matches inside "metabolism".
"""
from dataclasses import dataclass, field
from typing import Dict, List, Tuple
import re


@dataclass(frozen=True)
class CompanyInfo:
    ticker: str
    name: str
    aliases: Tuple[str, ...] = field(default_factory=tuple)

    @property
    def keywords(self) -> Tuple[str, ...]:
        return (self.name.lower(),) + tuple(a.lower() for a in self.aliases)


COMPANIES: Dict[str, CompanyInfo] = {
    "AAPL": CompanyInfo("AAPL", "Apple", (
        "apple inc", "iphone", "ipad", "macbook", "app store", "apple music",
        "vision pro", "tim cook", "airpods", "apple watch",
    )),
    "MSFT": CompanyInfo("MSFT", "Microsoft", (
        "azure", "windows 11", "office 365", "microsoft 365", "xbox",
        "satya nadella", "copilot", "linkedin",
    )),
    "GOOGL": CompanyInfo("GOOGL", "Alphabet", (
        "google", "youtube", "android", "google cloud", "gemini",
        "sundar pichai", "waymo", "deepmind", "chrome",
    )),
    "AMZN": CompanyInfo("AMZN", "Amazon", (
        "aws", "amazon web services", "prime video", "alexa", "andy jassy",
        "amazon prime", "kuiper",
    )),
    "NVDA": CompanyInfo("NVDA", "Nvidia", (
        "geforce", "rtx", "cuda", "jensen huang", "blackwell", "hopper gpu",
        "h100", "b200",
    )),
    "META": CompanyInfo("META", "Meta", (
        "facebook", "instagram", "whatsapp", "mark zuckerberg", "oculus",
        "quest headset", "threads app", "meta platforms",
    )),
    "TSLA": CompanyInfo("TSLA", "Tesla", (
        "elon musk", "cybertruck", "model 3", "model y", "model s", "model x",
        "gigafactory", "full self-driving", "autopilot", "robotaxi",
    )),
}


# Three Mag 7 names are ordinary English words. Observed live: "Get Ready for
# the Gravenstein Apple Fair in Sebastopol" matched AAPL — a fruit festival.
# Word boundaries cannot help; "Apple" is a real word used correctly.
#
# The fix is negative evidence, not positive. Requiring a finance term to
# corroborate was tried first and was badly lopsided: measured against live
# data it dropped 35 articles to fix 1, killing obvious hits like "Apple
# Reclaims Title as the World's Most Valuable Public Company" — because the ways
# a company can be discussed are unbounded, while a keyword allowlist is finite.
#
# Inverted, the problem is tractable: the NON-corporate senses are few and
# stereotyped. Reject a bare ambiguous name only when the text positively looks
# like the fruit, the rainforest, or the prefix.
AMBIGUOUS_NAMES = {"apple", "meta", "amazon"}

_NON_CORPORATE = {
    "apple": re.compile(
        r"\bapple\s+(fair|pie|orchard|cider|farm|harvest|tree|trees|sauce|juice|"
        r"festival|picking|crisp|butter|blossom|season|grower|growers|variety)\b"
        r"|\b(gravenstein|honeycrisp|granny\s+smith|fuji|gala|mcintosh|braeburn)\s+apple",
        re.IGNORECASE,
    ),
    "amazon": re.compile(
        r"\bamazon\s+(rainforest|rain\s+forest|river|basin|jungle|delta|tribe|"
        r"tribes|region|indigenous)\b"
        r"|\b(deforestation|rainforest|rain\s+forest)\b",
        re.IGNORECASE,
    ),
    "meta": re.compile(r"\bmeta[-\s]analys[ie]s\b|\bmeta[-\s]data\b", re.IGNORECASE),
}


def _looks_non_corporate(keyword: str, text: str) -> bool:
    pattern = _NON_CORPORATE.get(keyword)
    return bool(pattern and pattern.search(text))


def build_keyword_mapping() -> Dict[str, str]:
    """keyword -> ticker, for every name and alias."""
    mapping: Dict[str, str] = {}
    for ticker, info in COMPANIES.items():
        for kw in info.keywords:
            mapping[kw] = ticker
        mapping[ticker.lower()] = ticker
    return mapping


KEYWORD_TO_TICKER: Dict[str, str] = build_keyword_mapping()

# Longest first so "amazon web services" wins over "amazon", and compile once —
# this runs on every article inside a Flink operator.
_SORTED: List[Tuple[str, str, re.Pattern]] = sorted(
    (
        (kw, ticker, re.compile(r"\b" + re.escape(kw) + r"\b", re.IGNORECASE))
        for kw, ticker in KEYWORD_TO_TICKER.items()
    ),
    key=lambda item: len(item[0]),
    reverse=True,
)


def detect_companies(headline: str, body: str = "") -> List[Dict[str, str]]:
    """Return [{ticker, role}] for every Mag 7 company mentioned.

    role is "primary" when the match appears in the headline, else "mentioned".
    Google News gives us no body text, so in practice everything is "primary" —
    the parameter stays for the wire feeds that do carry a summary.

    A bare ambiguous name ("apple", "meta", "amazon") is rejected when the text
    positively reads as the non-corporate sense; specific aliases always stand
    alone. See AMBIGUOUS_NAMES and _NON_CORPORATE.
    """
    headline = headline or ""
    haystack = f"{headline} {body}".strip()

    out: List[Dict[str, str]] = []
    seen = set()
    for keyword, ticker, pattern in _SORTED:
        if ticker in seen:
            continue
        if not pattern.search(haystack):
            continue
        if keyword in AMBIGUOUS_NAMES and _looks_non_corporate(keyword, haystack):
            continue          # "Gravenstein Apple Fair" is not Apple Inc.

        role = "primary" if pattern.search(headline) else "mentioned"
        out.append({"ticker": ticker, "role": role})
        seen.add(ticker)
    return out


def ticker_query(ticker: str) -> str:
    """Google News search expression for a ticker.

    Alphabet and Meta both need their consumer brand OR'd in, otherwise the
    query misses most coverage.
    """
    special = {
        "GOOGL": "Google OR Alphabet",
        "META": "Meta OR Facebook",
    }
    if ticker in special:
        return special[ticker]
    return COMPANIES[ticker].name
