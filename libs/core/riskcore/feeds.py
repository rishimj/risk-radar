"""News feed definitions and fetching.

Source selection is empirical, not guessed.  Probing all the free options on
2026-07-27 (see the plan's "News sources" section):

  Google News `when:1h` x7 tickers   157 fresh items/hr, all <=60min   <- primary
  Seeking Alpha market currents        7 items, all <=1h
  PR Newswire financial               20 items, 17 <=1h
  CNBC markets / MarketWatch top      30 / 10 items, newest 5.7h / 1.0h
  Yahoo per-ticker                    returned HTTP 429 under repeat polling

Yahoo was the draft's primary pick; it throttles and cannot carry a 120s x 7
loop, so it is demoted to an opt-in backup.  Google News' `when:` operator is
what makes freshness free: constraining the query to the last hour means almost
everything arriving is genuinely new, so the age filter is a backstop rather
than the main defence.
"""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional
from urllib.parse import quote
import email.utils
import hashlib
import logging
import re
import time
import urllib.error
import urllib.request
import xml.etree.ElementTree as ET

from . import config
from .entities import ticker_query
from .models import NewsArticle, iso, utcnow

log = logging.getLogger(__name__)

# Google News titles arrive as "Headline text - Publisher"; the publisher is
# separately available in the <source> element, so strip the suffix.
_PUBLISHER_SUFFIX = re.compile(r"\s+-\s+[^-]{2,40}$")
_TAGS = re.compile(r"<[^>]+>")


@dataclass(frozen=True)
class Feed:
    name: str
    url: str
    ticker: Optional[str] = None      # set when the feed is inherently per-ticker
    enabled: bool = True


def google_news_url(query: str, window: str = "1h") -> str:
    return (
        "https://news.google.com/rss/search?q="
        f"{quote(query)}+when:{window}&hl=en-US&gl=US&ceid=US:en"
    )


def default_feeds(window: str = "1h") -> List[Feed]:
    feeds: List[Feed] = []

    if config.ENABLE_GNEWS:
        for ticker in config.MAG7:
            feeds.append(Feed(
                name=f"gnews:{ticker}",
                url=google_news_url(ticker_query(ticker), window),
                ticker=ticker,
            ))

    feeds.extend([
        Feed("seekingalpha:currents", "https://seekingalpha.com/market_currents.xml"),
        Feed("prnewswire:financial",
             "https://www.prnewswire.com/rss/financial-services-latest-news/"
             "financial-services-latest-news-list.rss"),
        Feed("cnbc:markets", "https://www.cnbc.com/id/20910258/device/rss/rss.html"),
        Feed("marketwatch:top", "https://feeds.content.dowjones.io/public/rss/mw_topstories"),
    ])
    return feeds


def clean_title(raw: str) -> str:
    return _PUBLISHER_SUFFIX.sub("", _TAGS.sub("", raw or "")).strip()


def clean_text(raw: str) -> str:
    return _TAGS.sub(" ", raw or "").replace("&nbsp;", " ").strip()


_ATOM = "{http://www.w3.org/2005/Atom}"


def parse_date(raw: Optional[str]) -> Optional[datetime]:
    """Parse RSS (RFC-822) or Atom (ISO-8601) timestamps into aware UTC."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        dt = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        try:
            dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def article_id(url: str) -> str:
    return hashlib.sha1(url.encode()).hexdigest()


def fetch_url(url: str, timeout: float = 25.0) -> bytes:
    req = urllib.request.Request(url, headers={"User-Agent": config.USER_AGENT})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _items(root: ET.Element) -> List[ET.Element]:
    """RSS <item> or Atom <entry>, whichever this feed uses."""
    return root.findall(".//item") or root.findall(f".//{_ATOM}entry")


def _text(node: ET.Element, *names: str) -> str:
    for name in names:
        found = node.findtext(name)
        if found:
            return found.strip()
    return ""


def _link(node: ET.Element) -> str:
    direct = _text(node, "link", f"{_ATOM}id")
    if direct:
        return direct
    atom = node.find(f"{_ATOM}link")
    if atom is not None:
        return (atom.get("href") or "").strip()
    return ""


def parse_items(raw: bytes, feed: Feed, cutoff: datetime) -> List[NewsArticle]:
    """Parse feed bytes into age-filtered articles.

    Timestamps are NOT clamped here — clamping is the ingestion loop's job and
    must happen strictly after the age filter, otherwise CNBC's evergreen tail
    (observed up to 914h old) gets rewritten to "now" and re-injected as
    breaking news on every single poll.
    """
    try:
        root = ET.fromstring(raw)
    except ET.ParseError as exc:
        log.warning("feed %s unparseable: %s", feed.name, exc)
        return []

    channel_title = _text(root, ".//channel/title") or feed.name

    out: List[NewsArticle] = []
    for node in _items(root):
        url = _link(node)
        title = clean_title(_text(node, "title", f"{_ATOM}title"))
        if not url or not title:
            continue

        published = parse_date(
            _text(node, "pubDate", "published", f"{_ATOM}published", f"{_ATOM}updated")
        )
        if published is None or published < cutoff:
            continue

        source = _text(node, "source") or channel_title

        out.append(NewsArticle(
            article_id=article_id(url),
            title=title,
            url=url,
            source=source.strip(),
            published_at=iso(published),
            feed=feed.name,
            summary=clean_text(_text(node, "description", f"{_ATOM}summary"))[:500],
        ))
    return out


def fetch_feed(feed: Feed, max_age_hours: Optional[int] = None,
               now: Optional[datetime] = None) -> List[NewsArticle]:
    now = now or utcnow()
    cutoff_hours = max_age_hours if max_age_hours is not None else config.MAX_ARTICLE_AGE_HOURS
    cutoff = now - timedelta(hours=cutoff_hours)

    try:
        raw = fetch_url(feed.url)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
        log.warning("feed %s failed: %s", feed.name, exc)
        return []

    return parse_items(raw, feed, cutoff)


def fetch_all(feeds: Optional[Iterable[Feed]] = None,
              max_age_hours: Optional[int] = None,
              stagger_seconds: float = 0.4,
              now: Optional[datetime] = None) -> List[NewsArticle]:
    """Fetch every feed with a polite stagger, deduping by URL within the sweep.

    Cross-ticker overlap is expected and correct: an "Nvidia and Tesla" story
    appears in both sweeps, collapses to one article here, and then matches
    BOTH tickers downstream via riskcore.entities.
    """
    feeds = list(feeds if feeds is not None else default_feeds())
    seen = set()
    out: List[NewsArticle] = []

    for i, feed in enumerate(feeds):
        if not feed.enabled:
            continue
        for art in fetch_feed(feed, max_age_hours=max_age_hours, now=now):
            if art.article_id in seen:
                continue
            seen.add(art.article_id)
            out.append(art)
        if stagger_seconds and i < len(feeds) - 1:
            time.sleep(stagger_seconds)

    return out
