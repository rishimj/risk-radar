"""Feed parsing plus the age-filter/clamp ordering that keeps evergreen news out."""
from datetime import datetime, timedelta, timezone

import pytest

from riskcore import config
from riskcore.feeds import (
    Feed, article_id, clean_text, clean_title, default_feeds, google_news_url,
)
from riskcore.models import parse_iso

NOW = datetime(2026, 7, 27, 20, 0, tzinfo=timezone.utc)


# -- title cleaning ---------------------------------------------------------
def test_strips_google_news_publisher_suffix():
    assert clean_title("Tesla stock slides - The Motley Fool") == "Tesla stock slides"


def test_keeps_hyphens_inside_a_headline():
    assert clean_title("Full self-driving under review") == "Full self-driving under review"


def test_strips_html_tags():
    assert clean_text("<a href='x'>Read more</a>") == "Read more"


# -- urls -------------------------------------------------------------------
def test_google_news_url_constrains_the_time_window():
    url = google_news_url("Tesla", "1h")
    assert "when:1h" in url and "Tesla" in url


def test_article_id_is_stable_and_url_derived():
    assert article_id("https://a/b") == article_id("https://a/b")
    assert article_id("https://a/b") != article_id("https://a/c")


# -- feed set ---------------------------------------------------------------
def test_default_feeds_cover_every_mag7_ticker():
    per_ticker = {f.ticker for f in default_feeds() if f.ticker}
    assert per_ticker == set(config.MAG7)


def test_default_feeds_include_the_secondary_wires():
    names = {f.name for f in default_feeds()}
    assert "seekingalpha:currents" in names
    assert "prnewswire:financial" in names


# -- age filtering ----------------------------------------------------------
def rfc822(dt: datetime) -> str:
    return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")


def rss(*items: str) -> bytes:
    body = "".join(items)
    return (
        '<?xml version="1.0"?><rss version="2.0"><channel>'
        "<title>Test Feed</title>" + body + "</channel></rss>"
    ).encode()


def item(published: datetime, title="Tesla news", link="https://example.com/1",
         source: str = "", description: str = "") -> str:
    src = f"<source>{source}</source>" if source else ""
    return (
        f"<item><title>{title}</title><link>{link}</link>"
        f"<pubDate>{rfc822(published)}</pubDate>{src}"
        f"<description>{description}</description></item>"
    )


FEED = Feed("t", "http://x")


def parse(raw: bytes, hours: int = 6, now: datetime = NOW):
    from riskcore.feeds import parse_items
    return parse_items(raw, FEED, now - timedelta(hours=hours))


def test_evergreen_items_are_dropped_before_any_clamping():
    """CNBC's tail reached 914h old; clamping those to now would re-inject them
    as breaking news on every poll, forever."""
    raw = rss(
        item(NOW - timedelta(hours=1), link="https://example.com/fresh"),
        item(NOW - timedelta(hours=914), link="https://example.com/ancient"),
    )
    assert [a.url for a in parse(raw)] == ["https://example.com/fresh"]


def test_articles_without_a_timestamp_are_dropped():
    raw = rss("<item><title>No date</title><link>https://example.com/x</link></item>")
    assert parse(raw) == []


def test_parse_preserves_the_original_timestamp():
    """Parsing must NOT clamp — that is the ingestion loop's job, and only
    after the age filter has run."""
    published = NOW - timedelta(hours=2)
    got = parse(rss(item(published)))
    assert parse_iso(got[0].published_at) == published


def test_entries_missing_title_or_link_are_skipped():
    raw = rss(
        item(NOW, title="", link="https://example.com/a"),
        item(NOW, title="Fine", link=""),
        item(NOW, title="Good", link="https://example.com/c"),
    )
    assert [a.title for a in parse(raw)] == ["Good"]


def test_publisher_suffix_stripped_from_real_feed_xml():
    got = parse(rss(item(NOW, title="Tesla stock slides - Barron's")))
    assert got[0].title == "Tesla stock slides"


def test_source_element_becomes_the_publisher():
    got = parse(rss(item(NOW, source="The Motley Fool")))
    assert got[0].source == "The Motley Fool"


def test_falls_back_to_channel_title_when_item_has_no_source():
    assert parse(rss(item(NOW)))[0].source == "Test Feed"


def test_malformed_xml_yields_nothing_rather_than_raising():
    assert parse(b"<rss><channel><item>truncated") == []


def test_atom_entries_are_supported():
    """SEC EDGAR's 8-K feed is Atom, not RSS."""
    raw = (
        '<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom">'
        "<title>EDGAR</title><entry><title>8-K - Tesla Inc</title>"
        '<link href="https://sec.gov/x"/>'
        f"<updated>{(NOW - timedelta(minutes=5)).isoformat()}</updated>"
        "</entry></feed>"
    ).encode()
    got = parse(raw)
    assert len(got) == 1 and got[0].url == "https://sec.gov/x"


@pytest.mark.parametrize("raw,expected_hour", [
    ("Mon, 27 Jul 2026 19:23:47 GMT", 19),          # RSS / RFC-822
    ("2026-07-27T19:23:47Z", 19),                    # Atom / ISO-8601
    ("2026-07-27T19:23:47+00:00", 19),
])
def test_parse_date_handles_both_feed_dialects(raw, expected_hour):
    from riskcore.feeds import parse_date
    assert parse_date(raw).hour == expected_hour


def test_parse_date_rejects_garbage():
    from riskcore.feeds import parse_date
    assert parse_date("not a date") is None
    assert parse_date("") is None
    assert parse_date(None) is None
