"""Entity matching: aliases, roles, and the word-boundary false positives."""
import pytest

from riskcore.entities import COMPANIES, build_keyword_mapping, detect_companies, ticker_query


def tickers(headline, body=""):
    return {c["ticker"] for c in detect_companies(headline, body)}


def test_matches_company_name():
    assert tickers("Tesla shares plunge on weak deliveries") == {"TSLA"}


def test_matches_ticker_symbol():
    assert "NVDA" in tickers("NVDA leads chip selloff")


@pytest.mark.parametrize("headline,expected", [
    ("New iPhone sales disappoint", "AAPL"),
    ("Azure outage hits enterprise customers", "MSFT"),
    ("YouTube ad revenue misses", "GOOGL"),
    ("AWS suffers major region failure", "AMZN"),
    ("GeForce demand cools", "NVDA"),
    ("Instagram faces new lawsuit", "META"),
    ("Cybertruck recall widens", "TSLA"),
])
def test_aliases_resolve_to_the_right_ticker(headline, expected):
    assert expected in tickers(headline)


# -- the reason for \b anchoring -------------------------------------------
@pytest.mark.parametrize("headline", [
    "Teslamotors fan site launches",     # substring of an alias
    "Targeted ad spending rises",        # contains "targeted", not a ticker
    "Metabolism research funding cut",   # contains "meta"
    "Pineapple farming subsidies",       # contains "apple"
])
def test_no_false_positive_on_substrings(headline):
    assert tickers(headline) == set()


def test_case_insensitive():
    assert tickers("TESLA RECALLS VEHICLES") == {"TSLA"}


# -- ambiguous names (found in live data, not invented) ---------------------
@pytest.mark.parametrize("headline", [
    "Get Ready for the Gravenstein Apple Fair in Sebastopol Aug. 8-9",
    "Apple pie recipes for the county fair",
    "Apple orchard owners report a strong harvest",
    "Amazon rainforest deforestation slows in June",
    "Illegal logging in the Amazon basin continues",
    "Meta-analysis finds no link between the two",
])
def test_non_corporate_senses_are_rejected(headline):
    """Ordinary English used correctly. The first is a real headline that
    reached the pipeline during live testing."""
    assert tickers(headline) == set()


@pytest.mark.parametrize("headline,expected", [
    # These must survive: an allowlist-of-finance-words approach killed all of
    # them, which is why the guard is negative evidence instead.
    ("Apple Reclaims Title as the World's Most Valuable Public Company", "AAPL"),
    ("Apple on Verge of Becoming $5 Trillion Company", "AAPL"),
    ("Traders Bet $590 Million on Apple Options Ahead of Results", "AAPL"),
    ("Apple dethrones Nvidia as the world's most valuable company", "AAPL"),
    ("Ford's $30K EVs Are Ditching Google For Apple Maps", "AAPL"),
    ("Apple stock hits record high before Q3 earnings", "AAPL"),
    ("Amazon shares slide as analysts cut price target", "AMZN"),
    ("Meta revenue misses estimates", "META"),
])
def test_corporate_uses_survive_the_guard(headline, expected):
    assert expected in tickers(headline)


@pytest.mark.parametrize("headline,expected", [
    ("New iPhone sales disappoint fans", "AAPL"),      # no finance word at all
    ("AWS suffers a major outage", "AMZN"),
    ("Instagram rolls out a new feed", "META"),
])
def test_specific_aliases_need_no_corroboration(headline, expected):
    assert expected in tickers(headline)


def test_unambiguous_names_are_unaffected():
    """Tesla and Nvidia are not English words; they never need corroboration."""
    assert tickers("Tesla opens a showroom") == {"TSLA"}
    assert tickers("Nvidia announces a partnership") == {"NVDA"}


# -- roles ------------------------------------------------------------------
def test_headline_match_is_primary():
    got = detect_companies("Tesla recalls vehicles", "")
    assert got == [{"ticker": "TSLA", "role": "primary"}]


def test_body_only_match_is_mentioned():
    got = detect_companies("Chip sector wobbles", "Analysts singled out Nvidia for scrutiny.")
    assert got == [{"ticker": "NVDA", "role": "mentioned"}]


# -- multi-ticker -----------------------------------------------------------
def test_one_article_can_match_several_tickers():
    """Cross-query overlap is deliberate: dedup keeps one article, both match."""
    assert tickers("Nvidia and Tesla both slide in afternoon trading") == {"NVDA", "TSLA"}


def test_longest_keyword_wins():
    """'amazon web services' must not be shadowed by a shorter alias."""
    assert tickers("Amazon Web Services outage") == {"AMZN"}


def test_each_ticker_appears_at_most_once():
    got = detect_companies("Apple iPhone and Apple Watch and the App Store all featured")
    assert len(got) == 1 and got[0]["ticker"] == "AAPL"


# -- query construction -----------------------------------------------------
def test_alphabet_and_meta_queries_include_consumer_brands():
    assert "Google" in ticker_query("GOOGL")
    assert "Facebook" in ticker_query("META")


def test_every_mag7_company_is_defined():
    from riskcore.config import MAG7
    assert set(COMPANIES) == set(MAG7)


def test_keyword_mapping_covers_names_and_symbols():
    mapping = build_keyword_mapping()
    assert mapping["tesla"] == "TSLA"
    assert mapping["tsla"] == "TSLA"
