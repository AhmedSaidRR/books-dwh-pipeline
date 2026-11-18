"""Unit tests for the scraper parsing logic (no network required)."""

from __future__ import annotations

import pytest

from src.config import ScraperSettings
from src.scraper import (
    BooksScraper,
    ScraperError,
    book_key_from_url,
    parse_availability,
    parse_price,
    parse_rating,
)

PAGE_1_URL = "https://books.toscrape.com/catalogue/category/books/travel_2/index.html"
PAGE_2_URL = "https://books.toscrape.com/catalogue/category/books/travel_2/page-2.html"


class FakeResponse:
    def __init__(self, text: str) -> None:
        self.text = text
        self.encoding = "utf-8"

    def raise_for_status(self) -> None:  # noqa: D401 - test double
        return None


class FakeSession:
    """Minimal stand-in for requests.Session."""

    def __init__(self, pages: dict[str, str]) -> None:
        self.pages = pages
        self.requested: list[str] = []

    def get(self, url: str, timeout: int | None = None) -> FakeResponse:
        self.requested.append(url)
        if url not in self.pages:
            raise AssertionError(f"unexpected URL requested: {url}")
        return FakeResponse(self.pages[url])


def make_scraper(session: FakeSession) -> BooksScraper:
    settings = ScraperSettings(request_delay=0.0, timeout=1, max_retries=0)
    return BooksScraper(settings=settings, session=session)  # type: ignore[arg-type]


def test_parse_price_handles_currency_symbols() -> None:
    assert parse_price("£51.77") == 51.77
    assert parse_price("Â£13.99") == 13.99
    assert parse_price("£1,234.50") == 1234.50
    assert parse_price("  £9.00  ") == 9.00


def test_parse_price_rejects_garbage() -> None:
    with pytest.raises(ScraperError):
        parse_price("free")


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("In stock", True),
        ("In stock (22 available)", True),
        ("Out of stock", False),
        ("", False),
    ],
)
def test_parse_availability(text: str, expected: bool) -> None:
    assert parse_availability(text) is expected


@pytest.mark.parametrize(
    ("classes", "expected"),
    [
        (["star-rating", "One"], 1),
        (["star-rating", "Three"], 3),
        (["star-rating", "Five"], 5),
        (["star-rating"], None),
        (None, None),
    ],
)
def test_parse_rating(classes: list[str] | None, expected: int | None) -> None:
    assert parse_rating(classes) == expected


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html", "a-light-in-the-attic_1000"),
        ("https://books.toscrape.com/catalogue/the-black-maria_1001/", "the-black-maria_1001"),
        ("https://books.toscrape.com/catalogue/tipping-the-velvet_999/index.html", "tipping-the-velvet_999"),
    ],
)
def test_book_key_from_url(url: str, expected: str) -> None:
    assert book_key_from_url(url) == expected


def test_parse_categories(read_fixture) -> None:
    categories = BooksScraper.parse_categories(
        read_fixture("home.html"), "https://books.toscrape.com/"
    )
    assert [category["name"] for category in categories] == ["Travel", "Mystery", "Sequential Art"]
    assert categories[0]["url"] == "https://books.toscrape.com/catalogue/category/books/travel_2/index.html"
    assert categories[0]["slug"] == "travel_2"


def test_parse_categories_fails_on_unexpected_layout() -> None:
    with pytest.raises(ScraperError):
        BooksScraper.parse_categories("<html><body>maintenance</body></html>", "https://example.com/")


def test_parse_books_extracts_every_field(read_fixture) -> None:
    books = BooksScraper.parse_books(
        read_fixture("category_page_1.html"), PAGE_1_URL, "Travel", "travel_2"
    )
    assert len(books) == 3

    first = books[0]
    assert first["title"] == "A Light in the Attic"
    assert first["book_key"] == "a-light-in-the-attic_1000"
    assert first["category"] == "Travel"
    assert first["price_gbp"] == 51.77
    assert first["rating"] == 3
    assert first["in_stock"] is True
    assert first["book_url"] == (
        "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html"
    )
    assert first["image_url"].endswith("fe72f0532301.jpg")
    assert first["scraped_at"].endswith("+00:00")

    # Latin-1 mojibake from the HTML must not leak into the price.
    assert books[1]["price_gbp"] == 13.99
    assert books[1]["rating"] == 5


def test_scrape_category_follows_pagination(read_fixture) -> None:
    session = FakeSession(
        {
            PAGE_1_URL: read_fixture("category_page_1.html"),
            PAGE_2_URL: read_fixture("category_page_2.html"),
        }
    )
    scraper = make_scraper(session)
    books = scraper.scrape_category(
        {"name": "Travel", "slug": "travel_2", "url": PAGE_1_URL}
    )

    assert session.requested == [PAGE_1_URL, PAGE_2_URL]
    assert len(books) == 4
    assert books[-1]["book_key"] == "the-dark-side_1002"
    assert books[-1]["in_stock"] is False


def test_scrape_deduplicates_and_sorts(read_fixture) -> None:
    home_url = "https://books.toscrape.com/"
    session = FakeSession(
        {
            home_url: read_fixture("home.html"),
            PAGE_1_URL: read_fixture("category_page_1.html"),
            PAGE_2_URL: read_fixture("category_page_2.html"),
        }
    )
    scraper = make_scraper(session)

    # Only the first category is crawled, but its 4 cards contain a duplicate.
    books = scraper.scrape(max_categories=1)

    keys = [book["book_key"] for book in books]
    assert keys == sorted(set(keys))
    assert keys == ["a-light-in-the-attic_1000", "the-black-maria_1001", "the-dark-side_1002"]
