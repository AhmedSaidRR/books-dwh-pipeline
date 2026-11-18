"""Web scraper for https://books.toscrape.com.

The site is a sandbox built specifically for scraping practice, so crawling it
is safe. The scraper is intentionally written as a class (OOP) with small,
separately testable parsing methods:

* ``parse_categories`` parses the sidebar of the home page.
* ``parse_books`` parses a single category page (one HTML document).
* ``scrape_category`` walks the ``next`` pagination links.
* ``scrape`` orchestrates the whole crawl and de-duplicates books.

Scraping is done category by category because the listing pages do not expose
the category of a book: the category is the *context* of the page being read.
That way we need ~100 requests instead of ~1000 detail-page requests.
"""

from __future__ import annotations

import logging
import re
import time
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import ScraperSettings

LOGGER = logging.getLogger(__name__)

RATING_WORDS = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
}

_PRICE_RE = re.compile(r"\d+(?:\.\d+)?")
_AVAILABLE_RE = re.compile(r"(\d+)\s+available", re.IGNORECASE)
_MAX_PAGES_PER_CATEGORY = 100  # hard stop, protects against pagination loops


class ScraperError(RuntimeError):
    """Raised when the remote page cannot be understood at all."""


def parse_price(raw: str) -> float:
    """Turn ``£51.77`` (or the mojibake ``Â£51.77``) into ``51.77``."""
    if raw is None:
        raise ScraperError("price text is missing")
    # The site is UTF-8 but some clients decode it as latin-1 -> "Â£".
    cleaned = raw.replace("\xa0", " ").replace("Â", "").replace(",", "")
    match = _PRICE_RE.search(cleaned)
    if not match:
        raise ScraperError(f"cannot parse price from {raw!r}")
    return round(float(match.group()), 2)


def parse_availability(raw: str) -> bool:
    """``In stock`` / ``In stock (22 available)`` -> True."""
    return "in stock" in (raw or "").lower()


def parse_rating(classes: list[str] | None) -> int | None:
    """Map the CSS class of the star rating (``star-rating Three``) to 1..5."""
    for css_class in classes or []:
        value = RATING_WORDS.get(css_class.strip().lower())
        if value is not None:
            return value
    return None


def book_key_from_url(url: str) -> str:
    """Stable natural key, e.g. ``a-light-in-the-attic_1000``.

    The slug in the URL is unique per book and never changes, unlike the title
    (which can be edited) or the price (which changes every day).
    """
    path = urlparse(url).path.rstrip("/")
    parts = [part for part in path.split("/") if part]
    if parts and parts[-1].lower() in {"index.html", "index.htm"}:
        parts = parts[:-1]
    if not parts:
        raise ScraperError(f"cannot derive a book key from {url!r}")
    return parts[-1]


class BooksScraper:
    """Crawls books.toscrape.com and returns one dict per book."""

    def __init__(
        self,
        settings: ScraperSettings | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.settings = settings or ScraperSettings()
        self.session = session or self._build_session()

    # ------------------------------------------------------------------ HTTP
    def _build_session(self) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=self.settings.max_retries,
            connect=self.settings.max_retries,
            read=self.settings.max_retries,
            status=self.settings.max_retries,
            backoff_factor=0.5,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset({"GET"}),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_maxsize=8)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "User-Agent": self.settings.user_agent,
                "Accept": "text/html,application/xhtml+xml",
                "Accept-Language": "en-GB,en;q=0.9",
            }
        )
        return session

    def fetch(self, url: str) -> str:
        response = self.session.get(url, timeout=self.settings.timeout)
        response.raise_for_status()
        # The site serves UTF-8; forcing it avoids the classic "Â£" mojibake.
        response.encoding = "utf-8"
        return response.text

    def _sleep(self) -> None:
        if self.settings.request_delay > 0:
            time.sleep(self.settings.request_delay)

    # -------------------------------------------------------------- parsing
    @staticmethod
    def parse_categories(html: str, page_url: str) -> list[dict[str, str]]:
        """Return ``[{"name": "Travel", "url": ...}, ...]`` from the sidebar."""
        soup = BeautifulSoup(html, "lxml")
        links = soup.select("div.side_categories ul li ul li a")
        categories: list[dict[str, str]] = []
        seen: set[str] = set()
        for link in links:
            name = link.get_text(strip=True)
            href = link.get("href")
            if not name or not href:
                continue
            url = urljoin(page_url, href)
            slug = url.rstrip("/").split("/")[-2]
            if url in seen:
                continue
            seen.add(url)
            categories.append({"name": name, "slug": slug, "url": url})
        if not categories:
            raise ScraperError("no categories found on the home page")
        return categories

    @staticmethod
    def parse_books(
        html: str,
        page_url: str,
        category: str,
        category_slug: str,
    ) -> list[dict[str, Any]]:
        """Parse every ``article.product_pod`` of one category page."""
        soup = BeautifulSoup(html, "lxml")
        scraped_at = datetime.now(UTC).isoformat(timespec="seconds")
        books: list[dict[str, Any]] = []

        for pod in soup.select("article.product_pod"):
            title_link = pod.select_one("h3 a")
            price_tag = pod.select_one("p.price_color")
            rating_tag = pod.select_one("p.star-rating")
            stock_tag = pod.select_one("p.instock.availability")
            image_tag = pod.select_one("img")

            if title_link is None or price_tag is None:
                LOGGER.warning("Skipping malformed product card on %s", page_url)
                continue

            book_url = urljoin(page_url, title_link.get("href", ""))
            rating = parse_rating(rating_tag.get("class") if rating_tag else None)
            if rating is None:
                LOGGER.warning("Missing rating for %s", book_url)

            image_src = image_tag.get("src") if image_tag else None

            books.append(
                {
                    "book_key": book_key_from_url(book_url),
                    "title": " ".join(
                        (title_link.get("title") or title_link.get_text(" ", strip=True)).split()
                    ),
                    "category": category,
                    "category_slug": category_slug,
                    "price_gbp": parse_price(price_tag.get_text(strip=True)),
                    "rating": rating,
                    "in_stock": parse_availability(
                        stock_tag.get_text(" ", strip=True) if stock_tag else ""
                    ),
                    "book_url": book_url,
                    "image_url": urljoin(page_url, image_src) if image_src else None,
                    "scraped_at": scraped_at,
                }
            )
        return books

    # --------------------------------------------------------------- crawling
    def scrape_category(self, category: dict[str, str], max_pages: int | None = None) -> list[dict]:
        """Walk all pages of one category and return its books."""
        books: list[dict[str, Any]] = []
        page_url: str | None = category["url"]
        page_number = 1

        while page_url:
            if page_number > _MAX_PAGES_PER_CATEGORY or (
                max_pages is not None and page_number > max_pages
            ):
                LOGGER.warning("Stopping pagination for %s at page %s", category["name"], page_number)
                break

            html = self.fetch(page_url)
            page_books = self.parse_books(
                html, page_url, category["name"], category["slug"]
            )
            if not page_books:
                raise ScraperError(
                    f"no books parsed on {page_url} (HTML layout changed?)"
                )
            LOGGER.info(
                "  %s page %s -> %s books", category["name"], page_number, len(page_books)
            )
            books.extend(page_books)

            soup = BeautifulSoup(html, "lxml")
            next_link = soup.select_one("li.next a")
            next_url = urljoin(page_url, next_link["href"]) if next_link else None
            page_url = next_url
            page_number += 1
            if page_url:
                self._sleep()

        return books

    def scrape(self, max_categories: int | None = None) -> list[dict[str, Any]]:
        """Crawl the whole catalogue (or the first N categories).

        Returns one record per book, de-duplicated by ``book_key`` and sorted
        by it so that output is deterministic and diff-friendly.
        """
        limit = max_categories if max_categories is not None else self.settings.max_categories
        LOGGER.info("Fetching category index from %s", self.settings.base_url)
        home_html = self.fetch(self.settings.base_url)
        categories = self.parse_categories(home_html, self.settings.base_url)
        if limit is not None:
            categories = categories[:limit]
        LOGGER.info("Crawling %s categories", len(categories))

        records: dict[str, dict[str, Any]] = {}
        for index, category in enumerate(categories, start=1):
            LOGGER.info("[%s/%s] %s", index, len(categories), category["name"])
            for book in self.scrape_category(category):
                # A book could theoretically appear in two categories; the
                # first occurrence wins so the run stays deterministic.
                records.setdefault(book["book_key"], book)
            self._sleep()

        LOGGER.info("Scraped %s unique books", len(records))
        return sorted(records.values(), key=lambda book: book["book_key"])
