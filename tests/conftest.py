"""Shared pytest fixtures."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

FIXTURES_DIR = Path(__file__).parent / "fixtures"


@pytest.fixture()
def read_fixture() -> Callable[[str], str]:
    def _read(name: str) -> str:
        return (FIXTURES_DIR / name).read_text(encoding="utf-8")

    return _read


@pytest.fixture()
def make_record() -> Callable[..., dict[str, Any]]:
    """Factory for a valid raw scraper record."""

    def _make(index: int = 1, **overrides: Any) -> dict[str, Any]:
        record = {
            "book_key": f"book-{index:03d}",
            "title": f"Book {index}",
            "category": "Travel",
            "category_slug": "travel",
            "price_gbp": 10.0 + index,
            "rating": 3,
            "in_stock": True,
            "book_url": f"https://books.toscrape.com/catalogue/book-{index}/index.html",
            "image_url": "https://books.toscrape.com/media/cache/x.jpg",
            "scraped_at": "2026-11-18T06:00:00+00:00",
        }
        record.update(overrides)
        return record

    return _make


@pytest.fixture()
def raw_records(make_record: Callable[..., dict[str, Any]]) -> list[dict[str, Any]]:
    """Five valid records spread over two categories."""
    return [
        make_record(1, category="Travel"),
        make_record(2, category="Travel", price_gbp=19.99),
        make_record(3, category="Mystery", price_gbp=35.50),
        make_record(4, category="Mystery", price_gbp=40.00, rating=5),
        make_record(5, category="Mystery", price_gbp=7.25, in_stock=False),
    ]
