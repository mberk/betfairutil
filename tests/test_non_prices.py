from typing import Any, Dict

import pytest
from betfairlightweight.resources import MarketBook

from betfairutil import calculate_book_percentage
from betfairutil import Side


@pytest.fixture
def market_book():
    return {
        "runners": [
            {
                "selectionId": 123,
                "status": "ACTIVE",
                "handicap": 0.0,
                "ex": {
                    "availableToBack": [{"price": 1.98, "size": 1}],
                    "availableToLay": [],
                    "tradedVolume": [],
                },
            },
            {
                "selectionId": 456,
                "status": "ACTIVE",
                "handicap": 0.0,
                "ex": {
                    "availableToBack": [{"price": 1.98, "size": 1}],
                    "availableToLay": [],
                    "tradedVolume": [],
                },
            },
        ]
    }


@pytest.mark.parametrize("use_market_book_objects", [False, True])
def test_calculate_book_percentage(
    market_book: Dict[str, Any], use_market_book_objects: bool
):
    assert (
        calculate_book_percentage(
            MarketBook(**market_book) if use_market_book_objects else market_book,
            Side.BACK,
        )
        == 1.0 / 1.98 + 1.0 / 1.98
    )
    assert (
        calculate_book_percentage(
            MarketBook(**market_book) if use_market_book_objects else market_book,
            Side.LAY,
        )
        == 0
    )

    market_book["runners"][1]["ex"]["availableToBack"].pop()
    market_book["runners"][1]["ex"]["availableToLay"].append({"price": 2.02, "size": 1})

    assert (
        calculate_book_percentage(
            MarketBook(**market_book) if use_market_book_objects else market_book,
            Side.BACK,
        )
        == 1.0 + 1.0 / 1.98
    )
