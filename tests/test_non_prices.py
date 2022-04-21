from copy import deepcopy
from typing import Any, Dict

import pytest
from betfairlightweight.resources import MarketBook

from betfairutil import calculate_book_percentage
from betfairutil import calculate_market_book_diff
from betfairutil import EX_KEYS
from betfairutil import random_from_market_id
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


def test_calculate_market_book_diff(market_book: Dict[str, Any]):
    market_book_diff = calculate_market_book_diff(market_book, market_book)
    for runner in market_book["runners"]:
        for ex_key in EX_KEYS:
            assert market_book_diff.get_size_changes(runner["selectionId"], ex_key) == {}

    market_book_diff = calculate_market_book_diff(MarketBook(**market_book), MarketBook(**market_book))
    for runner in market_book["runners"]:
        for ex_key in EX_KEYS:
            assert market_book_diff.get_size_changes(runner["selectionId"], ex_key) == {}

    previous_market_book = deepcopy(market_book)
    market_book["runners"][1]["ex"]["availableToBack"].pop()
    market_book_diff = calculate_market_book_diff(market_book, previous_market_book)
    assert market_book_diff.get_size_changes(123, "availableToBack") == {}
    assert market_book_diff.get_size_changes(123, "availableToLay") == {}
    assert market_book_diff.get_size_changes(123, "tradedVolume") == {}
    assert market_book_diff.get_size_changes(456, "availableToBack") == {1.98: -1}
    assert market_book_diff.get_size_changes(456, "availableToLay") == {}
    assert market_book_diff.get_size_changes(456, "tradedVolume") == {}

    market_book_diff = calculate_market_book_diff(MarketBook(**market_book), MarketBook(**previous_market_book))
    assert market_book_diff.get_size_changes(123, "availableToBack") == {}
    assert market_book_diff.get_size_changes(123, "availableToLay") == {}
    assert market_book_diff.get_size_changes(123, "tradedVolume") == {}
    assert market_book_diff.get_size_changes(456, "availableToBack") == {1.98: -1}
    assert market_book_diff.get_size_changes(456, "availableToLay") == {}
    assert market_book_diff.get_size_changes(456, "tradedVolume") == {}


def test_random_from_market_id():
    with pytest.raises(ValueError):
        random_from_market_id(-1)

    with pytest.raises(ValueError):
        random_from_market_id(0.5)

    assert random_from_market_id("1.123") == 0.5181806162370606
