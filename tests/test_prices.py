from typing import Any, Dict

import pytest
from betfairlightweight.resources import RunnerBook

from betfairutil import BETFAIR_PRICES
from betfairutil import BETFAIR_PRICE_TO_PRICE_INDEX_MAP
from betfairutil import decrement_price
from betfairutil import get_inside_best_price
from betfairutil import get_spread
from betfairutil import increment_price
from betfairutil import Side


@pytest.fixture
def runner():
    return {
        "selectionId": 123,
        "status": "ACTIVE",
        "handicap": 0.0,
        "ex": {"availableToBack": [], "availableToLay": [], "tradedVolume": []},
    }


def test_increment_price():
    assert increment_price(1000) is None

    for price in BETFAIR_PRICES:
        if price == 1000:
            continue
        next_price = increment_price(price)
        assert (
            BETFAIR_PRICE_TO_PRICE_INDEX_MAP[next_price]
            == BETFAIR_PRICE_TO_PRICE_INDEX_MAP[price] + 1
        )


def test_decrement_price():
    assert decrement_price(1.01) is None

    for price in BETFAIR_PRICES:
        if price == 1.01:
            continue
        prev_price = decrement_price(price)
        assert (
            BETFAIR_PRICE_TO_PRICE_INDEX_MAP[prev_price]
            == BETFAIR_PRICE_TO_PRICE_INDEX_MAP[price] - 1
        )


@pytest.mark.parametrize("use_runner_book_objects", [(False,), (True,)])
def test_get_spread(runner: Dict[str, Any], use_runner_book_objects: bool):
    assert (
        get_spread(RunnerBook(**runner) if use_runner_book_objects else runner) is None
    )

    runner["ex"]["availableToBack"].append({"price": 1.01, "size": 1})
    assert (
        get_spread(RunnerBook(**runner) if use_runner_book_objects else runner) is None
    )

    runner["ex"]["availableToLay"].append({"size": 1})
    for i, price in enumerate(BETFAIR_PRICES):
        if price == 1.01:
            continue
        runner["ex"]["availableToLay"][0]["price"] = price
        assert (
            get_spread(RunnerBook(**runner) if use_runner_book_objects else runner) == i
        )


@pytest.mark.parametrize("use_runner_book_objects", [(False,), (True,)])
def test_get_inside_best_price(runner: Dict[str, Any], use_runner_book_objects: bool):
    assert (
        get_inside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        is None
    )
    assert (
        get_inside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.LAY
        )
        is None
    )

    runner["ex"]["availableToBack"].append({"price": 1.01, "size": 1})
    assert (
        get_inside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        == 1.02
    )
    assert (
        get_inside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.LAY
        )
        is None
    )

    for i, price in enumerate(BETFAIR_PRICES):
        if price == 1.01:
            continue
        runner["ex"]["availableToBack"][0]["price"] = price
        inside_best_price = get_inside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        if price == 1000:
            assert inside_best_price is None
        else:
            assert (
                BETFAIR_PRICE_TO_PRICE_INDEX_MAP[price] + 1
                == BETFAIR_PRICE_TO_PRICE_INDEX_MAP[inside_best_price]
            )
        assert (
            get_inside_best_price(
                RunnerBook(**runner) if use_runner_book_objects else runner, Side.LAY
            )
            is None
        )
