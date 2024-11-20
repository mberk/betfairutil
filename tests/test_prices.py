from typing import Any, Dict

import pytest
from betfairlightweight.resources import RunnerBook

from betfairutil import BETFAIR_PRICES
from betfairutil import BETFAIR_PRICE_TO_PRICE_INDEX_MAP
from betfairutil import decrement_price
from betfairutil import get_inside_best_price
from betfairutil import get_outside_best_price
from betfairutil import get_spread
from betfairutil import increment_price
from betfairutil import is_market_contiguous
from betfairutil import is_market_one_tick_wide
from betfairutil import is_price_the_same_or_better
from betfairutil import is_price_worse
from betfairutil import make_price_betfair_valid
from betfairutil import Side
from betfairutil import virtualise_two_runner_price


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


@pytest.mark.parametrize("use_runner_book_objects", [False, True])
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


@pytest.mark.parametrize("use_runner_book_objects", [False, True])
def test_is_market_one_tick_wide(runner: Dict[str, Any], use_runner_book_objects: bool):
    assert not is_market_one_tick_wide(
        RunnerBook(**runner) if use_runner_book_objects else runner
    )

    runner["ex"]["availableToBack"].append({"price": 1.01, "size": 1})
    assert not is_market_one_tick_wide(
        RunnerBook(**runner) if use_runner_book_objects else runner
    )

    runner["ex"]["availableToLay"].append({"size": 1})
    for i, price in enumerate(BETFAIR_PRICES):
        if price == 1.01:
            continue
        runner["ex"]["availableToLay"][0]["price"] = price
        assert (
            price == 1.02
            and is_market_one_tick_wide(
                RunnerBook(**runner) if use_runner_book_objects else runner
            )
        ) or (
            price != 1.02
            and not is_market_one_tick_wide(
                RunnerBook(**runner) if use_runner_book_objects else runner
            )
        )


@pytest.mark.parametrize("use_runner_book_objects", [False, True])
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


@pytest.mark.parametrize("use_runner_book_objects", [False, True])
def test_get_outside_best_price(runner: Dict[str, Any], use_runner_book_objects: bool):
    assert (
        get_outside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        is None
    )
    assert (
        get_outside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.LAY
        )
        is None
    )

    runner["ex"]["availableToBack"].append({"price": 1000, "size": 1})
    assert (
        get_outside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        == 990
    )
    assert (
        get_outside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.LAY
        )
        is None
    )

    for i, price in enumerate(BETFAIR_PRICES):
        if price == 1000:
            continue
        runner["ex"]["availableToBack"][0]["price"] = price
        outside_best_price = get_outside_best_price(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        if price == 1.01:
            assert outside_best_price is None
        else:
            assert (
                BETFAIR_PRICE_TO_PRICE_INDEX_MAP[price] - 1
                == BETFAIR_PRICE_TO_PRICE_INDEX_MAP[outside_best_price]
            )
        assert (
            get_outside_best_price(
                RunnerBook(**runner) if use_runner_book_objects else runner, Side.LAY
            )
            is None
        )


def test_is_price_the_same_or_better():
    with pytest.raises(TypeError):
        is_price_the_same_or_better(1.01, 1.02, "foo")

    for price in BETFAIR_PRICES:
        if price == 1000:
            continue
        next_price = increment_price(price)
        assert is_price_the_same_or_better(price, price, Side.BACK)
        assert not is_price_the_same_or_better(price, next_price, Side.BACK)
        assert is_price_the_same_or_better(next_price, price, Side.BACK)

        assert is_price_the_same_or_better(price, price, Side.LAY)
        assert is_price_the_same_or_better(price, next_price, Side.LAY)
        assert not is_price_the_same_or_better(next_price, price, Side.LAY)


def test_is_price_worse():
    with pytest.raises(TypeError):
        is_price_the_same_or_better(1.01, 1.02, "foo")

    for price in BETFAIR_PRICES:
        if price == 1000:
            continue
        next_price = increment_price(price)
        assert not is_price_worse(price, price, Side.BACK)
        assert is_price_worse(price, next_price, Side.BACK)
        assert not is_price_worse(next_price, price, Side.BACK)

        assert not is_price_worse(price, price, Side.LAY)
        assert not is_price_worse(price, next_price, Side.LAY)
        assert is_price_worse(next_price, price, Side.LAY)


def test_make_price_betfair_valid():
    with pytest.raises(TypeError):
        make_price_betfair_valid(2.0, "foo")

    assert make_price_betfair_valid(1.005, Side.LAY) is None
    assert make_price_betfair_valid(2000, Side.BACK) is None

    for price in BETFAIR_PRICES:
        prev_price = decrement_price(price)
        next_price = increment_price(price)

        if price != 1000:
            assert make_price_betfair_valid(price + 0.001, Side.BACK) == next_price
        if price != 1.01:
            assert make_price_betfair_valid(price - 0.001, Side.LAY) == prev_price


@pytest.mark.parametrize("use_runner_book_objects", [False, True])
def test_is_market_contiguous(runner: Dict[str, Any], use_runner_book_objects: bool):
    assert (
        is_market_contiguous(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        is None
    )

    runner["ex"]["availableToBack"].append({"price": 1.03, "size": 1})

    assert (
        is_market_contiguous(
            RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
        )
        is None
    )

    runner["ex"]["availableToBack"].append({"price": 1.02, "size": 1})

    assert is_market_contiguous(
        RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
    )

    runner["ex"]["availableToBack"][1]["price"] = 1.01

    assert not is_market_contiguous(
        RunnerBook(**runner) if use_runner_book_objects else runner, Side.BACK
    )

    with pytest.raises(ValueError):
        is_market_contiguous(runner, Side.BACK, max_depth=0)


def test_virtualise_two_runner_price():
    assert virtualise_two_runner_price(1.01, Side.BACK) == 100
    assert virtualise_two_runner_price(1.01, Side.BACK, raw=True) == pytest.approx(101)

    assert virtualise_two_runner_price(1000, Side.LAY) == 1.01
    assert virtualise_two_runner_price(1000, Side.LAY, raw=True) == pytest.approx(
        1.001001
    )
