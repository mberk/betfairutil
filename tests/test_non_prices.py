import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import pytest
import smart_open
from betfairlightweight.resources import MarketBook
from betfairlightweight.resources import MarketCatalogue
from betfairlightweight.resources import MarketDefinition
from betfairlightweight.resources import RunnerBook
from pyrsistent import pmap

from betfairutil import calculate_book_percentage
from betfairutil import calculate_market_book_diff
from betfairutil import calculate_total_matched
from betfairutil import convert_yards_to_metres
from betfairutil import DataFrameFormatEnum
from betfairutil import does_market_book_contain_runner_names
from betfairutil import does_market_definition_contain_runner_names
from betfairutil import EX_KEYS
from betfairutil import filter_runners
from betfairutil import get_best_price_with_rollup
from betfairutil import get_final_market_definition_from_prices_file
from betfairutil import get_event_id_from_string
from betfairutil import get_market_books_from_prices_file
from betfairutil import get_market_id_from_string
from betfairutil import get_minimum_book_percentage_market_books_from_prices_file
from betfairutil import get_pre_event_volume_traded_from_prices_file
from betfairutil import get_race_change_from_race_file
from betfairutil import get_race_id_from_string
from betfairutil import get_runner_book_from_market_book
from betfairutil import get_selection_id_to_runner_name_map_from_market_catalogue
from betfairutil import get_race_distance_in_metres_from_race_card
from betfairutil import get_win_market_id_from_race_card
from betfairutil import get_winners_from_market_definition
from betfairutil import is_market_book
from betfairutil import is_runner_book
from betfairutil import iterate_other_active_runners
from betfairutil import market_book_to_data_frame
from betfairutil import prices_file_to_csv_file
from betfairutil import random_from_market_id
from betfairutil import read_prices_file
from betfairutil import read_race_file
from betfairutil import remove_bet_from_runner_book
from betfairutil import Side


@pytest.fixture
def race_card():
    return {
        "race": {
            "markets": [
                {"marketId": "924.123", "marketType": "WIN", "numberOfWinners": 1},
                {"marketId": "927.123", "marketType": "WIN", "numberOfWinners": 1},
                {"marketId": "1.123", "marketType": "PLACE", "numberOfWinners": 3},
                {"marketId": "1.456", "marketType": "WIN", "numberOfWinners": 1},
            ],
            "distance": 1000,
        }
    }


@pytest.fixture
def market_catalogue():
    return {
        "runners": [
            {
                "selectionId": 123,
                "runnerName": "foo",
                "sortPriority": 1,
                "status": "ACTIVE",
            },
            {
                "selectionId": 456,
                "runnerName": "bar",
                "sortPriority": 2,
                "status": "ACTIVE",
            },
        ]
    }


@pytest.fixture
def market_definition():
    return {
        "runners": [
            {"id": 123, "name": "foo", "sortPriority": 1, "status": "ACTIVE"},
            {"id": 456, "name": "bar", "sortPriority": 2, "status": "ACTIVE"},
        ],
        "betDelay": 5,
        "bettingType": "ODDS",
        "bspMarket": False,
        "bspReconciled": False,
        "complete": True,
        "crossMatching": True,
        "discountAllowed": True,
        "eventId": "789",
        "inPlay": True,
        "marketBaseRate": 5,
        "marketTime": "2022-04-03T14:00:00.000Z",
        "eventTypeId": "2",
        "numberOfActiveRunners": 2,
        "numberOfWinners": 1,
        "persistenceEnabled": True,
        "regulators": ["MR_INT"],
        "runnersVoidable": False,
        "status": "OPEN",
        "timezone": "GMT",
        "turnInPlayEnabled": True,
        "version": 123456789,
        "marketType": "MATCH_ODDS",
    }


@pytest.fixture
def market_book(market_definition: Dict[str, Any]):
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
                "lastPriceTraded": 1.98,
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
                "lastPriceTraded": None,
            },
        ],
        "marketDefinition": market_definition,
        "marketId": "1.123",
        "inplay": True,
        "publishTime": 1648994400000,
    }


@pytest.fixture
def race_change():
    return {
        "id": "31945198.2354",
        "mid": "1.207120593",
        "rpc": {
            "ft": 1670024521800,
            "g": "",
            "st": 0,
            "rt": 0,
            "spd": 0,
            "prg": 1106.4,
            "ord": [],
            "J": [],
        },
    }


def test_side():
    assert Side.LAY.other_side == Side.BACK


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
            assert (
                market_book_diff.get_size_changes(runner["selectionId"], ex_key) == {}
            )

    market_book_diff = calculate_market_book_diff(
        MarketBook(**market_book), MarketBook(**market_book)
    )
    for runner in market_book["runners"]:
        for ex_key in EX_KEYS:
            assert (
                market_book_diff.get_size_changes(runner["selectionId"], ex_key) == {}
            )

    previous_market_book = deepcopy(market_book)
    market_book["runners"][1]["ex"]["availableToBack"].pop()
    market_book_diff = calculate_market_book_diff(market_book, previous_market_book)
    assert market_book_diff.get_size_changes(123, "availableToBack") == {}
    assert market_book_diff.get_size_changes(123, "availableToLay") == {}
    assert market_book_diff.get_size_changes(123, "tradedVolume") == {}
    assert market_book_diff.get_size_changes(456, "availableToBack") == {1.98: -1}
    assert market_book_diff.get_size_changes(456, "availableToLay") == {}
    assert market_book_diff.get_size_changes(456, "tradedVolume") == {}

    market_book_diff = calculate_market_book_diff(
        MarketBook(**market_book), MarketBook(**previous_market_book)
    )
    assert market_book_diff.get_size_changes(123, "availableToBack") == {}
    assert market_book_diff.get_size_changes(123, "availableToLay") == {}
    assert market_book_diff.get_size_changes(123, "tradedVolume") == {}
    assert market_book_diff.get_size_changes(456, "availableToBack") == {1.98: -1}
    assert market_book_diff.get_size_changes(456, "availableToLay") == {}
    assert market_book_diff.get_size_changes(456, "tradedVolume") == {}


@pytest.mark.parametrize("use_market_book_objects", [False, True])
def test_calculate_total_matched(
    market_book: Dict[str, Any], use_market_book_objects: bool
):
    assert (
        calculate_total_matched(
            MarketBook(**market_book) if use_market_book_objects else market_book
        )
        == 0
    )


def test_does_market_book_contain_runner_names(market_book: Dict[str, Any]):
    assert does_market_book_contain_runner_names(market_book)
    assert not does_market_book_contain_runner_names(MarketBook(**market_book))
    assert does_market_book_contain_runner_names(
        MarketBook(**market_book, market_definition=market_book["marketDefinition"])
    )


def test_does_market_definition_contain_runner_names(market_definition: Dict[str, Any]):
    assert does_market_definition_contain_runner_names(
        MarketDefinition(**market_definition)
    )
    market_definition["runners"] = []
    assert not does_market_definition_contain_runner_names(
        MarketDefinition(**market_definition)
    )


def test_filter_runners(market_book: Dict[str, Any]):
    assert (
        len(
            list(
                filter_runners(market_book, status="REMOVED", excluded_selection_ids=[])
            )
        )
        == 0
    )
    assert (
        len(
            list(
                filter_runners(
                    market_book, status="ACTIVE", excluded_selection_ids=[123]
                )
            )
        )
        == 1
    )


def test_get_runner_book_from_market_book(market_book: Dict[str, Any]):
    with pytest.raises(ValueError):
        get_runner_book_from_market_book(
            market_book, selection_id=123, runner_name="foo"
        )

    with pytest.raises(TypeError):
        get_runner_book_from_market_book(market_book, selection_id=123, return_type=int)

    assert (
        type(
            get_runner_book_from_market_book(
                MarketBook(**market_book), selection_id=123
            )
        )
        is RunnerBook
    )
    assert get_runner_book_from_market_book(market_book, runner_name="alice") is None
    assert (
        get_runner_book_from_market_book(market_book, runner_name="bar")["selectionId"]
        == 456
    )
    assert (
        get_runner_book_from_market_book(
            MarketBook(**market_book), runner_name="bar"
        ).selection_id
        == 456
    )
    assert (
        get_runner_book_from_market_book(pmap(market_book), runner_name="bar")[
            "selectionId"
        ]
        == 456
    )

    assert get_runner_book_from_market_book(None, runner_name="bar") is None

    del market_book["runners"][0]["selectionId"]
    assert get_runner_book_from_market_book(market_book, selection_id=123) is None

    del market_book["marketDefinition"]
    assert get_runner_book_from_market_book(market_book, runner_name="bar") is None

    del market_book["runners"]
    assert get_runner_book_from_market_book(market_book, selection_id=123) is None


def test_get_best_price_with_rollup(market_book: Dict[str, Any]):
    runner_book = get_runner_book_from_market_book(market_book, 123)
    runner_book["ex"]["availableToBack"].append({"price": 1.97, "size": 1})
    assert get_best_price_with_rollup(runner_book, Side.BACK, 10) is None
    assert get_best_price_with_rollup(runner_book, Side.BACK, 1) == 1.98
    assert get_best_price_with_rollup(runner_book, Side.BACK, 2) == 1.97
    runner_book = RunnerBook(**runner_book)
    assert get_best_price_with_rollup(runner_book, Side.BACK, 10) is None
    assert get_best_price_with_rollup(runner_book, Side.BACK, 1) == 1.98
    assert get_best_price_with_rollup(runner_book, Side.BACK, 2) == 1.97


def test_get_market_id_from_string():
    assert (
        get_market_id_from_string("/srv/betfair-prices/horse-racing/1.196842297.gz")
        == "1.196842297"
    )
    assert (
        get_market_id_from_string(
            "/srv/betfair-prices/horse-racing/1.196842297.gz", as_integer=True
        )
        == 196842297
    )
    assert (
        get_market_id_from_string("/srv/betfair-races/31323606.2355.jsonl.gz") is None
    )


def test_get_event_id_from_string():
    assert (
        get_event_id_from_string("/srv/betfair-races/31323606.2355.jsonl.gz")
        == 31323606
    )


def test_get_race_id_from_string():
    assert (
        get_race_id_from_string("/srv/betfair-races/31323606.2355.jsonl.gz")
        == "31323606.2355"
    )
    assert (
        get_race_id_from_string("/srv/betfair-prices/horse-racing/1.196842297.gz")
        is None
    )


def test_get_selection_id_to_runner_name_map_from_market_catalogue(
    market_catalogue: Dict[str, Any]
):
    assert get_selection_id_to_runner_name_map_from_market_catalogue(
        market_catalogue
    ) == {123: "foo", 456: "bar"}
    assert get_selection_id_to_runner_name_map_from_market_catalogue(
        MarketCatalogue(**market_catalogue)
    ) == {123: "foo", 456: "bar"}


def test_convert_yards_to_metres():
    assert convert_yards_to_metres(None) is None


def test_get_race_distance_in_metres_from_race_card(race_card: Dict[str, Any]):
    assert get_race_distance_in_metres_from_race_card(race_card) == 914.4


def test_get_win_market_id_from_race_card(race_card: Dict[str, Any]):
    assert get_win_market_id_from_race_card(race_card) == "1.456"
    assert get_win_market_id_from_race_card(race_card, as_integer=True) == 456


def test_is_market_book(
    market_book: Dict[str, Any],
    market_catalogue: Dict[str, Any],
    race_card: Dict[str, Any],
):
    assert is_market_book(market_book)
    assert not is_market_book(market_catalogue)
    assert not is_market_book(race_card)

    assert is_market_book(MarketBook(**market_book))
    assert not is_market_book(MarketBook(**market_book).runners[0])


def test_is_runner_book(
    market_book: Dict[str, Any],
    market_catalogue: Dict[str, Any],
    race_card: Dict[str, Any],
):
    assert not is_runner_book(market_book)
    assert not is_runner_book(market_catalogue)
    assert not is_runner_book(race_card)

    assert is_runner_book(market_book["runners"][0])
    assert is_runner_book(MarketBook(**market_book).runners[0])


def test_iterate_other_active_runners(market_book: Dict[str, Any]):
    assert next(iterate_other_active_runners(market_book, 123))["selectionId"] == 456
    assert next(iterate_other_active_runners(market_book, 456))["selectionId"] == 123


def test_market_book_to_data_frame(market_book: Dict[str, Any]):
    df = market_book_to_data_frame(
        market_book, should_format_publish_time=True, should_output_runner_names=True
    )
    assert len(df) == 2
    assert df["publish_time"].iloc[0] == "2022-04-03T14:00:00.000000Z"
    assert df["publish_time"].iloc[1] == "2022-04-03T14:00:00.000000Z"
    assert df["runner_name"].iloc[0] == "foo"
    assert df["runner_name"].iloc[1] == "bar"

    df = market_book_to_data_frame(
        MarketBook(**market_book),
        should_format_publish_time=True,
        should_output_runner_names=True,
    )
    assert len(df) == 2
    assert df["publish_time"].iloc[0] == "2022-04-03T14:00:00.000000Z"
    assert df["publish_time"].iloc[1] == "2022-04-03T14:00:00.000000Z"
    assert df["runner_name"].iloc[0] == "foo"
    assert df["runner_name"].iloc[1] == "bar"


def test_prices_file_to_csv_file(
    market_definition: Dict[str, Any],
    market_book: Dict[str, Any],
    market_catalogue: Dict[str, Any],
    tmp_path: Path,
):
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    with smart_open.open(path_to_prices_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "atb": [[1.98, 1]]},
                                {"id": 456, "atb": [[1.98, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")
    path_to_csv_file = tmp_path / f"1.123.csv"
    prices_file_to_csv_file(
        path_to_prices_file,
        path_to_csv_file,
        should_output_runner_names=True,
        should_output_runner_statuses=True,
        should_format_publish_time=True,
        market_definition_fields={"marketType": "market_type"},
        market_catalogues=[market_catalogue],
    )
    pd.testing.assert_frame_equal(
        pd.read_csv(path_to_csv_file, dtype={"market_id": str, "handicap": "float64"}),
        market_book_to_data_frame(
            market_book,
            should_output_runner_names=True,
            should_output_runner_statuses=True,
            should_format_publish_time=True,
        ).assign(market_type="MATCH_ODDS"),
    )
    with smart_open.open(path_to_prices_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "ltp": 1.98},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")
    prices_file_to_csv_file(
        path_to_prices_file,
        path_to_csv_file,
        should_output_runner_names=True,
        should_output_runner_statuses=True,
        should_format_publish_time=True,
        market_definition_fields={"marketType": "market_type"},
        market_catalogues=[market_catalogue],
        _format=DataFrameFormatEnum.LAST_PRICE_TRADED,
    )
    pd.testing.assert_frame_equal(
        pd.read_csv(path_to_csv_file, dtype={"market_id": str, "handicap": "float64"}),
        market_book_to_data_frame(
            market_book,
            should_output_runner_names=True,
            should_output_runner_statuses=True,
            should_format_publish_time=True,
            _format=DataFrameFormatEnum.LAST_PRICE_TRADED,
        ).assign(market_type="MATCH_ODDS"),
    )


def test_read_prices_file(
    market_definition: Dict[str, Any],
    market_book: Dict[str, Any],
    market_catalogue: Dict[str, Any],
    tmp_path: Path,
):
    del market_definition["runners"][0]["name"]
    del market_definition["runners"][1]["name"]
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    with smart_open.open(path_to_prices_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "atb": [[1.98, 1]]},
                                {"id": 456, "atb": [[1.98, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")
    market_books = read_prices_file(
        path_to_prices_file, market_catalogues=[market_catalogue]
    )
    assert len(market_books) == 1

    market_books = read_prices_file(
        path_to_prices_file, lightweight=False, market_catalogues=[market_catalogue]
    )
    assert len(market_books) == 1


def test_read_race_file(tmp_path: Path):
    path_to_race_file = tmp_path / "31323606.2355.jsonl.gz"
    rc = {
        "id": "31323606.2355",
        "mid": "1.196610303",
        "rrc": [
            {
                "ft": 1648165718800,
                "id": 12883263,
                "long": -95.5316312,
                "lat": 29.9292254,
                "spd": 0.11,
                "prg": 1609.3,
                "sfq": 0,
            },
            {
                "ft": 1648165718800,
                "id": 38368882,
                "long": -95.5315912,
                "lat": 29.9292075,
                "spd": 0.16,
                "prg": 1609.3,
                "sfq": 0,
            },
            {
                "ft": 1648165718800,
                "id": 40957918,
                "long": -95.5316203,
                "lat": 29.9291875,
                "spd": 0.11,
                "prg": 1609.3,
                "sfq": 0,
            },
            {
                "ft": 1648165718800,
                "id": 39605474,
                "long": -95.5317173,
                "lat": 29.9292874,
                "spd": 0.86,
                "prg": 1609.3,
                "sfq": 0,
            },
            {
                "ft": 1648165718800,
                "id": 39605475,
                "long": -95.5316561,
                "lat": 29.9291837,
                "spd": 1.14,
                "prg": 1609.3,
                "sfq": 0,
            },
        ],
    }
    with smart_open.open(path_to_race_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "rcm",
                    "id": 2,
                    "clk": "3097631015614646762",
                    "pt": 1648165718972,
                    "rc": [rc],
                }
            )
        )
        f.write("\n")
    rcs = read_race_file(path_to_race_file)
    assert len(rcs) == 1

    del rcs[0]["pt"]
    del rcs[0]["rpc"]
    del rcs[0]["streaming_snap"]
    del rcs[0]["streaming_unique_id"]
    del rcs[0]["streaming_update"]

    assert rcs[0] == rc


def test_remove_bet_from_runner_book(market_book: Dict[str, Any]):
    runner_book = market_book["runners"][0]
    with pytest.raises(ValueError):
        remove_bet_from_runner_book(
            runner_book, price=1.98, size=2, available_side=Side.BACK
        )
    with pytest.raises(ValueError):
        remove_bet_from_runner_book(
            RunnerBook(**runner_book), price=1.98, size=2, available_side=Side.BACK
        )

    new_runner_book = remove_bet_from_runner_book(
        runner_book, price=1.98, size=1, available_side=Side.BACK
    )
    assert len(new_runner_book["ex"]["availableToBack"]) == 0

    new_runner_book = remove_bet_from_runner_book(
        RunnerBook(**runner_book), price=1.98, size=1, available_side=Side.BACK
    )
    assert len(new_runner_book.ex.available_to_back) == 0


def test_random_from_market_id():
    with pytest.raises(ValueError):
        random_from_market_id(-1)

    with pytest.raises(ValueError):
        random_from_market_id(0.5)

    assert random_from_market_id("1.123") == 0.5181806162370606


def test_get_final_market_definition_from_prices_file(
    market_definition: Dict[str, Any],
    tmp_path: Path,
):
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    with smart_open.open(path_to_prices_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": 0,
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                        }
                    ],
                }
            )
        )
        f.write("\n")

    final_market_definition = get_final_market_definition_from_prices_file(
        path_to_prices_file
    )
    assert final_market_definition == market_definition

    with smart_open.open(path_to_prices_file, "w") as f:
        f.write("")

    final_market_definition = get_final_market_definition_from_prices_file(
        path_to_prices_file
    )
    assert final_market_definition is None


def test_get_pre_event_volume_traded_from_prices_file(
    market_definition: Dict[str, Any],
    market_book: Dict[str, Any],
    tmp_path: Path,
):
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    with smart_open.open(path_to_prices_file, "w") as f:
        market_definition["inPlay"] = False
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "trd": [[1.98, 1]]},
                                {"id": 456, "trd": [[1.98, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")
        market_definition["inPlay"] = True
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [],
                        }
                    ],
                }
            )
        )
        f.write("\n")
    volume_traded = get_pre_event_volume_traded_from_prices_file(path_to_prices_file)
    assert volume_traded == 2


def test_get_winners_from_market_definition(market_definition: Dict[str, Any]):
    assert get_winners_from_market_definition(market_definition) == []

    market_definition["runners"][0]["status"] = "WINNER"
    assert get_winners_from_market_definition(market_definition) == [
        market_definition["runners"][0]["id"]
    ]


def test_get_race_change_from_race_file(race_change: Dict[str, Any], tmp_path: Path):
    path_to_race_file = tmp_path / f"31945198.2354.jsonl.gz"
    with smart_open.open(path_to_race_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "rcm",
                    "clk": 0,
                    "pt": race_change["rpc"]["ft"],
                    "rc": [race_change],
                }
            )
        )
        f.write("\n")
    with pytest.raises(AssertionError):
        get_race_change_from_race_file(
            path_to_race_file, gate_name="1f", publish_time=race_change["rpc"]["ft"]
        )

    assert get_race_change_from_race_file(path_to_race_file, gate_name="1f") is None
    assert (
        get_race_change_from_race_file(
            path_to_race_file, publish_time=race_change["rpc"]["ft"] + 1
        )
        is None
    )

    assert (
        get_race_change_from_race_file(path_to_race_file, gate_name="")["rpc"]
        == race_change["rpc"]
    )
    assert (
        get_race_change_from_race_file(
            path_to_race_file, publish_time=race_change["rpc"]["ft"]
        )["rpc"]
        == race_change["rpc"]
    )


def test_get_market_books_from_prices_file(
    market_definition: Dict[str, Any],
    market_book: Dict[str, Any],
    market_catalogue: Dict[str, Any],
    tmp_path: Path,
):
    del market_definition["runners"][0]["name"]
    del market_definition["runners"][1]["name"]
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    with smart_open.open(path_to_prices_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "atb": [[1.98, 1]]},
                                {"id": 456, "atb": [[1.98, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")

    assert (
        get_market_books_from_prices_file(path_to_prices_file, publish_times=[]) == {}
    )
    assert get_market_books_from_prices_file(
        path_to_prices_file, publish_times=[0]
    ) == {0: None}
    assert (
        get_market_books_from_prices_file(
            path_to_prices_file, publish_times=[market_book["publishTime"]]
        )[market_book["publishTime"]]
        is not None
    )


def test_get_minimum_book_percentage_market_books_from_prices_file(
    market_definition: Dict[str, Any],
    market_book: Dict[str, Any],
    market_catalogue: Dict[str, Any],
    tmp_path: Path,
):
    del market_definition["runners"][0]["name"]
    del market_definition["runners"][1]["name"]
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    with smart_open.open(path_to_prices_file, "w") as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"],
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "atb": [[1.98, 1]]},
                                {"id": 456, "atb": [[1.98, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": market_book["publishTime"] + 50,
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": [
                                {"id": 123, "atb": [[1.99, 1]]},
                                {"id": 456, "atb": [[1.99, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")

    with pytest.raises(AssertionError):
        get_minimum_book_percentage_market_books_from_prices_file(
            path_to_prices_file, publish_time_windows=[(1, 0)]
        )

    assert (
        get_minimum_book_percentage_market_books_from_prices_file(
            path_to_prices_file, publish_time_windows=[]
        )
        == {}
    )
    assert get_minimum_book_percentage_market_books_from_prices_file(
        path_to_prices_file, publish_time_windows=[(0, 1)]
    ) == {(0, 1): None}
    assert (
        get_minimum_book_percentage_market_books_from_prices_file(
            path_to_prices_file, publish_time_windows=[(0, market_book["publishTime"])]
        )[(0, market_book["publishTime"])]
        is not None
    )
    assert (
        get_minimum_book_percentage_market_books_from_prices_file(
            path_to_prices_file,
            publish_time_windows=[(0, market_book["publishTime"] + 50)],
        )[(0, market_book["publishTime"] + 50)]
        is not None
    )
