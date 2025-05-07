import datetime
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pandas as pd
import pytest
import smart_open
from betfairlightweight.resources import MarketBook
from betfairlightweight.resources import MarketCatalogue
from betfairlightweight.resources import MarketDefinition
from betfairlightweight.resources import RunnerBook
from pyrsistent import pmap

from betfairutil import calculate_available_volume
from betfairutil import calculate_book_percentage
from betfairutil import calculate_haversine_distance_between_runners
from betfairutil import calculate_market_book_diff
from betfairutil import calculate_order_book_imbalance
from betfairutil import calculate_total_matched
from betfairutil import convert_yards_to_metres
from betfairutil import create_combined_market_book_and_race_change_generator
from betfairutil import DataFrameFormatEnum
from betfairutil import datetime_to_publish_time
from betfairutil import does_market_book_contain_runner_names
from betfairutil import does_market_definition_contain_runner_names
from betfairutil import EX_KEYS
from betfairutil import filter_runners
from betfairutil import get_all_market_definitions_from_prices_file
from betfairutil import get_best_price_with_rollup
from betfairutil import get_bsp_from_market_definition
from betfairutil import get_bsp_from_prices_file
from betfairutil import get_bsp_from_race_result
from betfairutil import get_event_id_from_string
from betfairutil import get_final_market_definition_from_prices_file
from betfairutil import get_first_market_definition_from_prices_file
from betfairutil import get_inplay_publish_time_from_prices_file
from betfairutil import get_inplay_bet_delay_from_prices_file
from betfairutil import get_is_jump_from_race_card
from betfairutil import get_market_books_from_prices_file
from betfairutil import get_market_id_from_string
from betfairutil import get_market_time_as_datetime
from betfairutil import get_mid_price
from betfairutil import get_minimum_book_percentage_market_books_from_prices_file
from betfairutil import get_number_of_jumps_remaining
from betfairutil import get_pre_event_volume_traded_from_prices_file
from betfairutil import get_race_change_from_race_file
from betfairutil import get_race_distance_in_metres_from_race_card
from betfairutil import get_race_id_from_string
from betfairutil import get_race_leaders
from betfairutil import get_runner_book_from_market_book
from betfairutil import get_second_best_price
from betfairutil import get_second_best_price_size
from betfairutil import get_seconds_to_market_time
from betfairutil import get_selection_id_to_runner_name_map_from_market_catalogue
from betfairutil import get_total_volume_traded_from_prices_file
from betfairutil import get_win_market_id_from_race_card
from betfairutil import get_win_market_id_from_race_file
from betfairutil import get_winners_from_market_definition
from betfairutil import get_winners_from_prices_file
from betfairutil import get_winners_from_race_result
from betfairutil import is_market_book
from betfairutil import is_runner_book
from betfairutil import iterate_other_active_runners
from betfairutil import market_book_to_data_frame
from betfairutil import prices_file_to_csv_file
from betfairutil import publish_time_to_datetime
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
            "raceType": {"full": "Chase"},
        }
    }


@pytest.fixture
def race_result():
    return {
        "runners": [
            {
                "horseName": "foo",
                "position": "1",
                "selections": [
                    {"marketId": "924.123", "marketType": "WIN", "selectionId": "123"},
                    {"marketId": "927.123", "marketType": "WIN", "selectionId": "123"},
                    {
                        "marketId": "1.123",
                        "marketType": "WIN",
                        "selectionId": "123",
                        "bsp": 1.5,
                    },
                ],
            },
            {
                "horseName": "bar",
                "position": "2",
                "selections": [
                    {"marketId": "924.123", "marketType": "WIN", "selectionId": "456"},
                    {"marketId": "927.123", "marketType": "WIN", "selectionId": "456"},
                    {
                        "marketId": "1.123",
                        "marketType": "WIN",
                        "selectionId": "456",
                        "bsp": 3.0,
                    },
                ],
            },
        ]
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
def market_book(market_definition: dict[str, Any]):
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
        "rrc": [
            {
                "ft": 1670024522300,
                "id": 50749188,
                "long": -76.6608639,
                "lat": 40.3955184,
                "prg": 1106.4,
            }
        ],
    }


@pytest.fixture
def path_to_race_result_file(race_result: dict[str, Any], tmp_path: Path):
    path_to_race_result_file = tmp_path / "race-result.gz"
    with smart_open.open(path_to_race_result_file, "w") as f:
        f.write(json.dumps(race_result))

    return path_to_race_result_file


@pytest.fixture
def path_to_race_card_file(race_card: dict[str, Any], tmp_path: Path):
    path_to_race_card_file = tmp_path / "race-card.gz"
    with smart_open.open(path_to_race_card_file, "w") as f:
        f.write(json.dumps(race_card))

    return path_to_race_card_file


@pytest.fixture
def path_to_race_file(race_change: dict[str, Any], tmp_path: Path):
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

    return path_to_race_file


def write_to_prices_file(
    publish_time: int,
    market_definition: dict[str, Any],
    rc: list[dict[str, Any]],
    path_to_prices_file: Path,
    mode: str,
) -> None:
    with smart_open.open(path_to_prices_file, mode) as f:
        f.write(
            json.dumps(
                {
                    "op": "mcm",
                    "clk": 0,
                    "pt": publish_time,
                    "mc": [
                        {
                            "id": "1.123",
                            "marketDefinition": market_definition,
                            "rc": rc,
                        }
                    ],
                }
            )
        )
        f.write("\n")


@pytest.fixture
def path_to_prices_file(
    market_book: dict[str, Any], market_definition: dict[str, Any], tmp_path: Path
):
    del market_definition["runners"][0]["name"]
    del market_definition["runners"][1]["name"]
    path_to_prices_file = tmp_path / f"1.123.json.gz"
    write_to_prices_file(
        publish_time=market_book["publishTime"],
        market_definition=market_definition,
        rc=[
            {"id": 123, "atb": [[1.98, 1]]},
            {"id": 456, "atb": [[1.98, 1]]},
        ],
        path_to_prices_file=path_to_prices_file,
        mode="w",
    )

    return path_to_prices_file


@pytest.fixture
def path_to_prices_file_with_inplay_transition(
    market_book: dict[str, Any], market_definition: dict[str, Any], tmp_path: Path
):
    path_to_prices_file = tmp_path / f"1.123-inplay-transition.json.gz"
    market_definition["inPlay"] = False
    write_to_prices_file(
        publish_time=market_book["publishTime"],
        market_definition=market_definition,
        rc=[
            {"id": 123, "trd": [[1.98, 1]]},
            {"id": 456, "trd": [[1.98, 1]]},
        ],
        path_to_prices_file=path_to_prices_file,
        mode="w",
    )
    market_definition["inPlay"] = True
    write_to_prices_file(
        publish_time=market_book["publishTime"],
        market_definition=market_definition,
        rc=[],
        path_to_prices_file=path_to_prices_file,
        mode="a",
    )

    return path_to_prices_file


@pytest.fixture
def path_to_prices_file_with_turn_in_play_disabled(
    market_book: dict[str, Any], market_definition: dict[str, Any], tmp_path: Path
):
    path_to_prices_file = tmp_path / f"1.123-turn-in-play-disabled.json.gz"
    market_definition["turnInPlayEnabled"] = False
    market_definition["inPlay"] = False
    write_to_prices_file(
        publish_time=market_book["publishTime"],
        market_definition=market_definition,
        rc=[
            {"id": 123, "trd": [[1.98, 1]]},
            {"id": 456, "trd": [[1.98, 1]]},
        ],
        path_to_prices_file=path_to_prices_file,
        mode="w",
    )
    return path_to_prices_file


def test_side():
    assert Side.LAY.other_side == Side.BACK


@pytest.mark.parametrize("use_market_book_objects", [False, True])
def test_calculate_book_percentage(
    market_book: dict[str, Any], use_market_book_objects: bool
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


def test_calculate_market_book_diff(market_book: dict[str, Any]):
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
    market_book: dict[str, Any], use_market_book_objects: bool
):
    assert (
        calculate_total_matched(
            MarketBook(**market_book) if use_market_book_objects else market_book
        )
        == 0
    )


def test_does_market_book_contain_runner_names(market_book: dict[str, Any]):
    assert does_market_book_contain_runner_names(market_book)
    assert not does_market_book_contain_runner_names(MarketBook(**market_book))
    assert does_market_book_contain_runner_names(
        MarketBook(
            **market_book,
            market_definition=MarketDefinition(**market_book["marketDefinition"]),
        )
    )


def test_does_market_definition_contain_runner_names(market_definition: dict[str, Any]):
    assert does_market_definition_contain_runner_names(
        MarketDefinition(**market_definition)
    )
    market_definition["runners"] = []
    assert not does_market_definition_contain_runner_names(
        MarketDefinition(**market_definition)
    )


def test_filter_runners(market_book: dict[str, Any]):
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


def test_get_runner_book_from_market_book(market_book: dict[str, Any]):
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


def test_get_best_price_with_rollup(market_book: dict[str, Any]):
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
    market_catalogue: dict[str, Any]
):
    assert get_selection_id_to_runner_name_map_from_market_catalogue(
        market_catalogue
    ) == {123: "foo", 456: "bar"}
    assert get_selection_id_to_runner_name_map_from_market_catalogue(
        MarketCatalogue(**market_catalogue)
    ) == {123: "foo", 456: "bar"}


def test_convert_yards_to_metres():
    assert convert_yards_to_metres(None) is None


def test_get_race_distance_in_metres_from_race_card(
    race_card: dict[str, Any], path_to_race_card_file: Path
):
    assert get_race_distance_in_metres_from_race_card(race_card) == 914.4
    assert get_race_distance_in_metres_from_race_card(path_to_race_card_file) == 914.4


def test_get_win_market_id_from_race_card(
    race_card: dict[str, Any], path_to_race_card_file: Path
):
    assert get_win_market_id_from_race_card(race_card) == "1.456"
    assert get_win_market_id_from_race_card(race_card, as_integer=True) == 456

    assert get_win_market_id_from_race_card(path_to_race_card_file) == "1.456"
    assert (
        get_win_market_id_from_race_card(path_to_race_card_file, as_integer=True) == 456
    )


def test_is_market_book(
    market_book: dict[str, Any],
    market_catalogue: dict[str, Any],
    race_card: dict[str, Any],
):
    assert is_market_book(market_book)
    assert not is_market_book(market_catalogue)
    assert not is_market_book(race_card)

    assert is_market_book(MarketBook(**market_book))
    assert not is_market_book(MarketBook(**market_book).runners[0])


def test_is_runner_book(
    market_book: dict[str, Any],
    market_catalogue: dict[str, Any],
    race_card: dict[str, Any],
):
    assert not is_runner_book(market_book)
    assert not is_runner_book(market_catalogue)
    assert not is_runner_book(race_card)

    assert is_runner_book(market_book["runners"][0])
    assert is_runner_book(MarketBook(**market_book).runners[0])


def test_iterate_other_active_runners(market_book: dict[str, Any]):
    assert next(iterate_other_active_runners(market_book, 123))["selectionId"] == 456
    assert next(iterate_other_active_runners(market_book, 456))["selectionId"] == 123


def test_market_book_to_data_frame(market_book: dict[str, Any]):
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
    market_definition: dict[str, Any],
    market_book: dict[str, Any],
    market_catalogue: dict[str, Any],
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
    path_to_prices_file: Path,
    market_catalogue: dict[str, Any],
):
    market_books = read_prices_file(
        path_to_prices_file, market_catalogues=[market_catalogue]
    )
    assert len(market_books) == 1

    market_books = read_prices_file(
        path_to_prices_file, lightweight=False, market_catalogues=[market_catalogue]
    )
    assert len(market_books) == 1


def test_read_race_file(race_change: dict[str, Any], path_to_race_file: Path):
    rcs = read_race_file(path_to_race_file)
    assert len(rcs) == 1

    del rcs[0]["pt"]
    del rcs[0]["streaming_snap"]
    del rcs[0]["streaming_unique_id"]
    del rcs[0]["streaming_update"]

    assert rcs[0] == race_change


def test_remove_bet_from_runner_book(market_book: dict[str, Any]):
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
        runner_book, price=1.98, size=0.75, available_side=Side.BACK
    )
    assert new_runner_book["ex"]["availableToBack"][0]["price"] == 1.98
    assert new_runner_book["ex"]["availableToBack"][0]["size"] == 0.25

    new_runner_book = remove_bet_from_runner_book(
        RunnerBook(**runner_book), price=1.98, size=1, available_side=Side.BACK
    )
    assert len(new_runner_book.ex.available_to_back) == 0

    new_runner_book = remove_bet_from_runner_book(
        RunnerBook(**runner_book), price=1.98, size=0.75, available_side=Side.BACK
    )
    assert new_runner_book.ex.available_to_back[0].price == 1.98
    assert new_runner_book.ex.available_to_back[0].size == 0.25

    original_price_sizes = runner_book["ex"]["availableToBack"]
    runner_book["ex"]["availableToBack"] = []
    new_runner_book = remove_bet_from_runner_book(
        runner_book, price=1.98, size=1, available_side=Side.BACK
    )
    assert new_runner_book == runner_book

    runner_book = RunnerBook(**runner_book)
    runner_book.ex.available_to_back = original_price_sizes
    new_runner_book = remove_bet_from_runner_book(
        runner_book, price=1.98, size=1, available_side=Side.BACK
    )
    assert len(new_runner_book.ex.available_to_back) == 0

    new_runner_book = remove_bet_from_runner_book(
        runner_book, price=1.98, size=0.75, available_side=Side.BACK
    )
    assert new_runner_book.ex.available_to_back[0]["price"] == 1.98
    assert new_runner_book.ex.available_to_back[0]["size"] == 0.25


def test_random_from_market_id():
    with pytest.raises(ValueError):
        random_from_market_id(-1)

    with pytest.raises(ValueError):
        random_from_market_id(0.5)

    assert random_from_market_id("1.123") == 0.5181806162370606


def test_get_final_market_definition_from_prices_file(
    market_definition: dict[str, Any], path_to_prices_file: Path
):
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
    path_to_prices_file_with_inplay_transition: Path,
    path_to_prices_file_with_turn_in_play_disabled: Path,
):
    volume_traded = get_pre_event_volume_traded_from_prices_file(
        path_to_prices_file_with_inplay_transition
    )
    assert volume_traded == 2

    volume_traded = get_pre_event_volume_traded_from_prices_file(
        path_to_prices_file_with_turn_in_play_disabled
    )
    assert volume_traded == 2


def test_get_winners_from_market_definition(market_definition: dict[str, Any]):
    assert get_winners_from_market_definition(market_definition) == []
    assert (
        get_winners_from_market_definition(MarketDefinition(**market_definition)) == []
    )

    market_definition["runners"][0]["status"] = "WINNER"
    assert get_winners_from_market_definition(market_definition) == [
        market_definition["runners"][0]["id"]
    ]
    assert get_winners_from_market_definition(
        MarketDefinition(**market_definition)
    ) == [market_definition["runners"][0]["id"]]


def test_get_race_change_from_race_file(
    race_change: dict[str, Any], path_to_race_file: Path
):
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
    market_definition: dict[str, Any],
    market_book: dict[str, Any],
    market_catalogue: dict[str, Any],
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
    market_definition: dict[str, Any],
    market_book: dict[str, Any],
    market_catalogue: dict[str, Any],
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


def test_calculate_order_book_imbalance(market_book: dict[str, Any]):
    runner_book = market_book["runners"][0]
    runner_book["ex"]["availableToLay"].append({"price": 1.99, "size": 2})

    assert calculate_order_book_imbalance(runner_book) == -1.0 / 3.0
    assert calculate_order_book_imbalance(RunnerBook(**runner_book)) == -1.0 / 3.0


def test_get_second_best_price_size(market_book: dict[str, Any]):
    runner_book = market_book["runners"][0]

    assert get_second_best_price_size(runner_book, Side.BACK) is None
    assert get_second_best_price_size(RunnerBook(**runner_book), Side.BACK) is None

    runner_book["ex"]["availableToBack"].append({"price": 1.97, "size": 2})

    assert get_second_best_price_size(runner_book, Side.BACK) == {
        "price": 1.97,
        "size": 2,
    }

    second_best_price_size = get_second_best_price_size(
        RunnerBook(**runner_book), Side.BACK
    )
    assert second_best_price_size.price == 1.97
    assert second_best_price_size.size == 2


def test_get_second_best_price(market_book: dict[str, Any]):
    runner_book = market_book["runners"][0]

    assert get_second_best_price(runner_book, Side.BACK) is None
    assert get_second_best_price(RunnerBook(**runner_book), Side.BACK) is None

    runner_book["ex"]["availableToBack"].append({"price": 1.97, "size": 2})

    assert get_second_best_price(runner_book, Side.BACK) == 1.97
    assert get_second_best_price(RunnerBook(**runner_book), Side.BACK) == 1.97


def test_get_market_time_as_datetime(market_book: dict[str, Any]):
    expected = datetime.datetime(
        year=2022,
        month=4,
        day=3,
        hour=14,
        minute=0,
        second=0,
        tzinfo=datetime.timezone.utc,
    )
    assert get_market_time_as_datetime(market_book) == expected
    assert (
        get_market_time_as_datetime(MarketDefinition(**market_book["marketDefinition"]))
        == expected
    )
    assert (
        get_market_time_as_datetime(
            MarketBook(
                **market_book,
                market_definition=MarketDefinition(**market_book["marketDefinition"]),
            )
        )
        == expected
    )


def test_get_seconds_to_market_time(market_book: dict[str, Any]):
    current_time = datetime.datetime.strptime(
        "2022-01-01T00:00:00.000Z", "%Y-%m-%dT%H:%M:%S.000Z"
    ).replace(tzinfo=datetime.timezone.utc)
    assert get_seconds_to_market_time(market_book, current_time) == 7999200.0
    assert (
        get_seconds_to_market_time(market_book, int(current_time.timestamp() * 1000))
        == 7999200.0
    )

    assert get_seconds_to_market_time(market_book) == 0.0
    assert (
        get_seconds_to_market_time(
            MarketBook(
                **market_book,
                market_definition=MarketDefinition(**market_book["marketDefinition"]),
            )
        )
        == 0.0
    )

    with pytest.raises(ValueError):
        get_seconds_to_market_time(MarketDefinition(**market_book["marketDefinition"]))


def test_get_win_market_id_from_race_file(
    race_change: dict[str, Any], path_to_race_file: Path
):
    assert get_win_market_id_from_race_file(path_to_race_file) == race_change["mid"]

    with smart_open.open(path_to_race_file, "w"):
        pass

    assert get_win_market_id_from_race_file(path_to_race_file) is None


def test_create_combined_market_book_and_race_change_generator(
    path_to_race_file: Path, path_to_prices_file: Path
):
    pairs = list(
        create_combined_market_book_and_race_change_generator(
            path_to_prices_file=path_to_prices_file,
            path_to_race_file=path_to_race_file,
            lightweight=False,
        )
    )

    _is_market_book, _object = pairs[0]
    assert _is_market_book is True
    assert type(_object) is MarketBook

    _is_market_book, _object = pairs[1]
    assert _is_market_book is False
    assert type(_object) is dict
    assert not is_market_book(_object)


def test_get_bsp_from_race_result(
    race_result: dict[str, Any], path_to_race_result_file: Path
):
    assert get_bsp_from_race_result(race_result) == {123: 1.5, 456: 3.0}
    assert get_bsp_from_race_result(path_to_race_result_file) == {123: 1.5, 456: 3.0}


def test_get_winners_from_race_result(
    race_result: dict[str, Any], path_to_race_result_file: Path
):
    assert get_winners_from_race_result(race_result) == [123]
    assert get_winners_from_race_result(path_to_race_result_file) == [123]


def test_get_bsp_from_market_definition(market_definition: dict[str, Any]):
    assert get_bsp_from_market_definition(market_definition) == {123: None, 456: None}
    assert get_bsp_from_market_definition(MarketDefinition(**market_definition)) == {
        123: None,
        456: None,
    }

    market_definition["runners"][0]["bsp"] = 1.5
    market_definition["runners"][1]["bsp"] = 3.0

    assert get_bsp_from_market_definition(market_definition) == {123: 1.5, 456: 3.0}
    assert get_bsp_from_market_definition(MarketDefinition(**market_definition)) == {
        123: 1.5,
        456: 3.0,
    }


def test_get_bsp_from_prices_file(path_to_prices_file: Path):
    assert get_bsp_from_prices_file(path_to_prices_file) == {123: None, 456: None}


def test_get_all_market_definitions_from_prices_file(
    path_to_prices_file: Path, market_definition: dict[str, Any]
):
    assert (
        get_all_market_definitions_from_prices_file(path_to_prices_file)[0][1]
        == market_definition
    )


def test_get_first_market_definition_from_prices_file(
    path_to_prices_file: Path, market_definition: dict[str, Any]
):
    assert (
        get_first_market_definition_from_prices_file(path_to_prices_file)
        == market_definition
    )


def test_get_winners_from_prices_file(path_to_prices_file: Path):
    assert get_winners_from_prices_file(path_to_prices_file) == []


def test_calculate_available_volume(market_book: dict[str, Any]):
    assert calculate_available_volume(market_book, Side.BACK, 1.05) == 2

    market_book["runners"][0]["ex"]["availableToBack"].append(
        {"price": 1.96, "size": 1}
    )
    market_book["runners"][1]["ex"]["availableToBack"].append(
        {"price": 1.96, "size": 1}
    )
    assert calculate_available_volume(market_book, Side.BACK, 1.05) == 4
    assert calculate_available_volume(market_book, Side.BACK, 1.02) == 2


def test_get_inplay_publish_time_from_prices_file(
    market_book: MarketBook,
    path_to_prices_file_with_inplay_transition: Path,
):
    assert (
        get_inplay_publish_time_from_prices_file(
            path_to_prices_file_with_inplay_transition
        )
        == market_book["publishTime"]
    )
    assert get_inplay_publish_time_from_prices_file(
        path_to_prices_file_with_inplay_transition, as_datetime=True
    ) == publish_time_to_datetime(market_book["publishTime"])

    # Empty file
    with smart_open.open(path_to_prices_file_with_inplay_transition, "w"):
        pass

    assert (
        get_inplay_publish_time_from_prices_file(
            path_to_prices_file_with_inplay_transition
        )
        is None
    )


def test_get_is_jump_from_race_card(race_card: dict[str, Any]):
    is_jump = get_is_jump_from_race_card(race_card)
    assert is_jump


def test_calculate_haversine_distance_between_runners(race_change: dict[str, Any]):
    haversine_distance = calculate_haversine_distance_between_runners(
        race_change["rrc"][0], race_change["rrc"][0]
    )

    assert haversine_distance == 0


def test_get_race_leaders(race_change: dict[str, Any]):
    race_leaders = get_race_leaders(race_change)
    assert race_leaders == {race_change["rrc"][0]["id"]}

    race_change["rrc"] = None
    race_leaders = get_race_leaders(race_change)
    assert race_leaders == set()


def test_get_number_of_jumps_remaining(race_change: dict[str, Any]):
    number_of_jumps_remaining = get_number_of_jumps_remaining(race_change)
    assert number_of_jumps_remaining is None

    race_change["rpc"]["J"] = [{"L": 1000}]
    number_of_jumps_remaining = get_number_of_jumps_remaining(race_change)
    assert number_of_jumps_remaining == 1


def test_get_mid_price(market_book: dict[str, Any]):
    runner_book = get_runner_book_from_market_book(market_book, selection_id=123)
    assert get_mid_price(runner_book) is None

    runner_book["ex"]["availableToLay"].append({"price": 1.99, "size": 1})
    assert get_mid_price(runner_book) == pytest.approx(1.985)


def test_get_total_volume_traded_from_prices_file(
    path_to_prices_file: Path,
    market_book: dict[str, Any],
    market_definition: dict[str, Any],
):
    total_volume_traded = get_total_volume_traded_from_prices_file(path_to_prices_file)
    assert total_volume_traded == 0

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
                                {"id": 123, "atb": [[1.98, 1]], "trd": [[1.99, 1]]},
                                {"id": 456, "atb": [[1.98, 1]]},
                            ],
                        }
                    ],
                }
            )
        )
        f.write("\n")
    total_volume_traded = get_total_volume_traded_from_prices_file(path_to_prices_file)
    assert total_volume_traded == 1

    for runner in market_definition["runners"]:
        runner["status"] = "REMOVED"
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
    total_volume_traded = get_total_volume_traded_from_prices_file(path_to_prices_file)
    assert total_volume_traded is None


def test_publish_time_to_datetime():
    assert publish_time_to_datetime(None) is None


def test_datetime_to_publish_time(market_book: dict[str, Any]):
    assert datetime_to_publish_time(None) is None
    assert (
        datetime_to_publish_time(publish_time_to_datetime(market_book["publishTime"]))
        == market_book["publishTime"]
    )


def test_get_inplay_bet_delay_from_prices_file(
    path_to_prices_file: Path,
):
    assert get_inplay_bet_delay_from_prices_file(path_to_prices_file) == 5
