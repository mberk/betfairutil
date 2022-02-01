import datetime
import enum
import itertools
import re
from bisect import bisect_left
from bisect import bisect_right
from copy import deepcopy
from math import sqrt
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union

import pandas as pd
from betfairlightweight import APIClient
from betfairlightweight import StreamListener
from betfairlightweight.resources.bettingresources import MarketBook
from betfairlightweight.resources.bettingresources import PriceSize
from betfairlightweight.resources.bettingresources import RunnerBook

BETFAIR_TICKS = [
    1.01,
    1.02,
    1.03,
    1.04,
    1.05,
    1.06,
    1.07,
    1.08,
    1.09,
    1.1,
    1.11,
    1.12,
    1.13,
    1.14,
    1.15,
    1.16,
    1.17,
    1.18,
    1.19,
    1.2,
    1.21,
    1.22,
    1.23,
    1.24,
    1.25,
    1.26,
    1.27,
    1.28,
    1.29,
    1.3,
    1.31,
    1.32,
    1.33,
    1.34,
    1.35,
    1.36,
    1.37,
    1.38,
    1.39,
    1.4,
    1.41,
    1.42,
    1.43,
    1.44,
    1.45,
    1.46,
    1.47,
    1.48,
    1.49,
    1.5,
    1.51,
    1.52,
    1.53,
    1.54,
    1.55,
    1.56,
    1.57,
    1.58,
    1.59,
    1.6,
    1.61,
    1.62,
    1.63,
    1.64,
    1.65,
    1.66,
    1.67,
    1.68,
    1.69,
    1.7,
    1.71,
    1.72,
    1.73,
    1.74,
    1.75,
    1.76,
    1.77,
    1.78,
    1.79,
    1.8,
    1.81,
    1.82,
    1.83,
    1.84,
    1.85,
    1.86,
    1.87,
    1.88,
    1.89,
    1.9,
    1.91,
    1.92,
    1.93,
    1.94,
    1.95,
    1.96,
    1.97,
    1.98,
    1.99,
    2,
    2.02,
    2.04,
    2.06,
    2.08,
    2.1,
    2.12,
    2.14,
    2.16,
    2.18,
    2.2,
    2.22,
    2.24,
    2.26,
    2.28,
    2.3,
    2.32,
    2.34,
    2.36,
    2.38,
    2.4,
    2.42,
    2.44,
    2.46,
    2.48,
    2.5,
    2.52,
    2.54,
    2.56,
    2.58,
    2.6,
    2.62,
    2.64,
    2.66,
    2.68,
    2.7,
    2.72,
    2.74,
    2.76,
    2.78,
    2.8,
    2.82,
    2.84,
    2.86,
    2.88,
    2.9,
    2.92,
    2.94,
    2.96,
    2.98,
    3,
    3.05,
    3.1,
    3.15,
    3.2,
    3.25,
    3.3,
    3.35,
    3.4,
    3.45,
    3.5,
    3.55,
    3.6,
    3.65,
    3.7,
    3.75,
    3.8,
    3.85,
    3.9,
    3.95,
    4,
    4.1,
    4.2,
    4.3,
    4.4,
    4.5,
    4.6,
    4.7,
    4.8,
    4.9,
    5,
    5.1,
    5.2,
    5.3,
    5.4,
    5.5,
    5.6,
    5.7,
    5.8,
    5.9,
    6,
    6.2,
    6.4,
    6.6,
    6.8,
    7,
    7.2,
    7.4,
    7.6,
    7.8,
    8,
    8.2,
    8.4,
    8.6,
    8.8,
    9,
    9.2,
    9.4,
    9.6,
    9.8,
    10,
    10.5,
    11,
    11.5,
    12,
    12.5,
    13,
    13.5,
    14,
    14.5,
    15,
    15.5,
    16,
    16.5,
    17,
    17.5,
    18,
    18.5,
    19,
    19.5,
    20,
    21,
    22,
    23,
    24,
    25,
    26,
    27,
    28,
    29,
    30,
    32,
    34,
    36,
    38,
    40,
    42,
    44,
    46,
    48,
    50,
    55,
    60,
    65,
    70,
    75,
    80,
    85,
    90,
    95,
    100,
    110,
    120,
    130,
    140,
    150,
    160,
    170,
    180,
    190,
    200,
    210,
    220,
    230,
    240,
    250,
    260,
    270,
    280,
    290,
    300,
    310,
    320,
    330,
    340,
    350,
    360,
    370,
    380,
    390,
    400,
    410,
    420,
    430,
    440,
    450,
    460,
    470,
    480,
    490,
    500,
    510,
    520,
    530,
    540,
    550,
    560,
    570,
    580,
    590,
    600,
    610,
    620,
    630,
    640,
    650,
    660,
    670,
    680,
    690,
    700,
    710,
    720,
    730,
    740,
    750,
    760,
    770,
    780,
    790,
    800,
    810,
    820,
    830,
    840,
    850,
    860,
    870,
    880,
    890,
    900,
    910,
    920,
    930,
    940,
    950,
    960,
    970,
    980,
    990,
    1000,
]
EX_KEYS = ["availableToBack", "availableToLay", "tradedVolume"]
MARKET_ID_PATTERN = re.compile(r"(1\.\d{9})")
_INVERSE_GOLDEN_RATIO = 2.0 / (1 + sqrt(5.0))


class Side(enum.Enum):
    BACK = "Back"
    LAY = "Lay"

    @property
    def other_side(self):
        if self is Side.BACK:
            return Side.LAY
        else:
            return Side.BACK

    @property
    def ex_key(self):
        return f"availableTo{self.value}"


class MarketBookDiff:
    def __init__(
        self,
        d: Dict[
            Tuple[int, Union[int, float]],
            Dict[str, Dict[Union[int, float], Union[int, float]]],
        ],
    ):
        self.d = d

    def get_size_changes(
        self, selection_id: int, ex_key: str, handicap: Union[int, float] = 0.0
    ) -> Optional[Dict[Union[int, float], Union[int, float]]]:
        return self.d[(selection_id, handicap)].get(ex_key)


def calculate_book_percentage(
    market_book: Union[Dict[str, Any], MarketBook], side: Side
) -> float:
    implied_probabilities = []
    for runner in iterate_active_runners(market_book):
        best_price_size = get_best_price_size(runner, side)
        if best_price_size is not None:
            if type(best_price_size) is PriceSize:
                best_price = best_price_size.price
            else:
                best_price = best_price_size["price"]
        else:
            best_price = None

        if best_price is not None:
            implied_probabilities.append(1.0 / best_price)
        else:
            if side is Side.BACK:
                other_best_price_size = get_best_price_size(runner, side.other_side)
                if other_best_price_size is not None:
                    implied_probabilities.append(1.0)

    return sum(implied_probabilities)


def calculate_market_book_diff(
    current_market_book: Union[Dict[str, Any], MarketBook],
    previous_market_book: Union[Dict[str, Any], MarketBook],
) -> MarketBookDiff:
    """
    Calculate the size differences between amounts available to back, available to lay, and traded between two market books

    :param current_market_book: The current market book to use in the comparison
    :param previous_market_book: The previous market book to use in the comparison
    :return: The complete set of size differences stored in a MarketBookDiff
    """
    if type(current_market_book) is MarketBook:
        current_market_book = current_market_book._data
    if type(previous_market_book) is MarketBook:
        previous_market_book = previous_market_book._data

    diff = {
        (runner["selectionId"], runner["handicap"]): {ex_key: {} for ex_key in EX_KEYS}
        for runner in current_market_book["runners"]
    }

    for current_runner in current_market_book["runners"]:
        previous_runner = get_runner_book_from_market_book(
            previous_market_book,
            current_runner["selectionId"],
            handicap=current_runner["handicap"],
        )
        if current_runner == previous_runner:
            continue

        for ex_key in EX_KEYS:
            previous_prices = {
                price_size["price"]: price_size["size"]
                for price_size in previous_runner.get("ex", {}).get(ex_key, [])
            }
            current_prices = {
                price_size["price"]: price_size["size"]
                for price_size in current_runner.get("ex", {}).get(ex_key, [])
            }
            all_prices = set(itertools.chain(previous_prices, current_prices))

            for price in all_prices:
                previous_size = previous_prices.get(price, 0)
                current_size = current_prices.get(price, 0)
                delta = round(current_size - previous_size, 2)

                diff[(current_runner["selectionId"], current_runner["handicap"])][
                    ex_key
                ][price] = delta

    return MarketBookDiff(diff)


def calculate_total_matched(
    market_book: Union[Dict[str, Any], MarketBook]
) -> Union[int, float]:
    """
    Calculate the total matched on this market from the amounts matched on each runner at each price point. Useful for historic data where this field is not populated

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :return: The total matched on this market
    """
    if type(market_book) is MarketBook:
        market_book = market_book._data

    return sum(
        ps["size"]
        for r in market_book.get("runners", [])
        for ps in r.get("ex", {}).get("tradedVolume", [])
    )


def filter_runners(
    market_book: Union[Dict[str, Any], MarketBook],
    status: str,
    excluded_selection_ids: Sequence[int],
) -> Generator[Union[Dict[str, Any], RunnerBook], None, None]:
    if type(market_book) is dict:
        runners = market_book["runners"]
        return_type = dict
    else:
        runners = market_book.runners
        return_type = RunnerBook

    for runner in runners:
        if runner["status"] != status:
            continue
        if runner["selectionId"] in excluded_selection_ids:
            continue
        yield return_type(**runner)


def get_runner_book_from_market_book(
    market_book: Union[Dict[str, Any], MarketBook],
    selection_id: Optional[int] = None,
    runner_name: Optional[str] = None,
    handicap: float = 0.0,
    return_type: Optional[type] = None,
) -> Optional[Union[Dict[str, Any], RunnerBook]]:
    """
    Extract a runner book from the given market book. The runner can be identified either by ID or name

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :param selection_id: Optionally identify the runner book to extract by the runner's ID
    :param runner_name: Alternatively identify the runner book to extract by the runner's name
    :param handicap: The handicap of the desired runner book
    :param return_type: Optionally specify the return type to be either a dict or RunnerBook. If not given then the return type will reflect the type of market_book; if market_book is a dictionary then the return value is a dictionary. If market_book is a MarketBook object then the return value will be a RunnerBook object
    :returns: The corresponding runner book if it can be found in the market book, otherwise None. The type of the return value will depend on the return_type parameter
    :raises: ValueError if both selection_id and runner_name are given. Only one is required to uniquely identify the runner book
    """
    if selection_id is not None and runner_name is not None:
        raise ValueError("Both selection_id and runner_name were given")
    if return_type is not None and not (
        return_type is dict or return_type is RunnerBook
    ):
        raise TypeError(
            f"return_type must be either dict or RunnerBook ({return_type} given)"
        )

    if type(market_book) is dict:
        return_type = return_type or dict
    else:
        market_book = market_book._data
        return_type = return_type or RunnerBook

    if selection_id is None:
        for runner in market_book["marketDefinition"]["runners"]:
            if runner["name"] == runner_name:
                selection_id = runner["id"]
                break
        if selection_id is None:
            return

    for runner in market_book["runners"]:
        if runner["selectionId"] == selection_id and runner["handicap"] == handicap:
            return return_type(**runner)


def get_best_price_size(
    runner: Union[Dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[Dict[str, Union[int, float]], PriceSize]]:
    if type(runner) is RunnerBook:
        return next(iter(getattr(runner.ex, side.ex_key)), None)
    else:
        return next(iter(runner.get("ex", {}).get(side.ex_key, [])), None)


def get_market_id_from_string(s: str, as_integer: bool = False) -> Union[str, int]:
    market_id = MARKET_ID_PATTERN.search(s).group(1)
    if as_integer:
        market_id = int(market_id[2:])
    return market_id


def is_market_book(x: Any) -> bool:
    """
    Test whether x is a betfairlightweight MarketBook object or a dictionary (mapping) with all required fields to construct one (as would be generated when using betfairlightweight in lightweight mode)

    :param x: The object to test
    :returns: True if x meets the above condition otherwise False
    """
    if type(x) is MarketBook:
        return True
    try:
        MarketBook(**x)
        return True
    except TypeError:
        return False


def is_runner_book(x: Any) -> bool:
    """
    Test whether x is a betfairlightweight RunnerBook object or a dictionary (mapping) with all required fields to construct one (as would be generated when using betfairlightweight in lightweight mode)

    :param x: The object to test
    :returns: True if x meets the above condition otherwise False
    """
    if type(x) is RunnerBook:
        return True
    try:
        RunnerBook(**x)
        return True
    except TypeError:
        return False


def iterate_active_runners(
    market_book: Union[Dict[str, Any], MarketBook]
) -> Generator[Union[Dict[str, Any], RunnerBook], None, None]:
    for runner in filter_runners(market_book, "ACTIVE", []):
        yield runner


def iterate_other_active_runners(
    market_book: Union[Dict[str, Any], MarketBook], selection_id: int
) -> Generator[Union[Dict[str, Any], RunnerBook], None, None]:
    for runner in iterate_active_runners(market_book):
        if runner["selectionId"] == selection_id:
            continue
        yield runner


def make_price_betfair_valid(
    price: Union[int, float], side: Side
) -> Optional[Union[int, float]]:
    if side == Side.BACK:
        fun = bisect_left
        offset = 0
    elif side == Side.LAY:
        fun = bisect_right
        offset = -1
    else:
        raise TypeError("side must be of type Side")

    index = fun(BETFAIR_TICKS, price) + offset

    if index < 0 or index > len(BETFAIR_TICKS) - 1:
        return None

    return BETFAIR_TICKS[index]


def market_book_to_data_frame(
    market_book: Union[Dict[str, Any], MarketBook],
    should_output_runner_names: bool = False,
    should_format_publish_time: bool = False,
    max_depth: Optional[int] = None,
) -> pd.DataFrame:
    """
    Construct a data frame representation of a market book. Each row is one point on the price ladder for a particular
    runner

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :param should_output_runner_names: Should the data frame contain a runner name column. This requires the market book to have been generated from streaming data and contain a MarketDefinition
    :param should_format_publish_time: Should the publish time (if present in the market book) be output as is (an integer number of milliseconds) or as an ISO 8601 formatted string
    :param max_depth: Optionally limit the depth of the price ladder
    :return: A data frame with the following columns:

      - market_id: The Betfair market ID
      - inplay: Whether the market is in play
      - selection_id: The selection ID of the runner
      - handicap: The handicap of the runner
      - side: Either 'Back' or 'Lay'
      - depth: The depth of this point on the ladder
      - price: The price of this point on the ladder
      - size: The amount of volume available at this point on the ladder
      - publish_time (Optional): If the market book was generated from streaming data (as opposed to calling the listMarketBook API endpoint) then the publish time of the market book. Otherwise this column will not be present
      - runner_name: (Optional): If should_output_runner_names is True then this column will be present. It will be populated if the market book was generated from streaming data (as opposed to calling the listMarketBook API endpoint) otherwise all entries will be None
    """
    if type(market_book) is MarketBook:
        market_book = market_book._data

    df = pd.DataFrame(
        {
            "market_id": market_book["marketId"],
            "inplay": market_book["inplay"],
            "selection_id": runner["selectionId"],
            "handicap": runner["handicap"],
            "side": side,
            "depth": depth,
            "price": price_size["price"],
            "size": price_size["size"],
        }
        for runner in market_book["runners"]
        for side in ["Back", "Lay"]
        for depth, price_size in enumerate(
            runner.get("ex", {}).get(f"availableTo{side}", [])
        )
        if max_depth is None or depth <= max_depth
    )

    if "publishTime" in market_book:
        publish_time = market_book["publishTime"]
        if should_format_publish_time:
            publish_time = publish_time_to_datetime(publish_time).strftime(
                "%Y-%m-%dT%H:%M:%S.%fZ"
            )
        df["publish_time"] = publish_time

    if should_output_runner_names:
        selection_id_to_runner_name_map = {
            runner["id"]: runner["name"]
            for runner in market_book.get("marketDefinition", {}).get("runners", [])
        }
        df["runner_name"] = df["selection_id"].apply(
            selection_id_to_runner_name_map.get
        )

    return df


def prices_file_to_csv_file(
    path_to_prices_file: str, path_to_csv_file: str, **kwargs
) -> None:
    prices_file_to_data_frame(path_to_prices_file, **kwargs).to_csv(
        path_to_csv_file, index=False
    )


def prices_file_to_data_frame(
    path_to_prices_file: str,
    should_output_runner_names: bool = False,
    should_format_publish_time: bool = False,
    max_depth: Optional[int] = None,
) -> pd.DataFrame:
    """
    Read a Betfair prices file (either from the official historic data or data recorded from the streaming API in the same format) directly into a data frame

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
    :param should_output_runner_names: Should the data frame contain a runner name column. For efficiency, the names are added once the entire file has been processed
    :param should_format_publish_time: Should the publish time be output as is (an integer number of milliseconds) or as an ISO 8601 formatted string. For efficiency, this formatting is applied once the entire file has been processed
    :param max_depth: Optionally limit the depth of the price ladder
    :return: A data frame where each row is one point on the price ladder for a particular runner at a particular publish time. The data frame has the following columns:

      - market_id: The Betfair market ID
      - inplay: Whether the market is in play
      - selection_id: The selection ID of the runner
      - handicap: The handicap of the runner
      - side: Either 'Back' or 'Lay'
      - depth: The depth of this point on the ladder
      - price: The price of this point on the ladder
      - size: The amount of volume available at this point on the ladder
      - publish_time: The publish time of the market book corresponding to this data point
      - runner_name: (Optional): If should_output_runner_names is True then this column will contain the name of the runner
    """
    import smart_open
    from unittest.mock import patch

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_prices_file,
        listener=StreamListener(
            max_latency=None, lightweight=True, update_clk=False
        ),
    )

    with patch("builtins.open", smart_open.open):
        g = stream.get_generator()
        first_market_book = next(g())[0]
        df = pd.concat(
            market_book_to_data_frame(mbs[0], max_depth=max_depth) for mbs in g()
        )
        if should_format_publish_time:
            df["publish_time"] = pd.to_datetime(
                df["publish_time"], unit="ms", utc=True
            ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if should_output_runner_names:
            selection_id_to_runner_name_map = {
                runner["id"]: runner["name"]
                for runner in first_market_book.get("marketDefinition", {}).get(
                    "runners", []
                )
            }
            df["runner_name"] = df["selection_id"].apply(
                selection_id_to_runner_name_map.get
            )
        # Fix integer column types
        df["selection_id"] = df["selection_id"].astype(int)
        df["depth"] = df["depth"].astype(int)
        return df


def publish_time_to_datetime(publish_time: int) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(publish_time / 1000).replace(
        tzinfo=datetime.timezone.utc
    )


def read_prices_file(
    path_to_prices_file: str, lightweight: bool = True
) -> Union[List[MarketBook], List[Dict[str, Any]]]:
    import smart_open
    from unittest.mock import patch

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_prices_file,
        listener=StreamListener(
            max_latency=None, lightweight=lightweight, update_clk=False
        ),
    )

    with patch("builtins.open", smart_open.open):
        g = stream.get_generator()
        return list(mbs[0] for mbs in g())


def remove_bet_from_runner_book(
    runner_book: Union[Dict[str, Any], RunnerBook],
    price: Union[int, float],
    size: Union[int, float],
    available_side: Side,
) -> Union[Dict[str, Any], RunnerBook]:
    """
    Create a new runner book with a bet removed from the order book

    :param runner_book: The runner book from which the bet is going to be removed either as a dictionary or betfairlightweight RunnerBook object
    :param price: The price of the bet
    :param size: The size of the bet
    :param available_side: The side of the order book that the bet appears on
    :return: A new runner book with the bet removed. The type of the return value will reflect the type of runner_book. If the given price is not available on the given side then the new runner book will be identical to runner_book
    :raises: ValueError if size is greater than the size present in the order book
    """
    runner_book = deepcopy(runner_book)
    if type(runner_book) is dict:
        for price_size in runner_book["ex"][available_side.ex_key]:
            if price_size["price"] == price and price_size["size"] < size:
                raise ValueError(
                    f'size = {size} but only {price_size["size"]} available to {available_side.ex_key} at {price_size["price"]}'
                )

        runner_book["ex"][available_side.ex_key] = [
            {
                "price": price_size["price"],
                "size": price_size["size"]
                - (size if price_size["price"] == price else 0),
            }
            for price_size in runner_book["ex"][available_side.ex_key]
            # If price_size['price'] == price and price_size['size'] == size then it should be removed from the list completely
            if price_size["price"] != price or price_size["size"] != size
        ]
    else:
        for price_size in getattr(runner_book.ex, available_side.ex_key):
            if price_size.price == price and price_size.size < size:
                raise ValueError(
                    f"size = {size} but only {price_size.size} available to {available_side.ex_key} at {price_size.price}"
                )

        setattr(
            runner_book.ex,
            available_side.ex_key,
            [
                PriceSize(price=price_size.price, size=price_size.size)
                for price_size in getattr(runner_book.ex, available_side.ex_key)
                if price_size.price != price or price_size.size != size
            ],
        )
    return runner_book


def random_from_market_id(market_id: Union[int, str]):
    """
    Maps a market ID to a real number in [0, 1)

    :param market_id: A market ID, either in the standard string form provided by Betfair that starts "1." or an integer where the "1." prefix has been discarded
    :return: A quasi-random number generated from the market ID. See random_from_positive_int for details
    """
    if type(market_id) is str:
        market_id = int(market_id[2:])

    return random_from_positive_int(market_id)


def random_from_positive_int(i: int):
    """
    Maps a positive integer to a real number in [0, 1) by calculating the n-th term of the low discrepancy sequence described here: http://extremelearning.com.au/unreasonable-effectiveness-of-quasirandom-sequences/

    :param i: A positive integer
    :return: The n-th term of the low discrepancy sequence described here: http://extremelearning.com.au/unreasonable-effectiveness-of-quasirandom-sequences/
    :raises: ValueError if i is not a positive integer
    """
    if type(i) is not int or i <= 0:
        raise ValueError(f"{i} is not a positive integer")

    return (0.5 + _INVERSE_GOLDEN_RATIO * i) % 1


random_from_event_id = random_from_positive_int
