import enum
from typing import Any, Dict, Generator, Optional, Sequence, Union

import pandas as pd
from betfairlightweight import APIClient
from betfairlightweight import StreamListener
from betfairlightweight.resources.bettingresources import MarketBook
from betfairlightweight.resources.bettingresources import RunnerBook


class Side(enum.Enum):
    BACK = 'Back'
    LAY = 'Lay'

    @property
    def other_side(self):
        if self is Side.BACK:
            return Side.LAY
        else:
            return Side.BACK

    @property
    def ex_key(self):
        return f'availableTo{self.value}'


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


def get_runner_book_from_market_book(
    market_book: Union[Dict[str, Any], MarketBook],
    selection_id: Optional[int] = None,
    runner_name: Optional[str] = None,
    handicap: float = 0.0,
) -> Optional[Union[Dict[str, Any], RunnerBook]]:
    """
    Extract a runner book from the given market book. The runner can be identified either by ID or name

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :param selection_id: Optionally identify the runner book to extract by the runner's ID
    :param runner_name: Alternatively identify the runner book to extract by the runner's name
    :param handicap: The handicap of the desired runner book
    :returns: The corresponding runner book if it can be found in the market book, otherwise None. The type of the return value will reflect the type of market_book; if market_book is a dictionary then the return value is a dictionary. If market_book is a MarketBook object then the return value will be a RunnerBook object
    :raises: ValueError if both selection_id and runner_name are given. Only one is required to uniquely identify the runner book
    """
    if selection_id is not None and runner_name is not None:
        raise ValueError("Both selection_id and runner_name were given")

    if type(market_book) is dict:
        return_type = dict
    else:
        market_book = market_book._data
        return_type = RunnerBook

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


def get_best_price_size(runner: Union[Dict[str, Any], RunnerBook], side: Side) -> Optional[Dict[str, Union[int, float]]]:
    return next(iter(runner.get('ex', {}).get(side.ex_key, [])))


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


def market_book_to_data_frame(
    market_book: Union[Dict[str, Any], MarketBook]
) -> pd.DataFrame:
    """
    Construct a data frame representation of a market book. Each row is one point on the price ladder for a particular
    runner

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
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
    """
    if type(market_book) is MarketBook:
        market_book = market_book._data

    return pd.DataFrame(
        {
            "market_id": market_book["marketId"],
            "inplay": market_book["inplay"],
            "selection_id": runner["selectionId"],
            "handicap": runner["handicap"],
            "side": side,
            "depth": depth,
            "price": price_size["price"],
            "size": price_size["size"],
            **(
                {"publish_time": market_book["publishTime"]}
                if "publishTime" in market_book
                else {}
            ),
        }
        for runner in market_book["runners"]
        for side in ["Back", "Lay"]
        for depth, price_size in enumerate(
            runner.get("ex", {}).get(f"availableTo{side}", [])
        )
    )


def prices_file_to_data_frame(path_to_prices_file: str) -> pd.DataFrame:
    """
    Read a Betfair prices file (either from the official historic data or data recorded from the streaming API in the same format) directly into a data frame

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
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
    """
    import smart_open
    from unittest.mock import patch

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        directory=path_to_prices_file,
        listener=StreamListener(max_latency=None, lightweight=True),
    )

    with patch("builtins.open", smart_open.open):
        g = stream.get_generator()
        return pd.concat(market_book_to_data_frame(mbs[0]) for mbs in g())
