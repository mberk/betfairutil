from typing import Any, Dict, Optional, Union

from betfairlightweight.resources.bettingresources import MarketBook
from betfairlightweight.resources.bettingresources import RunnerBook


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
