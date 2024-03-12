# betfairutil

![Build Status](https://github.com/mberk/betfairutil/actions/workflows/test.yml/badge.svg)
[![Coverage Status](https://coveralls.io/repos/github/mberk/betfairutil/badge.svg?branch=master)](https://coveralls.io/github/mberk/betfairutil?branch=master)
[![PyPI version](https://badge.fury.io/py/betfairutil.svg)](https://pypi.python.org/pypi/betfairutil)
[![Downloads](https://pepy.tech/badge/betfairutil)](https://pepy.tech/project/betfairutil)

Utility functions for working with Betfair data. `betfairutil` code drives betting strategies that make millions in
betting profits a year

# Dependencies

* [betfairlightweight](https://github.com/liampauling/betfair)

Optionally, for working with Betfair prices files:

* smart_open

Optionally, for working with data frames:

* pandas

# Installation

Requires Python 3.9 or above.

If working with Betfair prices files:

```
pip install betfairutil[files]
```

If working with data frames:

```
pip install betfairutil[data_frames]
```

If working with both Betfair prices files and data frames:

```
pip install betfairutil[files,data_frames]
```

Otherwise:

```
pip install betfairutil
```

# Examples

## Create a Plot of Book Percentage Over Time

The first step in analysing Betfair market data is to get the market book at each update into memory. `betfairutil`
makes this trivial with the `read_prices_file` function. Once the market books are read in, `betfairutil` provides a
wide range of functions for extracting data from them. Here we show how to calculate the book percentage - also known as
the overround, book sum or vigorish - for each market book and plot that over time alongside human-readable Betfair
timestamps

```python
import betfairutil
import seaborn as sns

market_books = betfairutil.read_prices_file(path_to_prices_file)
book_percentages = [
  betfairutil.calculate_book_percentage(mb, betfairutil.Side.BACK)
  for mb in market_books
]
publish_times = [
  betfairutil.publish_time_to_datetime(mb["publishTime"])
  for mb in market_books
]
sns.lineplot(x=publish_times, y=book_percentages)
```

## Convert a Directory of Prices Files to CSV Files

A very common desire is to convert the Betfair historic prices files to CSV format for easier ingestion. This example
shows how easy that is using `betfairutil`. The format of the CSV file can be controlled via arguments to
`prices_file_to_csv_file` not demonstrated here - check the package source code for more details

```python
import os

import betfairutil

for path_to_prices_file in os.listdir(path_to_input_directory):
    market_id = betfairutil.get_market_id_from_string(path_to_prices_file)
    path_to_csv_file = os.path.join(path_to_output_directory, f"{market_id}.csv")
    betfairutil.prices_file_to_csv_file(path_to_prices_file, path_to_csv_file)
```

## Mark to Market

Once you've built up a position in a market, you can calculate your expected value according to the current market
implied probabilities. This example assumes that your position on each runner in stored in a `dict` called `positions`
mapping selection ID to your return if that selection wins

```python
import betfairutil

overround = betfairutil.calculate_book_percentage(current_market_book, betfairutil.Side.BACK)
implied_probabilities = {
  runner["selectionId"]: 1.0 / betfairutil.get_best_price(runner, betfairutil.Side.BACK) / overround
  for runner in betfairutil.iterate_active_runners(current_market_book)
}
expected_value = sum(
  implied_probability * positions[selection_id]
  for selection_id, implied_probability in implied_probabilities.items()
)
```

## A/B Testing or Cross Validation

A common requirement is to randomly assign markets to different groups. For example, when A/B testing new strategy
parameters or when doing cross-validation as part of backtesting. A good method for randomising markets will:

1. Be fast
2. Demonstrate good statistical properties - i.e. be as truly random as possible
3. Be reproducible
    1. This ensures that when repeating backtests with different sets of parameters, for example, the same set of
       markets is assigned to the same group. Commonly this is achieved by setting the random "seed"
    2. This ensures results are comparable across computers and versions. One major advantage of this is facilitating
       collaboration
4. Take account of the inherent structure in how Betfair assigns market IDs. For example, in horse racing the PLACE
   market's market ID is typical the WIN market's ID plus 1. Naive methods for randomly assigning markets to two groups
   such as basing it on whether the final digit of the ID is odd or even will end up always assigning a given race's
   WIN and PLACE markets to different groups

`betfairutil` includes functions for such random assignments that possess all of the above properties. The random number
generation is based on the low-discrepancy sequence described
[here](http://extremelearning.com.au/unreasonable-effectiveness-of-quasirandom-sequences/)

```python
import betfairutil

parameters = parameters_a if betfairutil.random_from_market_id(market_id) < 0.5 else parameters_b
```

```python
import betfairutil
import numpy as np
import pandas as pd

folds = pd.cut(
    [betfairutil.random_from_event_id(event_id) for event_id in event_ids],
    np.arange(0, 1.1, 0.1)
)
```

## Extract Market Books of Interest

`betfairutil` contains functions for efficiently extracting market books at times of interest rather than having to read
the entire sequence of market books into memory. This example also illustrates some functionality the package provides
for working with the Betfair race stream. First, we work out the exact time when the race enters the final furlong.
Then, we extract the market book at this moment in time

```python
import betfairutil

race_change = betfairutil.get_race_change_from_race_file(path_to_race_file, gate_name="1f")
publish_time = race_change["pt"]
market_book = betfairutil.get_market_books_from_prices_file(
    path_to_prices_file,
    publish_times=[publish_time]
)[publish_time]
```

# See Also

* There is some inevitable overlap between this package and [flumine's](https://github.com/liampauling/flumine) own
  [utils module](https://github.com/liampauling/flumine/blob/master/flumine/utils.py). However, that module
  understandably conflates utility functions for Betfair data structures, flumine, and general purposes. The betfairutil
  package:
    * Has a much tighter scope than flumine and is therefore a lighter weight solution for those who are not flumine
    users
    * It is hoped will ultimately provide a wider range of functions and therefore provide value to flumine users as 
    well
