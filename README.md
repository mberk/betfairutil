# betfairutil

Utility functions for working with Betfair data

# Dependencies

* [betfairlightweight](https://github.com/liampauling/betfair)
* pandas

Optionally, for working with Betfair prices files:

* smart_open

# Installation

Requires Python 3.5 or above.

If working with Betfair prices files:

```
pip install betfairutil[files]
```

Otherwise:

```
pip install betfairutil
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
