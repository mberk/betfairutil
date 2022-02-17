# Release History

## 0.2.0 - 2022-02-17

### Added

* Use market catalogues to inject runner names into self recorded data [#13](https://github.com/mberk/betfairutil/issues/13)

### Fixed

* Handle event-level historic prices files [#11](https://github.com/mberk/betfairutil/issues/11)

## 0.1.2 - 2022-02-08

### Changed

* Pass kwargs to StreamListener [142f4f6](https://github.com/mberk/betfairutil/commit/142f4f65c62253fd44b3ce6a849d19fb0e43d804)

## 0.1.1 - 2022-02-01

### Added

* Add publish_time_to_datetime function [a53cf58](https://github.com/mberk/betfairutil/pull/8/commits/a53cf58fdb9474bb8b46c85ae057112ced4f59f8)

### Fixed

* Remove deprecated argument to StreamListener [68fe522](https://github.com/mberk/betfairutil/pull/8/commits/68fe5229efda865f844d86016678013bbed0c7ba)

## 0.1.0 - 2022-02-01

### Added

* Add `get_market_id_from_string` function [ad56cb7](https://github.com/mberk/betfairutil/commit/ad56cb721caecfcc8bf301c4c6882c067773e60d)
* Add functions for random number generation [bff04a5](https://github.com/mberk/betfairutil/commit/bff04a58a0fb8ce56182d59d0c6e7fc8f38855e6)

### Changed

* Relax bflw version requirement for files [704c70b](https://github.com/mberk/betfairutil/commit/704c70bd6ce5b56bb5a91aa354dd3c319464b343)

### Fixed

* Remove deprecated argument to StreamListener [b1af45c](https://github.com/mberk/betfairutil/commit/b1af45ca9deca3ce6c95401576f077b70421f1b7)

## 0.0.2 - 2021-08-11

### Added

* Add `calculate_total_matched` function [c2233af](https://github.com/mberk/betfairutil/commit/c2233afda82785903816aae7f1e0a77f8ca3e23a)

### Fixed

* Fix `calculate_book_percentage` [#3](https://github.com/mberk/betfairutil/issues/3). For cases where a runner has no prices on either side
