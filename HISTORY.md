# Release History

# 0.6.2 - 2023-06-09

### Added

* datetime_to_publish_time function

# 0.6.1 - 2023-06-06

### Changed

* Handle None in publish_time_to_datetime

# 0.6.0 - 2023-05-31

### Added

* Order book related functions:
  * calculate_available_volume (@jorgegarcia7)
  * get_mid_price
* Prices files related functions:
  * get_inplay_publish_time_from_prices_file
  * get_last_pre_event_market_book_from_prices_file
  * get_total_volume_traded_from_prices_file
* Race stream related functions:
  * calculate_haversine_distance_between_runners
  * get_number_of_jumps_remaining
  * get_race_leaders
* Race card related functions:
  * get_is_jump_from_race_card
  
### Changed

* Made get_final_market_definition_from_prices_file more efficient
* Race card extraction functions now work on both dictionaries and paths to files

# 0.5.1 - 2023-02-25

### Added

* Functions for working with MarketDefinition objects:
  * create_market_definition_generator_from_prices_file
  * get_all_market_definitions_from_prices_file
  * get_winners_from_prices_file

### Changed

* Use isinstance() wherever type() was being used
* Improved create_combined_market_book_and_race_change_generator
  * Added type hint
  * Added docstring
  * Now generates pairs indicating which stream each object came from

# 0.5.0 - 2023-01-13

### Added

* Functions for working with RaceResult objects:
  * get_bsp_from_race_result
  * get_winners_from_race_result
* Functions for extracting BSP from different sources:
  * get_bsp_from_market_definition
  * get_bsp_from_prices_file
* Functions for working with marketTime:
  * get_market_time_as_datetime
  * get_seconds_to_market_time
* Functions for working with race files:
  * create_combined_market_book_and_race_change_generator
  * get_win_market_id_from_race_file
* Functions for extracting information from the order book:
  * calculate_order_book_imbalance
  * get_price_size_by_depth
  * get_second_best_price_size
  * get_second_best_price
* Functions for extracting market books and race changes at times of interest:
  * get_market_books_from_prices_file
  * get_minimum_book_percentage_market_books_from_prices_file
  * get_race_change_from_race_file
* Other Functions:
  * get_publish_time_from_object 
  * get_winners_from_market_definition

### Changed

* Improved patching of open
* Arguments to prices_file_to_data_frame have changed to permit adding any desired marketDefinition field to the data frame

# 0.4.0 - 2022-11-07

### Added

* get_event_id_from_string function
* get_pre_event_volume_traded_from_prices_file
* get_win_market_id_from_race_card function

### Changed

* pandas is now an optional dependency
* Added should_restrict_to_inplay argument to prices_file_to_data_frame
* Made get_runner_book_from_market_book more defensive
* Added LAST_PRICE_TRADED data frame format
* Optionally output runner statuses when converting market books to data frames
* Various README improvements:
    * More hype!
    * Badges
    * Examples

# 0.3.1 - 2022-05-12

### Added

* get_best_price_with_rollup function
* get_final_market_definition_from_prices_file function

# 0.3.0 - 2022-04-23

### Added

* Unit tests [#14](https://github.com/mberk/betfairutil/issues/14)
* Various price related functions:
    * get_spread
    * calculate_price_difference
    * get_inside_best_price
    * get_outside_best_price
    * get_best_price
    * increment_price
    * decrement_price
    * is_price_the_same_or_better
    * is_price_worse
* Various horse racing related functions:
    * get_race_id_from_string
    * get_win_market_id_from_race_card
    * read_race_file

### Changed

* Updated typing of all prices files related functions to accept Path objects

### Fixed

* Various fixes when using MarketBook objects instead of dictionaries

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
