import datetime
import enum
import heapq
import itertools
import pickle
import re
from bisect import bisect_left
from bisect import bisect_right
from collections import deque
from collections.abc import Mapping
from collections.abc import Sequence
from math import asin
from math import cos
from math import radians
from math import sin
from math import sqrt
from pathlib import Path
from typing import (
    Any,
    Generator,
    Iterator,
    Optional,
    TYPE_CHECKING,
    Union,
)

if TYPE_CHECKING:
    import pandas as pd

from betfairlightweight import APIClient
from betfairlightweight import StreamListener
from betfairlightweight.resources.bettingresources import MarketBook
from betfairlightweight.resources.bettingresources import MarketCatalogue
from betfairlightweight.resources.bettingresources import PriceSize
from betfairlightweight.resources.bettingresources import RunnerBook
from betfairlightweight.resources.streamingresources import MarketDefinition

from .examples import *

BETFAIR_PRICES = [
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
BETFAIR_PRICE_TO_NEXT_PRICE_UP_MAP = {
    1.01: 1.02,
    1.02: 1.03,
    1.03: 1.04,
    1.04: 1.05,
    1.05: 1.06,
    1.06: 1.07,
    1.07: 1.08,
    1.08: 1.09,
    1.09: 1.1,
    1.1: 1.11,
    1.11: 1.12,
    1.12: 1.13,
    1.13: 1.14,
    1.14: 1.15,
    1.15: 1.16,
    1.16: 1.17,
    1.17: 1.18,
    1.18: 1.19,
    1.19: 1.2,
    1.2: 1.21,
    1.21: 1.22,
    1.22: 1.23,
    1.23: 1.24,
    1.24: 1.25,
    1.25: 1.26,
    1.26: 1.27,
    1.27: 1.28,
    1.28: 1.29,
    1.29: 1.3,
    1.3: 1.31,
    1.31: 1.32,
    1.32: 1.33,
    1.33: 1.34,
    1.34: 1.35,
    1.35: 1.36,
    1.36: 1.37,
    1.37: 1.38,
    1.38: 1.39,
    1.39: 1.4,
    1.4: 1.41,
    1.41: 1.42,
    1.42: 1.43,
    1.43: 1.44,
    1.44: 1.45,
    1.45: 1.46,
    1.46: 1.47,
    1.47: 1.48,
    1.48: 1.49,
    1.49: 1.5,
    1.5: 1.51,
    1.51: 1.52,
    1.52: 1.53,
    1.53: 1.54,
    1.54: 1.55,
    1.55: 1.56,
    1.56: 1.57,
    1.57: 1.58,
    1.58: 1.59,
    1.59: 1.6,
    1.6: 1.61,
    1.61: 1.62,
    1.62: 1.63,
    1.63: 1.64,
    1.64: 1.65,
    1.65: 1.66,
    1.66: 1.67,
    1.67: 1.68,
    1.68: 1.69,
    1.69: 1.7,
    1.7: 1.71,
    1.71: 1.72,
    1.72: 1.73,
    1.73: 1.74,
    1.74: 1.75,
    1.75: 1.76,
    1.76: 1.77,
    1.77: 1.78,
    1.78: 1.79,
    1.79: 1.8,
    1.8: 1.81,
    1.81: 1.82,
    1.82: 1.83,
    1.83: 1.84,
    1.84: 1.85,
    1.85: 1.86,
    1.86: 1.87,
    1.87: 1.88,
    1.88: 1.89,
    1.89: 1.9,
    1.9: 1.91,
    1.91: 1.92,
    1.92: 1.93,
    1.93: 1.94,
    1.94: 1.95,
    1.95: 1.96,
    1.96: 1.97,
    1.97: 1.98,
    1.98: 1.99,
    1.99: 2,
    2: 2.02,
    2.02: 2.04,
    2.04: 2.06,
    2.06: 2.08,
    2.08: 2.1,
    2.1: 2.12,
    2.12: 2.14,
    2.14: 2.16,
    2.16: 2.18,
    2.18: 2.2,
    2.2: 2.22,
    2.22: 2.24,
    2.24: 2.26,
    2.26: 2.28,
    2.28: 2.3,
    2.3: 2.32,
    2.32: 2.34,
    2.34: 2.36,
    2.36: 2.38,
    2.38: 2.4,
    2.4: 2.42,
    2.42: 2.44,
    2.44: 2.46,
    2.46: 2.48,
    2.48: 2.5,
    2.5: 2.52,
    2.52: 2.54,
    2.54: 2.56,
    2.56: 2.58,
    2.58: 2.6,
    2.6: 2.62,
    2.62: 2.64,
    2.64: 2.66,
    2.66: 2.68,
    2.68: 2.7,
    2.7: 2.72,
    2.72: 2.74,
    2.74: 2.76,
    2.76: 2.78,
    2.78: 2.8,
    2.8: 2.82,
    2.82: 2.84,
    2.84: 2.86,
    2.86: 2.88,
    2.88: 2.9,
    2.9: 2.92,
    2.92: 2.94,
    2.94: 2.96,
    2.96: 2.98,
    2.98: 3,
    3: 3.05,
    3.05: 3.1,
    3.1: 3.15,
    3.15: 3.2,
    3.2: 3.25,
    3.25: 3.3,
    3.3: 3.35,
    3.35: 3.4,
    3.4: 3.45,
    3.45: 3.5,
    3.5: 3.55,
    3.55: 3.6,
    3.6: 3.65,
    3.65: 3.7,
    3.7: 3.75,
    3.75: 3.8,
    3.8: 3.85,
    3.85: 3.9,
    3.9: 3.95,
    3.95: 4,
    4: 4.1,
    4.1: 4.2,
    4.2: 4.3,
    4.3: 4.4,
    4.4: 4.5,
    4.5: 4.6,
    4.6: 4.7,
    4.7: 4.8,
    4.8: 4.9,
    4.9: 5,
    5: 5.1,
    5.1: 5.2,
    5.2: 5.3,
    5.3: 5.4,
    5.4: 5.5,
    5.5: 5.6,
    5.6: 5.7,
    5.7: 5.8,
    5.8: 5.9,
    5.9: 6,
    6: 6.2,
    6.2: 6.4,
    6.4: 6.6,
    6.6: 6.8,
    6.8: 7,
    7: 7.2,
    7.2: 7.4,
    7.4: 7.6,
    7.6: 7.8,
    7.8: 8,
    8: 8.2,
    8.2: 8.4,
    8.4: 8.6,
    8.6: 8.8,
    8.8: 9,
    9: 9.2,
    9.2: 9.4,
    9.4: 9.6,
    9.6: 9.8,
    9.8: 10,
    10: 10.5,
    10.5: 11,
    11: 11.5,
    11.5: 12,
    12: 12.5,
    12.5: 13,
    13: 13.5,
    13.5: 14,
    14: 14.5,
    14.5: 15,
    15: 15.5,
    15.5: 16,
    16: 16.5,
    16.5: 17,
    17: 17.5,
    17.5: 18,
    18: 18.5,
    18.5: 19,
    19: 19.5,
    19.5: 20,
    20: 21,
    21: 22,
    22: 23,
    23: 24,
    24: 25,
    25: 26,
    26: 27,
    27: 28,
    28: 29,
    29: 30,
    30: 32,
    32: 34,
    34: 36,
    36: 38,
    38: 40,
    40: 42,
    42: 44,
    44: 46,
    46: 48,
    48: 50,
    50: 55,
    55: 60,
    60: 65,
    65: 70,
    70: 75,
    75: 80,
    80: 85,
    85: 90,
    90: 95,
    95: 100,
    100: 110,
    110: 120,
    120: 130,
    130: 140,
    140: 150,
    150: 160,
    160: 170,
    170: 180,
    180: 190,
    190: 200,
    200: 210,
    210: 220,
    220: 230,
    230: 240,
    240: 250,
    250: 260,
    260: 270,
    270: 280,
    280: 290,
    290: 300,
    300: 310,
    310: 320,
    320: 330,
    330: 340,
    340: 350,
    350: 360,
    360: 370,
    370: 380,
    380: 390,
    390: 400,
    400: 410,
    410: 420,
    420: 430,
    430: 440,
    440: 450,
    450: 460,
    460: 470,
    470: 480,
    480: 490,
    490: 500,
    500: 510,
    510: 520,
    520: 530,
    530: 540,
    540: 550,
    550: 560,
    560: 570,
    570: 580,
    580: 590,
    590: 600,
    600: 610,
    610: 620,
    620: 630,
    630: 640,
    640: 650,
    650: 660,
    660: 670,
    670: 680,
    680: 690,
    690: 700,
    700: 710,
    710: 720,
    720: 730,
    730: 740,
    740: 750,
    750: 760,
    760: 770,
    770: 780,
    780: 790,
    790: 800,
    800: 810,
    810: 820,
    820: 830,
    830: 840,
    840: 850,
    850: 860,
    860: 870,
    870: 880,
    880: 890,
    890: 900,
    900: 910,
    910: 920,
    920: 930,
    930: 940,
    940: 950,
    950: 960,
    960: 970,
    970: 980,
    980: 990,
    990: 1000,
}
BETFAIR_PRICE_TO_NEXT_PRICE_DOWN_MAP = {
    1.02: 1.01,
    1.03: 1.02,
    1.04: 1.03,
    1.05: 1.04,
    1.06: 1.05,
    1.07: 1.06,
    1.08: 1.07,
    1.09: 1.08,
    1.1: 1.09,
    1.11: 1.1,
    1.12: 1.11,
    1.13: 1.12,
    1.14: 1.13,
    1.15: 1.14,
    1.16: 1.15,
    1.17: 1.16,
    1.18: 1.17,
    1.19: 1.18,
    1.2: 1.19,
    1.21: 1.2,
    1.22: 1.21,
    1.23: 1.22,
    1.24: 1.23,
    1.25: 1.24,
    1.26: 1.25,
    1.27: 1.26,
    1.28: 1.27,
    1.29: 1.28,
    1.3: 1.29,
    1.31: 1.3,
    1.32: 1.31,
    1.33: 1.32,
    1.34: 1.33,
    1.35: 1.34,
    1.36: 1.35,
    1.37: 1.36,
    1.38: 1.37,
    1.39: 1.38,
    1.4: 1.39,
    1.41: 1.4,
    1.42: 1.41,
    1.43: 1.42,
    1.44: 1.43,
    1.45: 1.44,
    1.46: 1.45,
    1.47: 1.46,
    1.48: 1.47,
    1.49: 1.48,
    1.5: 1.49,
    1.51: 1.5,
    1.52: 1.51,
    1.53: 1.52,
    1.54: 1.53,
    1.55: 1.54,
    1.56: 1.55,
    1.57: 1.56,
    1.58: 1.57,
    1.59: 1.58,
    1.6: 1.59,
    1.61: 1.6,
    1.62: 1.61,
    1.63: 1.62,
    1.64: 1.63,
    1.65: 1.64,
    1.66: 1.65,
    1.67: 1.66,
    1.68: 1.67,
    1.69: 1.68,
    1.7: 1.69,
    1.71: 1.7,
    1.72: 1.71,
    1.73: 1.72,
    1.74: 1.73,
    1.75: 1.74,
    1.76: 1.75,
    1.77: 1.76,
    1.78: 1.77,
    1.79: 1.78,
    1.8: 1.79,
    1.81: 1.8,
    1.82: 1.81,
    1.83: 1.82,
    1.84: 1.83,
    1.85: 1.84,
    1.86: 1.85,
    1.87: 1.86,
    1.88: 1.87,
    1.89: 1.88,
    1.9: 1.89,
    1.91: 1.9,
    1.92: 1.91,
    1.93: 1.92,
    1.94: 1.93,
    1.95: 1.94,
    1.96: 1.95,
    1.97: 1.96,
    1.98: 1.97,
    1.99: 1.98,
    2: 1.99,
    2.02: 2,
    2.04: 2.02,
    2.06: 2.04,
    2.08: 2.06,
    2.1: 2.08,
    2.12: 2.1,
    2.14: 2.12,
    2.16: 2.14,
    2.18: 2.16,
    2.2: 2.18,
    2.22: 2.2,
    2.24: 2.22,
    2.26: 2.24,
    2.28: 2.26,
    2.3: 2.28,
    2.32: 2.3,
    2.34: 2.32,
    2.36: 2.34,
    2.38: 2.36,
    2.4: 2.38,
    2.42: 2.4,
    2.44: 2.42,
    2.46: 2.44,
    2.48: 2.46,
    2.5: 2.48,
    2.52: 2.5,
    2.54: 2.52,
    2.56: 2.54,
    2.58: 2.56,
    2.6: 2.58,
    2.62: 2.6,
    2.64: 2.62,
    2.66: 2.64,
    2.68: 2.66,
    2.7: 2.68,
    2.72: 2.7,
    2.74: 2.72,
    2.76: 2.74,
    2.78: 2.76,
    2.8: 2.78,
    2.82: 2.8,
    2.84: 2.82,
    2.86: 2.84,
    2.88: 2.86,
    2.9: 2.88,
    2.92: 2.9,
    2.94: 2.92,
    2.96: 2.94,
    2.98: 2.96,
    3: 2.98,
    3.05: 3,
    3.1: 3.05,
    3.15: 3.1,
    3.2: 3.15,
    3.25: 3.2,
    3.3: 3.25,
    3.35: 3.3,
    3.4: 3.35,
    3.45: 3.4,
    3.5: 3.45,
    3.55: 3.5,
    3.6: 3.55,
    3.65: 3.6,
    3.7: 3.65,
    3.75: 3.7,
    3.8: 3.75,
    3.85: 3.8,
    3.9: 3.85,
    3.95: 3.9,
    4: 3.95,
    4.1: 4,
    4.2: 4.1,
    4.3: 4.2,
    4.4: 4.3,
    4.5: 4.4,
    4.6: 4.5,
    4.7: 4.6,
    4.8: 4.7,
    4.9: 4.8,
    5: 4.9,
    5.1: 5,
    5.2: 5.1,
    5.3: 5.2,
    5.4: 5.3,
    5.5: 5.4,
    5.6: 5.5,
    5.7: 5.6,
    5.8: 5.7,
    5.9: 5.8,
    6: 5.9,
    6.2: 6,
    6.4: 6.2,
    6.6: 6.4,
    6.8: 6.6,
    7: 6.8,
    7.2: 7,
    7.4: 7.2,
    7.6: 7.4,
    7.8: 7.6,
    8: 7.8,
    8.2: 8,
    8.4: 8.2,
    8.6: 8.4,
    8.8: 8.6,
    9: 8.8,
    9.2: 9,
    9.4: 9.2,
    9.6: 9.4,
    9.8: 9.6,
    10: 9.8,
    10.5: 10,
    11: 10.5,
    11.5: 11,
    12: 11.5,
    12.5: 12,
    13: 12.5,
    13.5: 13,
    14: 13.5,
    14.5: 14,
    15: 14.5,
    15.5: 15,
    16: 15.5,
    16.5: 16,
    17: 16.5,
    17.5: 17,
    18: 17.5,
    18.5: 18,
    19: 18.5,
    19.5: 19,
    20: 19.5,
    21: 20,
    22: 21,
    23: 22,
    24: 23,
    25: 24,
    26: 25,
    27: 26,
    28: 27,
    29: 28,
    30: 29,
    32: 30,
    34: 32,
    36: 34,
    38: 36,
    40: 38,
    42: 40,
    44: 42,
    46: 44,
    48: 46,
    50: 48,
    55: 50,
    60: 55,
    65: 60,
    70: 65,
    75: 70,
    80: 75,
    85: 80,
    90: 85,
    95: 90,
    100: 95,
    110: 100,
    120: 110,
    130: 120,
    140: 130,
    150: 140,
    160: 150,
    170: 160,
    180: 170,
    190: 180,
    200: 190,
    210: 200,
    220: 210,
    230: 220,
    240: 230,
    250: 240,
    260: 250,
    270: 260,
    280: 270,
    290: 280,
    300: 290,
    310: 300,
    320: 310,
    330: 320,
    340: 330,
    350: 340,
    360: 350,
    370: 360,
    380: 370,
    390: 380,
    400: 390,
    410: 400,
    420: 410,
    430: 420,
    440: 430,
    450: 440,
    460: 450,
    470: 460,
    480: 470,
    490: 480,
    500: 490,
    510: 500,
    520: 510,
    530: 520,
    540: 530,
    550: 540,
    560: 550,
    570: 560,
    580: 570,
    590: 580,
    600: 590,
    610: 600,
    620: 610,
    630: 620,
    640: 630,
    650: 640,
    660: 650,
    670: 660,
    680: 670,
    690: 680,
    700: 690,
    710: 700,
    720: 710,
    730: 720,
    740: 730,
    750: 740,
    760: 750,
    770: 760,
    780: 770,
    790: 780,
    800: 790,
    810: 800,
    820: 810,
    830: 820,
    840: 830,
    850: 840,
    860: 850,
    870: 860,
    880: 870,
    890: 880,
    900: 890,
    910: 900,
    920: 910,
    930: 920,
    940: 930,
    950: 940,
    960: 950,
    970: 960,
    980: 970,
    990: 980,
    1000: 990,
}
BETFAIR_PRICE_TO_PRICE_INDEX_MAP = {
    1.01: 0,
    1.02: 1,
    1.03: 2,
    1.04: 3,
    1.05: 4,
    1.06: 5,
    1.07: 6,
    1.08: 7,
    1.09: 8,
    1.1: 9,
    1.11: 10,
    1.12: 11,
    1.13: 12,
    1.14: 13,
    1.15: 14,
    1.16: 15,
    1.17: 16,
    1.18: 17,
    1.19: 18,
    1.2: 19,
    1.21: 20,
    1.22: 21,
    1.23: 22,
    1.24: 23,
    1.25: 24,
    1.26: 25,
    1.27: 26,
    1.28: 27,
    1.29: 28,
    1.3: 29,
    1.31: 30,
    1.32: 31,
    1.33: 32,
    1.34: 33,
    1.35: 34,
    1.36: 35,
    1.37: 36,
    1.38: 37,
    1.39: 38,
    1.4: 39,
    1.41: 40,
    1.42: 41,
    1.43: 42,
    1.44: 43,
    1.45: 44,
    1.46: 45,
    1.47: 46,
    1.48: 47,
    1.49: 48,
    1.5: 49,
    1.51: 50,
    1.52: 51,
    1.53: 52,
    1.54: 53,
    1.55: 54,
    1.56: 55,
    1.57: 56,
    1.58: 57,
    1.59: 58,
    1.6: 59,
    1.61: 60,
    1.62: 61,
    1.63: 62,
    1.64: 63,
    1.65: 64,
    1.66: 65,
    1.67: 66,
    1.68: 67,
    1.69: 68,
    1.7: 69,
    1.71: 70,
    1.72: 71,
    1.73: 72,
    1.74: 73,
    1.75: 74,
    1.76: 75,
    1.77: 76,
    1.78: 77,
    1.79: 78,
    1.8: 79,
    1.81: 80,
    1.82: 81,
    1.83: 82,
    1.84: 83,
    1.85: 84,
    1.86: 85,
    1.87: 86,
    1.88: 87,
    1.89: 88,
    1.9: 89,
    1.91: 90,
    1.92: 91,
    1.93: 92,
    1.94: 93,
    1.95: 94,
    1.96: 95,
    1.97: 96,
    1.98: 97,
    1.99: 98,
    2: 99,
    2.02: 100,
    2.04: 101,
    2.06: 102,
    2.08: 103,
    2.1: 104,
    2.12: 105,
    2.14: 106,
    2.16: 107,
    2.18: 108,
    2.2: 109,
    2.22: 110,
    2.24: 111,
    2.26: 112,
    2.28: 113,
    2.3: 114,
    2.32: 115,
    2.34: 116,
    2.36: 117,
    2.38: 118,
    2.4: 119,
    2.42: 120,
    2.44: 121,
    2.46: 122,
    2.48: 123,
    2.5: 124,
    2.52: 125,
    2.54: 126,
    2.56: 127,
    2.58: 128,
    2.6: 129,
    2.62: 130,
    2.64: 131,
    2.66: 132,
    2.68: 133,
    2.7: 134,
    2.72: 135,
    2.74: 136,
    2.76: 137,
    2.78: 138,
    2.8: 139,
    2.82: 140,
    2.84: 141,
    2.86: 142,
    2.88: 143,
    2.9: 144,
    2.92: 145,
    2.94: 146,
    2.96: 147,
    2.98: 148,
    3: 149,
    3.05: 150,
    3.1: 151,
    3.15: 152,
    3.2: 153,
    3.25: 154,
    3.3: 155,
    3.35: 156,
    3.4: 157,
    3.45: 158,
    3.5: 159,
    3.55: 160,
    3.6: 161,
    3.65: 162,
    3.7: 163,
    3.75: 164,
    3.8: 165,
    3.85: 166,
    3.9: 167,
    3.95: 168,
    4: 169,
    4.1: 170,
    4.2: 171,
    4.3: 172,
    4.4: 173,
    4.5: 174,
    4.6: 175,
    4.7: 176,
    4.8: 177,
    4.9: 178,
    5: 179,
    5.1: 180,
    5.2: 181,
    5.3: 182,
    5.4: 183,
    5.5: 184,
    5.6: 185,
    5.7: 186,
    5.8: 187,
    5.9: 188,
    6: 189,
    6.2: 190,
    6.4: 191,
    6.6: 192,
    6.8: 193,
    7: 194,
    7.2: 195,
    7.4: 196,
    7.6: 197,
    7.8: 198,
    8: 199,
    8.2: 200,
    8.4: 201,
    8.6: 202,
    8.8: 203,
    9: 204,
    9.2: 205,
    9.4: 206,
    9.6: 207,
    9.8: 208,
    10: 209,
    10.5: 210,
    11: 211,
    11.5: 212,
    12: 213,
    12.5: 214,
    13: 215,
    13.5: 216,
    14: 217,
    14.5: 218,
    15: 219,
    15.5: 220,
    16: 221,
    16.5: 222,
    17: 223,
    17.5: 224,
    18: 225,
    18.5: 226,
    19: 227,
    19.5: 228,
    20: 229,
    21: 230,
    22: 231,
    23: 232,
    24: 233,
    25: 234,
    26: 235,
    27: 236,
    28: 237,
    29: 238,
    30: 239,
    32: 240,
    34: 241,
    36: 242,
    38: 243,
    40: 244,
    42: 245,
    44: 246,
    46: 247,
    48: 248,
    50: 249,
    55: 250,
    60: 251,
    65: 252,
    70: 253,
    75: 254,
    80: 255,
    85: 256,
    90: 257,
    95: 258,
    100: 259,
    110: 260,
    120: 261,
    130: 262,
    140: 263,
    150: 264,
    160: 265,
    170: 266,
    180: 267,
    190: 268,
    200: 269,
    210: 270,
    220: 271,
    230: 272,
    240: 273,
    250: 274,
    260: 275,
    270: 276,
    280: 277,
    290: 278,
    300: 279,
    310: 280,
    320: 281,
    330: 282,
    340: 283,
    350: 284,
    360: 285,
    370: 286,
    380: 287,
    390: 288,
    400: 289,
    410: 290,
    420: 291,
    430: 292,
    440: 293,
    450: 294,
    460: 295,
    470: 296,
    480: 297,
    490: 298,
    500: 299,
    510: 300,
    520: 301,
    530: 302,
    540: 303,
    550: 304,
    560: 305,
    570: 306,
    580: 307,
    590: 308,
    600: 309,
    610: 310,
    620: 311,
    630: 312,
    640: 313,
    650: 314,
    660: 315,
    670: 316,
    680: 317,
    690: 318,
    700: 319,
    710: 320,
    720: 321,
    730: 322,
    740: 323,
    750: 324,
    760: 325,
    770: 326,
    780: 327,
    790: 328,
    800: 329,
    810: 330,
    820: 331,
    830: 332,
    840: 333,
    850: 334,
    860: 335,
    870: 336,
    880: 337,
    890: 338,
    900: 339,
    910: 340,
    920: 341,
    930: 342,
    940: 343,
    950: 344,
    960: 345,
    970: 346,
    980: 347,
    990: 348,
    1000: 349,
}
EX_KEYS = ["availableToBack", "availableToLay", "tradedVolume"]
MARKET_ID_PATTERN = re.compile(r"1\.\d{9}")
EVENT_ID_PATTERN = re.compile(r"\d{8}")
NUMBER_OF_METRES_IN_A_YARD = 0.9144
RACE_ID_PATTERN = re.compile(r"\d{8}\.\d{4}")
_INVERSE_GOLDEN_RATIO = 2.0 / (1 + sqrt(5.0))
_ORIGINAL_OPEN = open
_AVERAGE_EARTH_RADIUS_IN_METERS = 6371008.8


class _OpenMocker:
    def __init__(self, path_to_file: str):
        self.path_to_file = path_to_file

    def open(self, file, *args, **kwargs):
        if file == self.path_to_file:
            import smart_open

            return smart_open.open(file, *args, **kwargs)
        else:
            return _ORIGINAL_OPEN(file, *args, **kwargs)


def _load_json_object(o: Union[dict[str, Any], str, Path]) -> dict[str, Any]:
    """
    Takes either a JSON object and simply returns it or a path to a JSON file containing a JSON
    object in which case read that file and return the JSON object it contains

    :param o: Either a JSON object or a path to a JSON file containing a JSON object
    :return: If o is a JSON object then that object; otherwise the JSON object that the file
        o refers to contains
    """
    if isinstance(o, (str, Path)):
        import orjson
        import smart_open

        with smart_open.open(o, "r") as f:
            o = orjson.loads(f.read())

    return o


class DataFrameFormatEnum(enum.Enum):
    FULL_LADDER = "FULL_LADDER"
    LAST_PRICE_TRADED = "LAST_PRICE_TRADED"


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

    @property
    def ex_attribute(self):
        return f"available_to_{self.value.lower()}"

    @property
    def next_better_price_map(self):
        if self is Side.BACK:
            return BETFAIR_PRICE_TO_NEXT_PRICE_UP_MAP
        else:
            return BETFAIR_PRICE_TO_NEXT_PRICE_DOWN_MAP

    @property
    def next_worse_price_map(self):
        if self is Side.BACK:
            return BETFAIR_PRICE_TO_NEXT_PRICE_DOWN_MAP
        else:
            return BETFAIR_PRICE_TO_NEXT_PRICE_UP_MAP


class MarketBookDiff:
    def __init__(
        self,
        d: dict[
            tuple[int, Union[int, float]],
            dict[str, dict[Union[int, float], Union[int, float]]],
        ],
    ):
        self.d = d

    def get_size_changes(
        self, selection_id: int, ex_key: str, handicap: Union[int, float] = 0.0
    ) -> Optional[dict[Union[int, float], Union[int, float]]]:
        return self.d[(selection_id, handicap)].get(ex_key)


def calculate_book_percentage(
    market_book: Union[dict[str, Any], MarketBook], side: Side
) -> float:
    implied_probabilities = []
    for runner in iterate_active_runners(market_book):
        best_price_size = get_best_price_size(runner, side)
        if best_price_size is not None:
            if isinstance(best_price_size, PriceSize):
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


def calculate_available_volume(
    market_book: Union[dict[str, Any], MarketBook],
    side: Side,
    max_book_percentage: float,
) -> float:
    """
    Calculate the available volume up to a maximum book_percentage value.

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :param side: Indicate whether to get the available volume on the back or lay side.
    :param max_book_percentage: Maximum book_percentage value to use for calculating the volume.
    :return: Available total volume.
    """
    available_volume = 0
    for depth in itertools.count():
        book_percentage = 0
        size = 0
        for runner in market_book["runners"]:
            runner_price_size = get_price_size_by_depth(
                runner=runner, side=side, depth=depth
            )
            if runner_price_size:
                book_percentage += 1.0 / runner_price_size["price"]
                size += runner_price_size["size"]
            else:
                return available_volume

        if book_percentage <= max_book_percentage:
            available_volume += size


def calculate_market_book_diff(
    current_market_book: Union[dict[str, Any], MarketBook],
    previous_market_book: Union[dict[str, Any], MarketBook],
) -> MarketBookDiff:
    """
    Calculate the size differences between amounts available to back, available to lay, and traded between two market books

    :param current_market_book: The current market book to use in the comparison
    :param previous_market_book: The previous market book to use in the comparison
    :return: The complete set of size differences stored in a MarketBookDiff
    """
    if isinstance(current_market_book, MarketBook):
        current_market_book = current_market_book._data
    if isinstance(previous_market_book, MarketBook):
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


def calculate_order_book_imbalance(
    runner_book: Union[dict[str, Any], RunnerBook]
) -> Optional[float]:
    best_back_price_size = get_best_price_size(runner_book, Side.BACK)
    if best_back_price_size is not None:
        best_lay_price_size = get_best_price_size(runner_book, Side.LAY)
        if best_lay_price_size is not None:
            if isinstance(best_back_price_size, PriceSize):
                back_size = best_back_price_size.size
                lay_size = best_lay_price_size.size
            else:
                back_size = best_back_price_size["size"]
                lay_size = best_lay_price_size["size"]

            numerator = back_size - lay_size
            denominator = back_size + lay_size
            return numerator / denominator


def calculate_price_difference(a: Union[int, float], b: Union[int, float]) -> int:
    """
    Calculate the price difference between two prices as the number of steps on the Betfair price ladder. For example, the difference between 1.03 and 1.01 is 2. Conversely, the difference between 1.01 and 1.03 is -2

    :param a: A valid Betfair price
    :param b: A valid Betfair price
    :return: The difference between a and b as the number of steps on the Betfair price ladder
    :raises: As it is assumed a and b are already valid Betfair prices, an IndexError will be thrown when trying to look up an invalid price in the index map
    """
    a_index = BETFAIR_PRICE_TO_PRICE_INDEX_MAP[a]
    b_index = BETFAIR_PRICE_TO_PRICE_INDEX_MAP[b]
    return a_index - b_index


def calculate_total_matched(
    market_book: Union[dict[str, Any], MarketBook]
) -> Union[int, float]:
    """
    Calculate the total matched on this market from the amounts matched on each runner at each price point. Useful for historic data where this field is not populated

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :return: The total matched on this market
    """
    if isinstance(market_book, MarketBook):
        market_book = market_book._data

    return sum(
        ps["size"]
        for r in market_book.get("runners", [])
        for ps in r.get("ex", {}).get("tradedVolume", [])
    )


def convert_yards_to_metres(yards: Optional[Union[int, float]]) -> Optional[float]:
    if yards is None:
        return None

    return yards * NUMBER_OF_METRES_IN_A_YARD


def does_market_book_contain_runner_names(
    market_book: Union[dict[str, Any], MarketBook]
) -> bool:
    if isinstance(market_book, dict):
        market_definition = market_book["marketDefinition"]
    else:
        market_definition = market_book.market_definition
        if market_definition is None:
            return False

    return does_market_definition_contain_runner_names(market_definition)


def does_market_definition_contain_runner_names(
    market_definition: Union[dict[str, Any], MarketDefinition]
) -> bool:
    if isinstance(market_definition, dict):
        runners = market_definition.get("runners", [])
    else:
        runners = market_definition.runners

    if len(runners) == 0:
        return False

    runner = runners[0]

    if isinstance(runner, dict):
        name = runner.get("name")
    else:
        name = runner.name

    return name is not None


def filter_runners(
    market_book: Union[dict[str, Any], MarketBook],
    status: str,
    excluded_selection_ids: Sequence[int],
) -> Generator[Union[dict[str, Any], RunnerBook], None, None]:
    if isinstance(market_book, dict):
        runners = market_book["runners"]
    else:
        runners = market_book.runners

    for runner in runners:
        if isinstance(runner, dict):
            runner_status = runner["status"]
            selection_id = runner["selectionId"]
        else:
            runner_status = runner.status
            selection_id = runner.selection_id
        if runner_status != status:
            continue
        if selection_id in excluded_selection_ids:
            continue
        yield runner


def get_runner_book_from_market_book(
    market_book: Optional[Union[Mapping[str, Any], MarketBook]],
    selection_id: Optional[int] = None,
    runner_name: Optional[str] = None,
    handicap: float = 0.0,
    return_type: Optional[type] = None,
) -> Optional[Union[dict[str, Any], RunnerBook]]:
    """
    Extract a runner book from the given market book. The runner can be identified either by ID or name

    :param market_book: A market book either as an object whose class provides the mapping interface (e.g. a dict) or as a betfairlightweight MarketBook object. Alternatively can be None - if so, None will be returned
    :param selection_id: Optionally identify the runner book to extract by the runner's ID
    :param runner_name: Alternatively identify the runner book to extract by the runner's name
    :param handicap: The handicap of the desired runner book
    :param return_type: Optionally specify the return type to be either a dict or RunnerBook. If not given then the return type will reflect the type of market_book; if market_book is a dictionary then the return value is a dictionary. If market_book is a MarketBook object then the return value will be a RunnerBook object
    :returns: If market_book is None then None. Otherwise, the corresponding runner book if it can be found in the market book, otherwise None. The runner might not be found either because the given selection ID/runner name is not present in the market book or because the market book is missing some required fields such as the market definition. The type of the return value will depend on the return_type parameter
    :raises: ValueError if both selection_id and runner_name are given. Only one is required to uniquely identify the runner book
    """
    if market_book is None:
        return None

    if selection_id is not None and runner_name is not None:
        raise ValueError("Both selection_id and runner_name were given")
    if return_type is not None and not (
        return_type is dict or return_type is RunnerBook
    ):
        raise TypeError(
            f"return_type must be either dict or RunnerBook ({return_type} given)"
        )

    if isinstance(market_book, MarketBook):
        market_book = market_book._data
        return_type = return_type or RunnerBook
    else:
        return_type = return_type or dict

    if selection_id is None:
        for runner in market_book.get("marketDefinition", {}).get("runners", []):
            if runner.get("name") == runner_name:
                selection_id = runner.get("id")
                break
        if selection_id is None:
            return

    for runner in market_book.get("runners", []):
        if (
            runner.get("selectionId") == selection_id
            and runner.get("handicap", 0) == handicap
        ):
            return return_type(**runner)


def get_best_price_size(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[dict[str, Union[int, float]], PriceSize]]:
    if isinstance(runner, RunnerBook):
        return next(iter(getattr(runner.ex, side.ex_attribute)), None)
    else:
        return next(iter(runner.get("ex", {}).get(side.ex_key, [])), None)


def get_mid_price(
    runner: Union[dict[str, Any], RunnerBook]
) -> Optional[Union[int, float]]:
    best_back = get_best_price(runner, Side.BACK)
    best_lay = get_best_price(runner, Side.LAY)
    if best_back is not None and best_lay is not None:
        return (best_back + best_lay) / 2


def get_best_price(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[int, float]]:
    """
    Get the best price available on a runner on side Side. This is a convenience function which retrieves the best price/size pair using get_best_price_size then returns the price field

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side: Indicate whether to get the best available back or lay price
    :return: The best price if one exists otherwise None
    """
    best_price_size = get_best_price_size(runner, side)
    if isinstance(best_price_size, PriceSize):
        return best_price_size.price
    elif isinstance(best_price_size, dict):
        return best_price_size["price"]


def get_price_size_by_depth(
    runner: Union[dict[str, Any], RunnerBook], side: Side, depth: int
) -> Optional[Union[dict[str, Union[int, float]], PriceSize]]:
    if isinstance(runner, RunnerBook):
        available = getattr(runner.ex, side.ex_attribute)
    else:
        available = runner.get("ex", {}).get(side.ex_key, [])

    if len(available) > depth:
        return available[depth]


def get_second_best_price_size(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[dict[str, Union[int, float]], PriceSize]]:
    return get_price_size_by_depth(runner=runner, side=side, depth=1)


def get_second_best_price(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[int, float]]:
    second_best_price_size = get_second_best_price_size(runner, side)
    if isinstance(second_best_price_size, PriceSize):
        return second_best_price_size.price
    elif isinstance(second_best_price_size, dict):
        return second_best_price_size["price"]


def iterate_price_sizes(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Iterator[Union[dict[str, Any], PriceSize]]:
    if isinstance(runner, RunnerBook):
        _iter = iter(getattr(runner.ex, side.ex_attribute))
    else:
        _iter = iter(runner.get("ex", {}).get(side.ex_key, []))

    return _iter


def iterate_prices(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Generator[Union[int, float], None, None]:
    if isinstance(runner, RunnerBook):
        price_sizes = getattr(runner.ex, side.ex_attribute)
    else:
        price_sizes = runner.get("ex", {}).get(side.ex_key, [])

    if len(price_sizes) > 0:
        if isinstance(price_sizes[0], dict):
            accessor_fun = dict.__getitem__
        else:
            accessor_fun = getattr
        for price_size in price_sizes:
            yield accessor_fun(price_size, "price")


def is_market_contiguous(
    runner: Union[dict[str, Any], RunnerBook],
    side: Side,
    max_depth: Optional[int] = None,
) -> Optional[bool]:
    """
    Check whether there are no gaps between ladder levels on one side of a runner book. Optionally restrict the check to a maximum number of ladder levels

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side: Indicate whether to check the best available back or lay prices for gaps
    :param max_depth: Optionally restrict the check up to the i-th ladder level (0-indexed)
    :return: If the market is empty or only has one level the function returns None as the idea of it being contiguous under these conditions is nonsensical. Otherwise, it will return True if all of the prices are successive points on the Betfair price ladder and False otherwise
    """
    if max_depth is not None and max_depth < 1:
        raise ValueError(f"If given, max_depth must be 1 or greater")

    prices = [
        price
        for depth, price in enumerate(iterate_prices(runner, side))
        if max_depth is None or depth <= max_depth
    ]

    if len(prices) < 2:
        return

    for price, next_price in zip(prices, prices[1:]):
        if side.next_worse_price_map[price] != next_price:
            return False

    return True


def get_best_price_with_rollup(
    runner: Union[dict[str, Any], RunnerBook], side: Side, rollup: Union[int, float]
) -> Optional[Union[int, float]]:
    """
    Get the best price available on a runner on side Side when rolling up any volumes less than rollup

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side: Indicate whether to get the best available back or lay price
    :param rollup: Any prices with volumes under this amount will be rolled up to lower levels in the order book
    :return: The best price if one exists otherwise None
    """
    cumulative_size = 0
    for price_size in iterate_price_sizes(runner, side):
        if isinstance(price_size, dict):
            price = price_size["price"]
            size = price_size["size"]
        else:
            price = price_size.price
            size = price_size.size
        cumulative_size += size
        if cumulative_size >= rollup:
            return price


def get_inside_best_price(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[int, float]]:
    """
    Get the price one step up (side == Side.BACK) or down (side == Side.LAY) the Betfair price ladder from a runner's best available price

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side:
    :return: If the runner has any prices and the best price is not at the end of the ladder then the price one step better than the best available price. Otherwise None
    """
    best_price = get_best_price(runner, side)
    return side.next_better_price_map.get(best_price)


def get_outside_best_price(
    runner: Union[dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[int, float]]:
    """
    Get the price one step down (side == Side.BACK) or up (side == Side.LAY) the Betfair price ladder from a runner's best available price

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side:
    :return: If the runner has any prices and the best price is not at the end of the ladder then the price one step worse than the best available price. Otherwise None
    """
    best_price = get_best_price(runner, side)
    return side.next_worse_price_map.get(best_price)


def get_spread(runner: Union[dict[str, Any], RunnerBook]) -> Optional[int]:
    """
    Get the spread - the difference between the best available to lay and best available to back prices - on a runner in terms of number of steps on the Betfair price ladder

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :return: If the runner has no prices on either side then None otherwise the difference between the best available to lay and best available to back prices in terms of number of steps on the Betfair price ladder
    """
    best_back_price = get_best_price(runner, Side.BACK)
    if best_back_price is None:
        return
    best_lay_price = get_best_price(runner, Side.LAY)
    if best_lay_price is None:
        return
    return calculate_price_difference(best_lay_price, best_back_price)


def is_market_one_tick_wide(runner: Union[dict[str, Any], RunnerBook]) -> bool:
    spread = get_spread(runner)
    return spread == 1


def get_market_id_from_string(
    s: str, as_integer: bool = False
) -> Optional[Union[str, int]]:
    """
    Searches the given string for a market ID in the form 1.234567890 and returns it if one is found

    :param s: The string to search
    :param as_integer: Whether a found market ID should be returned as a string in the form "1.234567890" or whether the prefix "1." should be dropped and 234567890 returned as an integer
    :return: If no market ID can be found in the string then None. Otherwise, the market ID either as a string or integer according to the as_integer parameter
    """
    match = MARKET_ID_PATTERN.search(s)
    if match:
        market_id = match.group(0)
        if as_integer:
            market_id = int(market_id[2:])
        return market_id


def get_event_id_from_string(s: str) -> Optional[int]:
    """
    Searches the given string for an event ID in the form 12345678 and returns it if one is found. Take care to only use this function where it makes sense; if it is called on a string that contains a market ID instead of an event ID the returned integer will be nonsense

    :param s: The string to search
    :return: If a substring matching the pattern 12345678 is found then that substring otherwise None
    """
    match = EVENT_ID_PATTERN.search(s)
    if match:
        return int(match.group(0))


def get_race_id_from_string(s: str) -> Optional[str]:
    """
    Searches the given string for a race ID in the form 12345678.1234 and returns it if one is found

    :param s: The string to search
    :return: If a substring matching the pattern 12345678.1234 is found then that substring otherwise None
    """
    match = RACE_ID_PATTERN.search(s)
    if match:
        return match.group(0)


def get_selection_id_to_runner_name_map_from_market_catalogue(
    market_catalogue: Union[dict[str, Any], MarketCatalogue]
) -> dict[int, str]:
    if isinstance(market_catalogue, dict):
        runners = market_catalogue["runners"]
    else:
        runners = market_catalogue.runners

    selection_id_to_runner_name_map = {}
    for runner in runners:
        if isinstance(runner, dict):
            selection_id_to_runner_name_map[runner["selectionId"]] = runner[
                "runnerName"
            ]
        else:
            selection_id_to_runner_name_map[runner.selection_id] = runner.runner_name

    return selection_id_to_runner_name_map


def get_bsp_from_market_definition(
    market_definition: Union[dict[str, Any], MarketDefinition]
) -> dict[int, Optional[Union[int, float]]]:
    """
    Extract a dictionary mapping selection ID to Betfair starting price from a market definition object. Only gives a
    meaningful result when a market definition from after reconciliation of Betfair starting price is used

    :param market_definition: A market definition either as a dictionary or betfairlightweight MarketDefinition object
        from which to extract the Betfair starting prices
    :return: A dictionary mapping selection ID to Betfair starting price
    """
    if isinstance(market_definition, dict):
        bsp = {
            runner["id"]: runner.get("bsp") for runner in market_definition["runners"]
        }
    else:
        bsp = {runner.selection_id: runner.bsp for runner in market_definition.runners}
    return bsp


def get_bsp_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> dict[int, Optional[Union[int, float]]]:
    """
    Extract a dictionary mapping selection ID to Betfair starting price from the given prices file. Practically
    speaking, the final market definition is extracted from the prices file then the Betfair starting prices are
    extracted from that market definition

    :param path_to_prices_file: The prices file from which to extract the Betfair starting prices
    :return: A dictionary mapping selection ID to Betfair starting price
    """
    market_definition = get_final_market_definition_from_prices_file(
        path_to_prices_file
    )
    return get_bsp_from_market_definition(market_definition)


def get_winners_from_market_definition(
    market_definition: Union[dict[str, Any], MarketDefinition]
) -> list[int]:
    if isinstance(market_definition, dict):
        selection_ids = [
            runner["id"]
            for runner in market_definition["runners"]
            if runner["status"] == "WINNER"
        ]
    else:
        selection_ids = [
            runner.selection_id
            for runner in market_definition.runners
            if runner.status == "WINNER"
        ]
    return selection_ids


def get_winners_from_prices_file(path_to_prices_file: Union[str, Path]) -> list[int]:
    market_definition = get_final_market_definition_from_prices_file(
        path_to_prices_file
    )
    return get_winners_from_market_definition(market_definition)


def get_final_market_definition_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> Optional[dict[str, Any]]:
    """
    Get the last occurring market definition from the given prices file. This is typically useful for determining the outcome (winner) of the market

    :param path_to_prices_file: The prices file to search
    :return: None if there are no market definitions in the file, otherwise the last one found, as a dictionary
    """
    import orjson
    import smart_open

    the_line = None
    with smart_open.open(path_to_prices_file, "rb") as f:
        for line in f:
            if b"marketDefinition" in line:
                the_line = line

    if the_line is not None:
        market_definition = orjson.loads(the_line)["mc"][0]["marketDefinition"]
        return market_definition


def create_market_definition_generator_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> Generator[tuple[int, dict[str, Any]], None, None]:
    import orjson
    import smart_open

    with smart_open.open(path_to_prices_file, "rb") as f:
        for line in f:
            if b"marketDefinition" in line:
                message = orjson.loads(line)
                publish_time = message["pt"]
                for mc in message["mc"]:
                    market_definition = mc.get("marketDefinition")
                    if market_definition is not None:
                        yield publish_time, market_definition


def get_all_market_definitions_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> list[tuple[int, dict[str, Any]]]:
    return list(
        create_market_definition_generator_from_prices_file(path_to_prices_file)
    )


def get_first_market_definition_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> Optional[dict[str, Any]]:
    _, market_definition = next(
        create_market_definition_generator_from_prices_file(path_to_prices_file),
        (None, None),
    )
    return market_definition


def get_last_pre_event_market_book_from_prices_file(
    path_to_prices_file: Union[str, Path], filter_suspended: bool = True
) -> Optional[dict[str, Any]]:
    """
    Search a prices file for the last market book before the market turned in play

    :param path_to_prices_file: The prices file to search
    :param filter_suspended: Optionally ignore any pre-event market books where the market status is SUSPENDED
    :return: The last pre-event market book, where the status is not SUSPENDED if filter_suspended has been set to True, provided one such market book exists in the prices file. If not then None will be returned
    """
    g = create_market_book_generator_from_prices_file(path_to_prices_file)
    pre_event_market_book = None
    for market_book in g:
        if market_book["inplay"]:
            return pre_event_market_book
        if (
            not (filter_suspended and market_book["status"] == "SUSPENDED")
            and market_book["status"] != "CLOSED"
        ):
            pre_event_market_book = market_book

    if (
        pre_event_market_book is not None
        and not pre_event_market_book["marketDefinition"]["turnInPlayEnabled"]
    ):
        return pre_event_market_book


def get_pre_event_volume_traded_from_prices_file(
    path_to_prices_file: Union[str, Path],
) -> Optional[Union[int, float]]:
    pre_event_market_book = get_last_pre_event_market_book_from_prices_file(
        path_to_prices_file,
        filter_suspended=False,
    )
    if pre_event_market_book is not None:
        pre_event_volume_traded = calculate_total_matched(pre_event_market_book)
        return pre_event_volume_traded


def get_inplay_publish_time_from_prices_file(
    path_to_prices_file: Union[str, Path], as_datetime: bool = False
) -> Optional[Union[int, datetime.datetime]]:
    g = create_market_book_generator_from_prices_file(path_to_prices_file)
    for market_book in g:
        if market_book["inplay"]:
            publish_time = market_book["publishTime"]
            if as_datetime:
                publish_time = publish_time_to_datetime(publish_time)
            return publish_time


def get_inplay_bet_delay_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> Optional[int]:
    g = create_market_book_generator_from_prices_file(path_to_prices_file)
    for market_book in g:
        if market_book["inplay"]:
            return market_book["marketDefinition"]["betDelay"]


def get_total_volume_traded_from_prices_file(
    path_to_prices_file: Union[str, Path], deque_len: Optional[int] = 8
) -> Optional[Union[int, float]]:
    """
    Working out the total volume traded on a market is surprisingly tricky given Betfair's shenanigans around the market
    closure. Specifically, the last few updates in the price stream result in data getting zeroed out which means if you
    were to just look at the last available market book it would look like the total volume traded was zero. In fact, it
    appears there is no consistency as to how many price stream updates are involved in this zeroing of data and so it's
    not as simple as having a rule such as "look at the second to last market book available".

    If memory usage is not an issue then the most robust method would be to read the entire set of market books into
    memory then iterate them in reverse order looking for the first market book that has a non-zero total volume traded.
    However, in many use cases memory usage is a significant concern. For example, if you want to process tens of
    thousands of markets and use parallel processing to speed this up then you end up needing to hold the entire set of
    market books for multiple markets in memory simultaneously and, depending on the number of cores your machine has
    and the number of parallel processes you use, it is easy to exhaust the total memory available.

    The solution presented here is to use a deque
    (https://docs.python.org/3/library/collections.html#collections.deque). Think of this as a limited length list
    where when a new item is appended to the list, another item is removed for the start of the list. This is used when
    reading the prices file to leave us with the X last market books which can then be iterated in reverse order to find
    the last one which has a non-zero volume traded. Based on my own analysis of prices files I have settled on a
    default maximum length of the deque of 8 - i.e. at most this function will store 8 market books in memory at any one
    time. This is a trade-off between giving correct results and drastically cutting down on memory usage versus reading
    the entire set of market books into memory. If memory usage is not of concern to you then you can simply set the
    deque_len argument to None and the function will read the entire set of market books into memory ensuring
    correctness. If you observe any prices files where a deque of length 8 doesn't give correct results then please
    create a GitHub issue here: https://github.com/mberk/betfairutil/issues so I can investigate.

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one
        stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be
        compressed or uncompressed
    :param deque_len: The length of the deque used to limit memory usage when working out the total volume traded. See
        discussion above. This can be set to None to use the entire set of market books which should ensure correctness
        but dramatically increase memory usage
    :return: The calculated total volume traded if it is available or None if the market was pulled by Betfair (i.e. all
        runners have a status of "REMOVED")
    """
    g = create_market_book_generator_from_prices_file(path_to_prices_file)
    market_books = deque(g, deque_len)
    for market_book in reversed(market_books):
        volume_traded = calculate_total_matched(market_book)
        if volume_traded > 0:
            return volume_traded
        if all(
            runner["status"] == "REMOVED"
            for runner in market_book["marketDefinition"]["runners"]
        ):
            return None

    if len(market_books) > 0:
        return 0


def _is_exchange_win_market(d: dict[str, Any]) -> bool:
    return d["marketType"] == "WIN" and d["marketId"].startswith("1.")


def get_bsp_from_race_result(
    race_result: Union[dict[str, Any], str, Path]
) -> dict[int, Optional[Union[int, float]]]:
    """
    Extract a dictionary mapping selection ID to Betfair starting price from a race result object scraped from the
    undocumented RaceCard endpoint (see
    https://github.com/betcode-org/betfair/blob/master/betfairlightweight/endpoints/racecard.py)

    :param race_result: Either the race result object as a dictionary or the path to a JSON file containing the race
        result object
    :return: a dictionary mapping selection ID to Betfair starting price
    """
    race_result = _load_json_object(race_result)

    bsp = {}
    for runner in race_result["runners"]:
        for selection in runner.get("selections", []):
            if _is_exchange_win_market(selection):
                selection_id = int(selection["selectionId"])
                bsp[selection_id] = selection["bsp"]
                break

    return bsp


def get_winners_from_race_result(
    race_result: Union[dict[str, Any], str, Path]
) -> list[int]:
    """
    Extract a list of winning selection IDs from a race result object scraped from the undocumented RaceCard endpoint
    (see https://github.com/betcode-org/betfair/blob/master/betfairlightweight/endpoints/racecard.py)

    :param race_result: Either the race result object as a dictionary or the path to a JSON file containing the race
        result object
    :return: a list of winning selection IDs
    """
    if isinstance(race_result, (str, Path)):
        import orjson
        import smart_open

        with smart_open.open(race_result, "r") as f:
            race_result = orjson.loads(f.read())

    winning_selection_ids = []
    for runner in race_result["runners"]:
        if runner.get("position") != "1":
            continue

        for selection in runner.get("selections", []):
            if _is_exchange_win_market(selection):
                winning_selection_ids.append(int(selection["selectionId"]))

    return winning_selection_ids


def get_race_distance_in_metres_from_race_card(
    race_card: Union[dict[str, Any], str, Path]
) -> float:
    """
    Extract the race distance from a race card object in yards then convert it to metres

    :param race_card: Either the race card object as a dictionary or the path to a JSON file
        containing the race card object
    :return: the race distance in metres
    """
    race_card = _load_json_object(race_card)
    distance_in_yards = race_card["race"]["distance"]
    distance_in_metres = convert_yards_to_metres(distance_in_yards)
    return distance_in_metres


def get_is_jump_from_race_card(race_card: Union[dict[str, Any], str, Path]) -> bool:
    race_card = _load_json_object(race_card)
    race_type = race_card["race"]["raceType"]["full"]
    return race_type in ("Chase", "Hurdle")


def get_win_market_id_from_race_card(
    race_card: Union[dict[str, Any], str, Path], as_integer: bool = False
) -> Optional[Union[int, str]]:
    """
    Search the markets contained in a race card for the one corresponding to the Exchange WIN
    market and return its market ID

    :param race_card: Either the race card object as a dictionary or the path to a JSON file
        containing the race card object
    :param as_integer: Should the market ID be returned as is - in the standard string form
        provided by Betfair that starts "1." - or an integer where the "1." prefix has been
        discarded
    :return: The market ID corresponding to the Exchange WIN market, as either an integer or
        a string according to as_integer, if such a market can be found in the race card
        otherwise None
    """
    race_card = _load_json_object(race_card)
    for market in race_card["race"]["markets"]:
        market_id = market["marketId"]
        if market["marketType"] == "WIN" and market_id.startswith("1."):
            if as_integer:
                market_id = int(market_id[2:])
            return market_id


def get_win_market_id_from_race_file(
    path_to_race_file: Union[str, Path],
) -> Optional[str]:
    for race_change in create_race_change_generator_from_race_file(path_to_race_file):
        return race_change["mid"]


def get_market_time_as_datetime(
    market_book_or_market_definition: Union[
        dict[str, Any], MarketBook, MarketDefinition
    ]
) -> datetime.datetime:
    """
    Extract the market time - i.e. the time at which the market is due to start - as a TIMEZONE AWARE datetime object

    :param market_book_or_market_definition: Either a market book either as a dictionary or betfairlightweight
        MarketBook object or a market definition either as a dictionary or betfairlightweight MarketDefinition object
        from which to extract the market (start) time
    :return: The market (start) time as a TIMEZONE AWARE datetime object
    """
    if isinstance(market_book_or_market_definition, MarketBook):
        market_time_datetime = (
            market_book_or_market_definition.market_definition.market_time.replace(
                tzinfo=datetime.timezone.utc
            )
        )
    elif isinstance(market_book_or_market_definition, MarketDefinition):
        market_time_datetime = market_book_or_market_definition.market_time.replace(
            tzinfo=datetime.timezone.utc
        )
    else:
        market_time_string = (
            market_book_or_market_definition["marketDefinition"]["marketTime"]
            if "marketDefinition" in market_book_or_market_definition
            else market_book_or_market_definition["marketTime"]
        )
        market_time_datetime = datetime.datetime.strptime(
            market_time_string, "%Y-%m-%dT%H:%M:%S.000Z"
        ).replace(tzinfo=datetime.timezone.utc)

    return market_time_datetime


def get_seconds_to_market_time(
    market_book_or_market_definition: Union[
        dict[str, Any], MarketBook, MarketDefinition
    ],
    current_time: Optional[Union[int, datetime.datetime]] = None,
) -> float:
    """
    Given a market book or market definition and an optional notional current time, extract the market (start) time from
    the market book/definition and calculate the difference in seconds between it and the current time. The current_time
    MUST be provided if using a market definition. If using a market book and current_time is not provided then the
    publish time of the market book will be used

    :param market_book_or_market_definition: Either a market book either as a dictionary or betfairlightweight
        MarketBook object or a market definition either as a dictionary or betfairlightweight MarketDefinition object
    :param current_time: An optional notional current time, either as an integer number of milliseconds since the Unix
        epoch or a datetime object. If market_book_or_market_definition is a market definition then this MUST be
        provided
    :return: The number of seconds between the market time and current_time if provided, otherwise the number of seconds
        between the market time and the publish time of the market book
    """
    market_time = get_market_time_as_datetime(market_book_or_market_definition)

    if current_time is None:
        if isinstance(market_book_or_market_definition, MarketBook):
            current_time = market_book_or_market_definition.publish_time.replace(
                tzinfo=datetime.timezone.utc
            )
        elif isinstance(market_book_or_market_definition, MarketDefinition):
            raise ValueError(
                "current_time argument must be provided if market_book_or_market_definition is a market definition"
            )
        else:
            current_time = market_book_or_market_definition["publishTime"]

    if isinstance(current_time, int):
        current_time = publish_time_to_datetime(current_time)

    seconds_to_market_time = (market_time - current_time).total_seconds()
    return seconds_to_market_time


def decrement_price(price: Union[int, float]) -> Optional[Union[int, float]]:
    """
    Given a price return the next lower price on the Betfair price ladder

    :param price: The price to decrement
    :return: None if price is at the end of the ladder - i.e. 1.01 - or price does not exist on the ladder - i.e. it is not a valid Betfair price
    """
    return BETFAIR_PRICE_TO_NEXT_PRICE_DOWN_MAP.get(price)


def increment_price(price: Union[int, float]) -> Optional[Union[int, float]]:
    """
    Given a price return the next higher price on the Betfair price ladder

    :param price: The price to increment
    :return: None if price is at the end of the ladder - i.e. 1000 - or price does not exist on the ladder - i.e. it is not a valid Betfair price
    """
    return BETFAIR_PRICE_TO_NEXT_PRICE_UP_MAP.get(price)


def is_market_book(x: Any) -> bool:
    """
    Test whether x is a betfairlightweight MarketBook object or a dictionary (mapping) with all required fields to construct one (as would be generated when using betfairlightweight in lightweight mode)

    :param x: The object to test
    :returns: True if x meets the above condition otherwise False
    """
    if isinstance(x, MarketBook):
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
    if isinstance(x, RunnerBook):
        return True
    try:
        RunnerBook(**x)
        return True
    except TypeError:
        return False


def is_price_the_same_or_better(
    a: Union[int, float], b: Union[int, float], side: Side
) -> bool:
    """
    Test whether a price is the same or better than another price from the perspective of betting on a given Side. For example, if Side is Side.BACK then a price of 1.49 is worse than a price of 1.5 but a price of 1.51 is better than a price of 1.5

    :param a: The price to test if it is the same or better
    :param b: The price to compare against
    :param side: The perspective from which to make the comparison. If side is Side.BACK then better prices are prices which are larger. If side is Side.LAY then better prices are prices which are smaller
    :return: True if a is the same or better than b otherwise False
    :raises: TypeError if side is not a valid value of the Side enum
    """
    if side == Side.BACK:
        return a >= b
    elif side == Side.LAY:
        return a <= b
    else:
        raise TypeError("side must be of type Side")


def is_price_worse(a: Union[int, float], b: Union[int, float], side: Side) -> bool:
    """
    Test whether a price is worse than another price from the perspective of betting on a given Side. For example, if Side is Side.BACK then a price of 1.49 is worse than a price of 1.5 but a price of 1.51 is better than a price of 1.5

    :param a: The price to test if it is worse
    :param b: The price to compare against
    :param side: The perspective from which to make the comparison. If side is Side.BACK then worse prices are prices which are smaller. If side is Side.LAY then worse prices are prices which are larger
    :return: True if a is worse than b otherwise False
    :raises: TypeError if side is not a valid value of the Side enum
    """
    return not is_price_the_same_or_better(a, b, side)


def iterate_active_runners(
    market_book: Union[dict[str, Any], MarketBook]
) -> Generator[Union[dict[str, Any], RunnerBook], None, None]:
    for runner in filter_runners(market_book, "ACTIVE", []):
        yield runner


def iterate_other_active_runners(
    market_book: Union[dict[str, Any], MarketBook], selection_id: int
) -> Generator[Union[dict[str, Any], RunnerBook], None, None]:
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

    index = fun(BETFAIR_PRICES, price) + offset

    if index < 0 or index > len(BETFAIR_PRICES) - 1:
        return None

    return BETFAIR_PRICES[index]


def market_book_to_data_frame(
    market_book: Union[dict[str, Any], MarketBook],
    should_output_runner_names: bool = False,
    should_output_runner_statuses: bool = False,
    should_format_publish_time: bool = False,
    max_depth: Optional[int] = None,
    _format: DataFrameFormatEnum = DataFrameFormatEnum.FULL_LADDER,
) -> "pd.DataFrame":
    """
    Construct a data frame representation of a market book. Each row is one point on the price ladder for a particular
    runner

    :param market_book: A market book either as a dictionary or betfairlightweight MarketBook object
    :param should_output_runner_names: Should the data frame contain a runner name column. This requires the market book to have been generated from streaming data and contain a MarketDefinition
    :param should_output_runner_statuses: Should the data frame contain a column indicating the status for each runner (ACTIVE, REMOVED, WINNER, LOSER)
    :param should_format_publish_time: Should the publish time (if present in the market book) be output as is (an integer number of milliseconds) or as an ISO 8601 formatted string
    :param max_depth: Optionally limit the depth of the price ladder. Should only be used when format is DataFrameFormatEnum.FULL_LADDER
    :param _format: Controls the output of the data frame. Currently, there are two options: either the full price ladder (DataFrameFormatEnum.FULL_LADDER) or just the last price traded (DataFrameFormatEnum.LAST_PRICE_TRADED)
    :return: A data frame whose format is determined by the format parameter. In the case of DataFrameFormatEnum.FULL_LADDER format, each row is one point on the price ladder for a particular runner. The data frame has the following columns:

      - market_id: The Betfair market ID
      - inplay: Whether the market is in play
      - selection_id: The selection ID of the runner
      - handicap: The handicap of the runner
      - side: Either 'Back' or 'Lay'
      - depth: The depth of this point on the ladder
      - price: The price of this point on the ladder
      - size: The amount of volume available at this point on the ladder
      - runner_status: (Optional): If should_output_runner_statuses is True then this column will be present
      - publish_time (Optional): If the market book was generated from streaming data (as opposed to calling the listMarketBook API endpoint) then the publish time of the market book. Otherwise this column will not be present
      - runner_name: (Optional): If should_output_runner_names is True then this column will be present. It will be populated if the market book was generated from streaming data (as opposed to calling the listMarketBook API endpoint) otherwise all entries will be None

    In the case of DataFrameFormatEnum.LAST_PRICE_TRADED format, there is only one row per runner. The columns are as above except:

      - There are no side, depth, price or size columns
      - Instead there is a last_price_traded column
    """
    assert not (
        format == DataFrameFormatEnum.LAST_PRICE_TRADED and max_depth is not None
    )

    import pandas as pd

    if isinstance(market_book, MarketBook):
        market_book = market_book._data

    if _format == DataFrameFormatEnum.FULL_LADDER:
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
                **(
                    {"runner_status": runner["status"]}
                    if should_output_runner_statuses
                    else {}
                ),
            }
            for runner in market_book["runners"]
            for side in ["Back", "Lay"]
            for depth, price_size in enumerate(
                runner.get("ex", {}).get(f"availableTo{side}", [])
            )
            if max_depth is None or depth <= max_depth
        )
    else:
        df = pd.DataFrame(
            {
                "market_id": market_book["marketId"],
                "inplay": market_book["inplay"],
                "selection_id": runner["selectionId"],
                "handicap": runner["handicap"],
                "last_price_traded": runner["lastPriceTraded"],
                **(
                    {"runner_status": runner["status"]}
                    if should_output_runner_statuses
                    else {}
                ),
            }
            for runner in market_book["runners"]
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
    path_to_prices_file: Union[str, Path], path_to_csv_file: Union[str, Path], **kwargs
) -> None:
    prices_file_to_data_frame(path_to_prices_file, **kwargs).to_csv(
        path_to_csv_file, index=False
    )


def prices_file_to_data_frame(
    path_to_prices_file: Union[str, Path],
    should_output_runner_names: bool = False,
    should_output_runner_statuses: bool = False,
    should_format_publish_time: bool = False,
    should_restrict_to_inplay: bool = False,
    max_depth: Optional[int] = None,
    market_definition_fields: dict[str, str] = None,
    market_type_filter: Optional[Sequence[str]] = None,
    market_catalogues: Optional[Sequence[Union[dict[str, Any], MarketBook]]] = None,
    _format: DataFrameFormatEnum = DataFrameFormatEnum.FULL_LADDER,
) -> "pd.DataFrame":
    """
    Read a Betfair prices file (either from the official historic data or data recorded from the streaming API in the same format) directly into a data frame

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
    :param should_output_runner_names: Should the data frame contain a runner name column. For efficiency, the names are added once the entire file has been processed. If market_catalogues is given then this is ignored as it is assumed the intention with providing market_catalogues is to include the runner names
    :param should_output_runner_statuses: Should the data frame contain a column indicating the status for each runner (ACTIVE, REMOVED, WINNER, LOSER)
    :param should_format_publish_time: Should the publish time be output as is (an integer number of milliseconds) or as an ISO 8601 formatted string. For efficiency, this formatting is applied once the entire file has been processed
    :param should_restrict_to_inplay: Should only prices where the market was in play be output
    :param max_depth: Optionally limit the depth of the price ladder. Should only be used when format is DataFrameFormatEnum.FULL_LADDER
    :param market_definition_fields: An optional map that controls which marketDefinition fields should be included in the data frame. The keys are the marketDefinition field names and the values are the name to use for the column in the data frame. For example, {"marketType": "market_type"}
    :param market_type_filter: Optionally filter out market types which do not exist in the given sequence
    :param market_catalogues: Optionally provide a list of market catalogues, as either dicts or betfairlightweight MarketCatalogue objects, that can be used to add runner names to the data frame. Only makes sense when the prices file has been recorded from the streaming API
    :param _format: Controls the output of the data frame. Currently, there are two options: either the full price ladder (DataFrameFormatEnum.FULL_LADDER) or just the last price traded (DataFrameFormatEnum.LAST_PRICE_TRADED)
    :return: A data frame whose format is determined by the format parameter. In the case of DataFrameFormatEnum.FULL_LADDER format, each row is one point on the price ladder for a particular runner at a particular publish time. The data frame has the following columns:

      - market_id: The Betfair market ID
      - inplay: Whether the market is in play
      - selection_id: The selection ID of the runner
      - handicap: The handicap of the runner
      - side: Either 'Back' or 'Lay'
      - depth: The depth of this point on the ladder
      - price: The price of this point on the ladder
      - size: The amount of volume available at this point on the ladder
      - runner_status: (Optional): If should_output_runner_statuses is True then this column will be present
      - publish_time: The publish time of the market book corresponding to this data point
      - runner_name: (Optional): If should_output_runner_names is True then this column will contain the name of the runner

    In the case of DataFrameFormatEnum.LAST_PRICE_TRADED format, there is one row per runner per publish time. The columns are as above except:

      - There are no side, depth, price or size columns
      - Instead there is a last_price_traded column
    """
    assert not (
        format == DataFrameFormatEnum.LAST_PRICE_TRADED and max_depth is not None
    )

    import pandas as pd
    from unittest.mock import patch

    open_mocker = _OpenMocker(path_to_prices_file)

    market_definition_fields = market_definition_fields or {}
    market_catalogues = market_catalogues or []
    if market_catalogues:
        should_output_runner_names = True

    snapped_market_books = []

    def g():
        snapped_market_books.extend(
            (
                yield from create_market_book_generator_from_prices_file(
                    path_to_prices_file
                )
            )
        )

    with patch("builtins.open", open_mocker.open):
        df = pd.concat(
            market_book_to_data_frame(
                mb,
                should_output_runner_statuses=should_output_runner_statuses,
                max_depth=max_depth,
                _format=_format,
            )
            for mb in g()
            if (
                market_type_filter is None
                or mb["marketDefinition"]["marketType"] in market_type_filter
            )
            and (not should_restrict_to_inplay or mb["inplay"])
        )
        if should_format_publish_time:
            df["publish_time"] = pd.to_datetime(
                df["publish_time"], unit="ms", utc=True
            ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if should_output_runner_names:
            selection_id_to_runner_name_map = {
                **{
                    runner["id"]: runner.get("name")
                    for mb in snapped_market_books
                    for runner in mb.get("marketDefinition", {}).get("runners", [])
                },
                **{
                    selection_id: runner_name
                    for mc in market_catalogues
                    for selection_id, runner_name in get_selection_id_to_runner_name_map_from_market_catalogue(
                        mc
                    ).items()
                },
            }
            df["runner_name"] = df["selection_id"].apply(
                selection_id_to_runner_name_map.get
            )
        for market_definition_field, column_name in market_definition_fields.items():
            market_id_to_market_type_map = {
                mb["marketId"]: mb.get("marketDefinition", {}).get(
                    market_definition_field
                )
                for mb in snapped_market_books
            }
            df[column_name] = df["market_id"].apply(market_id_to_market_type_map.get)
        # Fix integer column types
        df["selection_id"] = df["selection_id"].astype(int)
        if "depth" in df.columns:
            df["depth"] = df["depth"].astype(int)
        return df


def publish_time_to_datetime(
    publish_time: Optional[int],
) -> Optional[datetime.datetime]:
    if publish_time is not None:
        return datetime.datetime.utcfromtimestamp(publish_time / 1000).replace(
            tzinfo=datetime.timezone.utc
        )


def datetime_to_publish_time(_datetime: Optional[datetime.datetime]) -> Optional[int]:
    if _datetime is not None:
        return int(_datetime.timestamp() * 1000)


def create_market_book_generator_from_prices_file(
    path_to_prices_file: Union[str, Path],
    lightweight: bool = True,
    market_type_filter: Optional[Sequence[str]] = None,
    **kwargs,
) -> Generator[
    Union[MarketBook, dict[str, Any]], None, list[Union[MarketBook, dict[str, Any]]]
]:
    from unittest.mock import patch

    open_mocker = _OpenMocker(path_to_prices_file)

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_prices_file,
        listener=StreamListener(
            max_latency=None, lightweight=lightweight, update_clk=False, **kwargs
        ),
    )

    with patch("builtins.open", open_mocker.open):
        g = stream.get_generator()
        for market_books in g():
            for market_book in market_books:
                if (
                    market_type_filter is None
                    or (
                        lightweight
                        and market_book["marketDefinition"]["marketType"]
                        in market_type_filter
                    )
                    or (
                        not lightweight
                        and market_book.market_definition.market_type
                        in market_type_filter
                    )
                ):
                    yield market_book

    return stream.listener.snap()


def get_market_books_from_prices_file(
    path_to_prices_file: Union[str, Path],
    publish_times: Sequence[int],
    lightweight: bool = True,
    **kwargs,
) -> dict[int, Optional[Union[MarketBook, dict[str, Any]]]]:
    """Extract the market books corresponding to the given publish times from a Betfair prices file

    The market books are extracted as a list of tuples of publish time and market book. If a publish time does not
    exactly appear in the prices file then the most recent market book prior to it will be returned. Any given publish
    times which come before the first market book in the prices file will have None paired with them. This function is
    far more memory efficient than using read_prices_file and bisect functions. Currently only intended to be used with
    prices files that contain data for a single Betfair market

    :param path_to_prices_file:
        Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any
        of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
    :param publish_times:
        The publish times of interest to which to attach market books
    :param lightweight:
        Passed to StreamListener. When True, the returned market books are dicts. When false, the returned market books
        are betfairlightweight MarketBook objects
        the streaming API
    :param kwargs:
        Passed to StreamListener
    :return:
        A dict mapping each publish time to the corresponding market book, either as a dict or betfairlightweight
        MarketBook object depending on whether the lightweight parameter is True or False respectively, if there is one
        otherwise None
    """
    g = create_market_book_generator_from_prices_file(
        path_to_prices_file=path_to_prices_file,
        lightweight=lightweight,
        **kwargs,
    )

    market_book = None
    previous_market_book = None
    result = {}
    _publish_times = sorted(publish_times)
    for market_book in g:
        if len(_publish_times) == 0:
            break

        while _publish_times[0] < market_book["publishTime"]:
            result[_publish_times[0]] = previous_market_book
            _publish_times.pop(0)
            if len(_publish_times) == 0:
                break

        previous_market_book = market_book

    # Add any leftover publish times after the last market book
    for pt in _publish_times:
        result[pt] = market_book
    return result


def get_minimum_book_percentage_market_books_from_prices_file(
    path_to_prices_file: Union[str, Path],
    publish_time_windows: Sequence[tuple[int, int]],
    lightweight: bool = True,
    **kwargs,
) -> dict[tuple[int, int], Optional[Union[MarketBook, dict[str, Any]]]]:
    for window in publish_time_windows:
        assert window[0] < window[1]

    g = create_market_book_generator_from_prices_file(
        path_to_prices_file=path_to_prices_file,
        lightweight=lightweight,
        **kwargs,
    )

    market_book = None
    previous_market_book = None
    previous_market_book_book_percentage = None
    result = {window: None for window in publish_time_windows}
    current_best_book_percentages = {window: None for window in publish_time_windows}
    _publish_time_windows = sorted(publish_time_windows, key=lambda x: x[0])
    for market_book in g:
        if len(_publish_time_windows) == 0:
            break

        while _publish_time_windows[0][1] < market_book["publishTime"]:
            if result[_publish_time_windows[0]] is None:
                result[_publish_time_windows[0]] = previous_market_book
            _publish_time_windows.pop(0)
            if len(_publish_time_windows) == 0:
                break

        if len(_publish_time_windows) == 0:
            break

        for window in _publish_time_windows:
            if window[0] < market_book["publishTime"]:
                if previous_market_book_book_percentage is not None and (
                    current_best_book_percentages[window] is None
                    or current_best_book_percentages[window]
                    > previous_market_book_book_percentage
                ):
                    result[window] = previous_market_book
                    current_best_book_percentages[window] = (
                        previous_market_book_book_percentage
                    )

        previous_market_book = market_book
        previous_market_book_book_percentage = calculate_book_percentage(
            previous_market_book,
            Side.BACK,
        )

    # Add any leftover publish times after the last market book
    last_market_book_book_percentage = calculate_book_percentage(market_book, Side.BACK)

    for window in _publish_time_windows:
        if (
            current_best_book_percentages[window] is None
            or current_best_book_percentages[window] > last_market_book_book_percentage
        ):
            result[window] = market_book

    return result


def create_race_change_generator_from_race_file(
    path_to_race_file: Union[str, Path],
) -> Generator[dict[str, Any], None, None]:
    from unittest.mock import patch

    open_mocker = _OpenMocker(path_to_race_file)

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_race_file,
        listener=StreamListener(max_latency=None, lightweight=True, update_clk=False),
        operation="raceSubscription",
    )
    with patch("builtins.open", open_mocker.open):
        g = stream.get_generator()
        for rcs in g():
            for rc in rcs:
                yield rc


def get_publish_time_from_object(o: Union[dict[str, Any], MarketBook]) -> int:
    _data = getattr(o, "_data", o)
    return _data.get("publishTime", _data.get("pt"))


def create_combined_market_book_and_race_change_generator(
    path_to_prices_file: Union[str, Path],
    path_to_race_file: Union[str, Path],
    lightweight: bool = True,
    market_type_filter: Optional[Sequence[str]] = None,
    **kwargs,
) -> Generator[tuple[bool, Union[MarketBook, dict[str, Any]]], None, None]:
    """
    Creates a generator for reading a Betfair prices file and a scraped race stream file simultaneously. The market book
    and race change objects will be interleaved and returned from the generator in publish time order. The generator
    will yield pairs of a boolean and an object where the boolean indicates whether the object is a market book (True)
    or a race change (False)

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one
        stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be
        compressed or uncompressed
    :param path_to_race_file: Where the scraped race stream file to be processed is located. This can be a local file,
        one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be
        compressed or uncompressed
    :param lightweight: Passed to the betfairlightweight StreamListener used to read the Betfair prices file. When True,
        the market books will be dicts. When False, the market books will be betfairlightweight MarketBook objects
    :param market_type_filter: Optionally filter out market books with a market type which does not exist in the given
        sequence. Generally only makes sense when the Betfair prices file contains multiple market types, such as
        the case of event-level official historic data files
    :param kwargs: Other arguments passed to the betfairlightweight StreamListener
    :return: A generator yielding pairs of a boolean and an object. The boolean indicates whether the object is a market
        book (True) or a race change (False)
    """
    market_book_generator = create_market_book_generator_from_prices_file(
        path_to_prices_file=path_to_prices_file,
        lightweight=lightweight,
        market_type_filter=market_type_filter,
        **kwargs,
    )
    race_change_generator = create_race_change_generator_from_race_file(
        path_to_race_file
    )
    yield from heapq.merge(
        ((True, mb) for mb in market_book_generator),
        ((False, rc) for rc in race_change_generator),
        key=lambda x: get_publish_time_from_object(x[1]),
    )


def read_prices_file(
    path_to_prices_file: Union[str, Path],
    lightweight: bool = True,
    market_type_filter: Optional[Sequence[str]] = None,
    market_catalogues: Optional[
        Sequence[Union[dict[str, Any], MarketCatalogue]]
    ] = None,
    **kwargs,
) -> Union[list[MarketBook], list[dict[str, Any]]]:
    """
    Read a Betfair prices file (either from the official historic data or data recorded from the streaming API in the same format) into memory as a list of dicts or betfairlightweight MarketBook objects

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
    :param lightweight: Passed to StreamListener. When True, the returned list contains dicts. When false, the returned list contains betfairlightweight MarketBook objects
    :param market_type_filter: Optionally filter out market books with a market type which does not exist in the given sequence. Generally only makes sense when reading files that contain multiple market types, such as event-level official historic data files
    :param market_catalogues: Optionally provide a list of market catalogues, as either dicts or betfairlightweight MarketCatalogue objects, that can be used to add runner names to the market books. Only makes sense when the prices file has been recorded from the streaming API
    :param kwargs: Passed to StreamListener
    :return: A list of market books, either as dicts or betfairlightweight MarketBook objects depending on whether the lightweight parameter is True or False respectively
    """
    market_catalogues = market_catalogues or []

    market_books = list(
        create_market_book_generator_from_prices_file(
            path_to_prices_file=path_to_prices_file,
            lightweight=lightweight,
            market_type_filter=market_type_filter,
            **kwargs,
        )
    )

    if (
        market_books
        and not does_market_book_contain_runner_names(market_books[0])
        and market_catalogues
    ):
        selection_id_to_runner_name_map = {
            selection_id: runner_name
            for mc in market_catalogues
            for selection_id, runner_name in get_selection_id_to_runner_name_map_from_market_catalogue(
                mc
            ).items()
        }
        for market_book in market_books:
            if lightweight:
                for runner in market_book["marketDefinition"]["runners"]:
                    if runner["id"] in selection_id_to_runner_name_map:
                        runner["name"] = selection_id_to_runner_name_map[runner["id"]]
            else:
                for runner in market_book.market_definition.runners:
                    if runner.selection_id in selection_id_to_runner_name_map:
                        runner.name = selection_id_to_runner_name_map[
                            runner.selection_id
                        ]

    return market_books


def read_race_file(path_to_race_file: Union[str, Path]) -> list[dict[str, Any]]:
    race_changes = list(create_race_change_generator_from_race_file(path_to_race_file))
    return race_changes


def get_race_change_from_race_file(
    path_to_race_file: Union[str, Path],
    gate_name: Optional[str] = None,
    publish_time: Optional[int] = None,
) -> Optional[dict[str, Any]]:
    """
    Search a recorded race file for the first update after the given criterion. You can search EITHER by the gate name,
    for example "1f" to get the first race change after entering the final furlong, OR by publish time. The latter is
    useful when cross-referencing with the Betfair price stream. This function is memory efficient in that only a single
    race change is held in memory at a time

    :param path_to_race_file: The path to the recorded race file to search
    :param gate_name: The Gmax gate name to search for, for example "1f"
    :param publish_time: The Betfair publish time to search for
    :return: The first race change meeting the search criterion
    :raises: AssertionError unless exactly one of gate_name and publish_time is not None
    """
    assert not (gate_name is not None and publish_time is not None)

    g = create_race_change_generator_from_race_file(path_to_race_file)
    if gate_name is not None:
        for race_change in g:
            if (race_change.get("rpc") or {}).get("g") == gate_name:
                return race_change
    else:
        for race_change in g:
            if race_change["pt"] >= publish_time:
                return race_change


def remove_bet_from_runner_book(
    runner_book: Union[dict[str, Any], RunnerBook],
    price: Union[int, float],
    size: Union[int, float],
    available_side: Side,
) -> Union[dict[str, Any], RunnerBook]:
    """
    Create a new runner book with a bet removed from the order book

    :param runner_book: The runner book from which the bet is going to be removed either as a dictionary or betfairlightweight RunnerBook object
    :param price: The price of the bet
    :param size: The size of the bet
    :param available_side: The side of the order book that the bet appears on
    :return: A new runner book with the bet removed. The type of the return value will reflect the type of runner_book. If the given price is not available on the given side then the new runner book will be identical to runner_book
    :raises: ValueError if size is greater than the size present in the order book
    """
    runner_book = pickle.loads(pickle.dumps(runner_book))
    if isinstance(runner_book, dict):
        ex = runner_book["ex"]
        price_sizes = ex[available_side.ex_key]
        assignment_fun = dict.__setitem__
        assignment_field = available_side.ex_key
    else:
        ex = runner_book.ex
        price_sizes = getattr(ex, available_side.ex_attribute)
        assignment_fun = setattr
        assignment_field = available_side.ex_attribute

    if len(price_sizes) == 0:
        return runner_book

    if isinstance(price_sizes[0], dict):
        constructor = dict
        accessor_fun = dict.__getitem__
    else:
        constructor = PriceSize
        accessor_fun = getattr

    # Validation.
    for price_size in price_sizes:
        _price = accessor_fun(price_size, "price")
        _size = accessor_fun(price_size, "size")
        if _price == price and _size < size:
            raise ValueError(
                f"size = {size} but only {_size} available to {available_side.ex_key} at {_price}"
            )

    new_price_sizes = [
        constructor(
            price=accessor_fun(price_size, "price"),
            size=accessor_fun(price_size, "size")
            - (size if accessor_fun(price_size, "price") == price else 0),
        )
        for price_size in price_sizes
        if accessor_fun(price_size, "price") != price
        or accessor_fun(price_size, "size") != size
    ]

    assignment_fun(ex, assignment_field, new_price_sizes)
    return runner_book


def random_from_market_id(market_id: Union[int, str]):
    """
    Maps a market ID to a real number in [0, 1)

    :param market_id: A market ID, either in the standard string form provided by Betfair that starts "1." or an integer where the "1." prefix has been discarded
    :return: A quasi-random number generated from the market ID. See random_from_positive_int for details
    """
    if isinstance(market_id, str):
        market_id = int(market_id[2:])

    return random_from_positive_int(market_id)


def random_from_positive_int(i: int):
    """
    Maps a positive integer to a real number in [0, 1) by calculating the n-th term of the low discrepancy sequence described here: http://extremelearning.com.au/unreasonable-effectiveness-of-quasirandom-sequences/

    :param i: A positive integer
    :return: The n-th term of the low discrepancy sequence described here: http://extremelearning.com.au/unreasonable-effectiveness-of-quasirandom-sequences/
    :raises: ValueError if i is not a positive integer
    """
    if not isinstance(i, int) or i <= 0:
        raise ValueError(f"{i} is not a positive integer")

    return (0.5 + _INVERSE_GOLDEN_RATIO * i) % 1


random_from_event_id = random_from_positive_int


def calculate_haversine_distance_between_runners(
    rrc_a: dict[str, Any], rrc_b: dict[str, Any]
) -> float:
    """
    Given two rrc objects from the Betfair race stream, calculate the approximate distance
    between the horses in metres using the haversine formula

    :param rrc_a: A rrc object from the Betfair race stream as a dictionary
    :param rrc_b: Another rrc object from the Betfair race stream as a dictionary
    :return: The approximate distance between the two horses in metres
    """
    delta_longitude = radians(rrc_a["long"]) - radians(rrc_b["long"])
    delta_latitude = radians(rrc_a["lat"]) - radians(rrc_b["lat"])
    d = (
        sin(delta_latitude * 0.5) ** 2
        + cos(radians(rrc_a["lat"]))
        * cos(radians(rrc_b["lat"]))
        * sin(delta_longitude * 0.5) ** 2
    )

    return 2 * _AVERAGE_EARTH_RADIUS_IN_METERS * asin(sqrt(d))


def get_number_of_jumps_remaining(rc: dict[str, Any]) -> Optional[int]:
    """
    Given a race change object, work out how many jumps there are between the _leader_ and the
    finishing line. If there are no jump locations present in the race change object, either
    because this is a flat race or because it comes from older data that lacks the jump
    locations, then None will be returned

    :param rc: A Betfair race change object as a Python dictionary
    :return: The number of jumps between the leader and the finishing line unless there are no
        jump locations present in the race change object in which case None
    """
    distance_remaining = (rc.get("rpc") or {}).get("prg")
    jumps = (rc.get("rpc") or {}).get("J") or []
    if distance_remaining is not None and len(jumps) > 0:
        return sum(j["L"] < distance_remaining for j in jumps)


def get_race_leaders(rc: dict[str, Any]) -> set[int]:
    """
    Given a race change object, return the set of selection IDs of horses which are closest to
    the finishing line

    :param rc: A Betfair race change object as a Python dictionary
    :return: A set containing the selection IDs corresponding to the horses which are in the
        lead. The size of the set may exceed 1 if multiple horses are tied for the lead
    """
    distances_remaining = sorted(rrc["prg"] for rrc in (rc.get("rrc") or []))
    if len(distances_remaining) > 0:
        leader_distance_remaining = distances_remaining[0]
        return {
            rrc["id"]
            for rrc in (rc.get("rrc") or [])
            if rrc["prg"] == leader_distance_remaining
        }
    else:
        return set()


def virtualise_two_runner_price(
    price: Union[int, float], side: Side, raw: bool = False
) -> Union[int, float]:
    raw_price = 1.0 / (1.0 - 1.0 / price)
    if raw:
        return raw_price
    virtual_price = make_price_betfair_valid(raw_price, side.other_side)
    return virtual_price
