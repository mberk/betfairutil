import datetime
import enum
import itertools
import re
from bisect import bisect_left
from bisect import bisect_right
from copy import deepcopy
from math import sqrt
from pathlib import Path
from typing import Any, Dict, Generator, List, Optional, Sequence, Tuple, Union

import pandas as pd
from betfairlightweight import APIClient
from betfairlightweight import StreamListener
from betfairlightweight.resources.bettingresources import MarketBook
from betfairlightweight.resources.bettingresources import MarketCatalogue
from betfairlightweight.resources.bettingresources import PriceSize
from betfairlightweight.resources.bettingresources import RunnerBook
from betfairlightweight.resources.streamingresources import MarketDefinition

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
RACE_ID_PATTERN = re.compile(r"\d{8}\.\d{4}")
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


def does_market_book_contain_runner_names(
    market_book: Union[Dict[str, Any], MarketBook]
) -> bool:
    if type(market_book) is dict:
        market_definition = market_book["marketDefinition"]
    else:
        market_definition = market_book.market_definition
        if market_definition is None:
            return False

    return does_market_definition_contain_runner_names(market_definition)


def does_market_definition_contain_runner_names(
    market_definition: Union[Dict[str, Any], MarketDefinition]
) -> bool:
    if type(market_definition) is dict:
        runners = market_definition.get("runners", [])
    else:
        runners = market_definition.runners

    if len(runners) == 0:
        return False

    runner = runners[0]

    if type(runner) is dict:
        name = runner.get("name")
    else:
        name = runner.name

    return name is not None


def filter_runners(
    market_book: Union[Dict[str, Any], MarketBook],
    status: str,
    excluded_selection_ids: Sequence[int],
) -> Generator[Union[Dict[str, Any], RunnerBook], None, None]:
    if type(market_book) is dict:
        runners = market_book["runners"]
    else:
        runners = market_book.runners

    for runner in runners:
        if type(runner) is dict:
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
        return next(iter(getattr(runner.ex, side.ex_attribute)), None)
    else:
        return next(iter(runner.get("ex", {}).get(side.ex_key, [])), None)


def get_best_price(
    runner: Union[Dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[int, float]]:
    """
    Get the best price available on a runner on side Side. This is a convenience function which retrieves the best price/size pair using get_best_price_size then returns the price field

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side: Indicate whether to get the best available back or lay price
    :return: The best price if one exists otherwise None
    """
    best_price_size = get_best_price_size(runner, side)
    if type(best_price_size) is PriceSize:
        return best_price_size.price
    elif type(best_price_size) is dict:
        return best_price_size["price"]


def get_best_price_with_rollup(
    runner: Union[Dict[str, Any], RunnerBook], side: Side, rollup: Union[int, float]
) -> Optional[Union[int, float]]:
    """
    Get the best price available on a runner on side Side when rolling up any volumes less than rollup

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side: Indicate whether to get the best available back or lay price
    :param rollup: Any prices with volumes under this amount will be rolled up to lower levels in the order book
    :return: The best price if one exists otherwise None
    """
    if type(runner) is RunnerBook:
        _iter = iter(getattr(runner.ex, side.ex_attribute))
    else:
        _iter = iter(runner.get("ex", {}).get(side.ex_key, []))

    cumulative_size = 0
    for price_size in _iter:
        if type(price_size) is dict:
            price = price_size["price"]
            size = price_size["size"]
        else:
            price = price_size.price
            size = price_size.size
        cumulative_size += size
        if cumulative_size >= rollup:
            return price


def get_inside_best_price(
    runner: Union[Dict[str, Any], RunnerBook], side: Side
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
    runner: Union[Dict[str, Any], RunnerBook], side: Side
) -> Optional[Union[int, float]]:
    """
    Get the price one step down (side == Side.BACK) or up (side == Side.LAY) the Betfair price ladder from a runner's best available price

    :param runner: A runner book as either a betfairlightweight RunnerBook object or a dictionary
    :param side:
    :return: If the runner has any prices and the best price is not at the end of the ladder then the price one step worse than the best available price. Otherwise None
    """
    best_price = get_best_price(runner, side)
    return side.next_worse_price_map.get(best_price)


def get_spread(runner: Union[Dict[str, Any], RunnerBook]) -> Optional[int]:
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


def get_race_id_from_string(s: str) -> str:
    """
    Searches the given string for a race ID in the 12345678.1234 and returns it if one is found

    :param s: The string to search
    :return: If a substring matching the pattern 12345678.1234 is found then that substring otherwise None
    """
    match = RACE_ID_PATTERN.search(s)
    if match:
        return match.group(0)


def get_selection_id_to_runner_name_map_from_market_catalogue(
    market_catalogue: Union[Dict[str, Any], MarketCatalogue]
) -> Dict[int, str]:
    if type(market_catalogue) is dict:
        runners = market_catalogue["runners"]
    else:
        runners = market_catalogue.runners

    selection_id_to_runner_name_map = {}
    for runner in runners:
        if type(runner) is dict:
            selection_id_to_runner_name_map[runner["selectionId"]] = runner[
                "runnerName"
            ]
        else:
            selection_id_to_runner_name_map[runner.selection_id] = runner.runner_name

    return selection_id_to_runner_name_map


def get_final_market_definition_from_prices_file(
    path_to_prices_file: Union[str, Path]
) -> Optional[Dict[str, Any]]:
    """
    Get the last occurring market definition from the given prices file. This is typically useful for determining the outcome (winner) of the market

    :param path_to_prices_file: The prices file to search
    :return: None if there are no market definitions in the file, otherwise the last one found, as a dictionary
    """
    import orjson
    import smart_open

    market_definition = None
    with smart_open.open(path_to_prices_file, "rb") as f:
        for line in f:
            if b"marketDefinition" in line:
                market_definition = orjson.loads(line)["mc"][0]["marketDefinition"]

    return market_definition


def get_win_market_id_from_race_card(
    race_card: Dict[str, Any], as_integer: bool = False
) -> Optional[Union[int, str]]:
    for market in race_card["race"]["markets"]:
        market_id = market["marketId"]
        if market["marketType"] == "WIN" and market_id.startswith("1."):
            if as_integer:
                market_id = int(market_id[2:])
            return market_id


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

    index = fun(BETFAIR_PRICES, price) + offset

    if index < 0 or index > len(BETFAIR_PRICES) - 1:
        return None

    return BETFAIR_PRICES[index]


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
    path_to_prices_file: Union[str, Path], path_to_csv_file: Union[str, Path], **kwargs
) -> None:
    prices_file_to_data_frame(path_to_prices_file, **kwargs).to_csv(
        path_to_csv_file, index=False
    )


def prices_file_to_data_frame(
    path_to_prices_file: Union[str, Path],
    should_output_runner_names: bool = False,
    should_format_publish_time: bool = False,
    max_depth: Optional[int] = None,
    should_output_market_types: bool = False,
    market_type_filter: Optional[Sequence[str]] = None,
    market_catalogues: Optional[Sequence[Union[Dict[str, Any], MarketBook]]] = None,
) -> pd.DataFrame:
    """
    Read a Betfair prices file (either from the official historic data or data recorded from the streaming API in the same format) directly into a data frame

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
    :param should_output_runner_names: Should the data frame contain a runner name column. For efficiency, the names are added once the entire file has been processed. If market_catalogues is given then this is ignored as it is assumed the intention with providing market_catalogues is to include the runner names
    :param should_format_publish_time: Should the publish time be output as is (an integer number of milliseconds) or as an ISO 8601 formatted string. For efficiency, this formatting is applied once the entire file has been processed
    :param max_depth: Optionally limit the depth of the price ladder
    :param should_output_market_types: Should the data frame contain a market type column. Only makes sense when reading files that contain multiple market types, such as event-level official historic data files. For efficiency, the market types are added once the entire file has been processed
    :param market_type_filter: Optionally filter out market types which do not exist in the given sequence
    :param market_catalogues: Optionally provide a list of market catalogues, as either dicts or betfairlightweight MarketCatalogue objects, that can be used to add runner names to the data frame. Only makes sense when the prices file has been recorded from the streaming API
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

    market_catalogues = market_catalogues or []
    if market_catalogues:
        should_output_runner_names = True

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_prices_file,
        listener=StreamListener(max_latency=None, lightweight=True, update_clk=False),
    )

    with patch("builtins.open", smart_open.open):
        g = stream.get_generator()
        df = pd.concat(
            market_book_to_data_frame(mb, max_depth=max_depth)
            for mbs in g()
            for mb in mbs
            if market_type_filter is None
            or mb["marketDefinition"]["marketType"] in market_type_filter
        )
        if should_format_publish_time:
            df["publish_time"] = pd.to_datetime(
                df["publish_time"], unit="ms", utc=True
            ).dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        if should_output_runner_names:
            selection_id_to_runner_name_map = {
                **{
                    runner["id"]: runner.get("name")
                    for mb in stream.listener.snap()
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
        if should_output_market_types:
            market_id_to_market_type_map = {
                mb["marketId"]: mb.get("marketDefinition", {}).get("marketType")
                for mb in stream.listener.snap()
            }
            df["market_type"] = df["market_id"].apply(market_id_to_market_type_map.get)
        # Fix integer column types
        df["selection_id"] = df["selection_id"].astype(int)
        df["depth"] = df["depth"].astype(int)
        return df


def publish_time_to_datetime(publish_time: int) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(publish_time / 1000).replace(
        tzinfo=datetime.timezone.utc
    )


def read_prices_file(
    path_to_prices_file: Union[str, Path],
    lightweight: bool = True,
    market_type_filter: Optional[Sequence[str]] = None,
    market_catalogues: Optional[
        Sequence[Union[Dict[str, Any], MarketCatalogue]]
    ] = None,
    **kwargs,
) -> Union[List[MarketBook], List[Dict[str, Any]]]:
    """
    Read a Betfair prices file (either from the official historic data or data recorded from the streaming API in the same format) into memory as a list of dicts or betfairlightweight MarketBook objects

    :param path_to_prices_file: Where the Betfair prices file to be processed is located. This can be a local file, one stored in AWS S3, or any of the other options that can be handled by the smart_open package. The file can be compressed or uncompressed
    :param lightweight: Passed to StreamListener. When True, the returned list contains dicts. When false, the returned list contains betfairlightweight MarketBook objects
    :param market_type_filter: Optionally filter out market books with a market type which does not exist in the given sequence. Generally only makes sense when reading files that contain multiple market types, such as event-level official historic data files
    :param market_catalogues: Optionally provide a list of market catalogues, as either dicts or betfairlightweight MarketCatalogue objects, that can be used to add runner names to the market books. Only makes sense when the prices file has been recorded from the streaming API
    :param kwargs: Passed to StreamListener
    :return: A list of market books, either as dicts or betfairlightweight MarketBook objects depending on whether the lightweight parameter is True or False respectively
    """
    import smart_open
    from unittest.mock import patch

    market_catalogues = market_catalogues or []

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_prices_file,
        listener=StreamListener(
            max_latency=None, lightweight=lightweight, update_clk=False, **kwargs
        ),
    )

    with patch("builtins.open", smart_open.open):
        g = stream.get_generator()
        market_books = list(
            mb
            for mbs in g()
            for mb in mbs
            if market_type_filter is None
            or (
                lightweight
                and mb["marketDefinition"]["marketType"] in market_type_filter
            )
            or (
                not lightweight
                and mb.market_definition.market_type in market_type_filter
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


def read_race_file(path_to_race_file: Union[str, Path]) -> List[Dict[str, Any]]:
    import smart_open
    from unittest.mock import patch

    trading = APIClient(username="", password="", app_key="")
    stream = trading.streaming.create_historical_generator_stream(
        file_path=path_to_race_file,
        listener=StreamListener(max_latency=None, lightweight=True, update_clk=False),
        operation="raceSubscription",
    )
    with patch("builtins.open", smart_open.open):
        g = stream.get_generator()
        return [rc for rcs in g() for rc in rcs]


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
        for price_size in getattr(runner_book.ex, available_side.ex_attribute):
            if price_size.price == price and price_size.size < size:
                raise ValueError(
                    f"size = {size} but only {price_size.size} available to {available_side.ex_key} at {price_size.price}"
                )

        setattr(
            runner_book.ex,
            available_side.ex_attribute,
            [
                PriceSize(price=price_size.price, size=price_size.size)
                for price_size in getattr(runner_book.ex, available_side.ex_attribute)
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
