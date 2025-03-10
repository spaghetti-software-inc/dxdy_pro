# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

import os
import sys

import dxdy.rtd.rtd_calcs as rtd_calcs
from dxdy.settings import Settings

import pickle
import duckdb
import pandas as pd

# disable default logger
from loguru import logger
logger.remove()
logger.add(sys.stderr, format="<blue>{time}</blue> <level>{level}</level> <white>{message}</white>", colorize=True)
logger.level("DEBUG")


if __name__ == "__main__":
    logger.info("This is dxdy v0.1 - 🍝 Spaghetti Software Inc")
    rtd_server = rtd_calcs.RtdCalcServer()
    rtd_server.run()
    