# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)
#
#   Job Scheduler Server                            
#  ---------------------------------------------------
#
#                                          calendar data |
#                                                        v
#                                                        |----------------------------------------------------------|
#                                                        | 
#               market close = 4pm                       |
#                            |                           |
#  |-------------------------|---------------------------|------------------------|------------------------|------------------------|
#  |          t-1            |            t0             |                        |                        |          t+1           |
#  |                         |                           |                        |                        |                        |
#  |-------------------------|---------------------------|------------------------|------------------------|------------------------|
#    cur_cob_date                next_cob_date                   (holidays)               (weekends)            next business day
#    (previous day)              (current day)
#    [database operating date]                           
#       
#    cur_cob_date date switches to next_cob_date after 17:00 local time
#    EOD market data is linked to calendar_data, so we do not update the calendar_data beyond next_cob_date
#
# EOD Operations
# -------------------
# 1. Update calendar data
# 2. Catch-up on missing data
# 3. Load transaction data
# 4. Load market data
# 5. Load FX rates data
# 6. Send EOD risk report
# 7. Backup DuckDB database
#


import sys
import argparse
import time
from datetime import date, datetime, timedelta

import pandas as pd

import rich
from rich.progress import Progress
from rich.traceback import install
install()


from loguru import logger
logger.remove()

log_level = "DEBUG"

log_format_stdout = "<blue>{time:%Y-%m-%d %I:%M:%S %p %Z}</blue> | <level>{level}</level> | <b>{message}</b>"
logger.add(sys.stderr, level=log_level, format=log_format_stdout, colorize=True, backtrace=False, diagnose=False)

log_format_file = "<blue>{time:%Y-%m-%d %I:%M:%S %p %Z}</blue> | <level>{level: <8}</level> | <yellow>Line {line: >4} ({file}) | </yellow> <b>{message}</b>"
logger.add("log_scheduler_dxdy.log", level=log_level, format=log_format_file, colorize=False, backtrace=True, diagnose=True)


import dxdy.eod.tasks as eod_tasks
import dxdy.db.utils as db_utils

from dxdy.settings import Settings
from dxdy.saas_settings import SaaSConfig



#RUN_TIME = datetime.strptime("17:00:00", "%H:%M:%S").time()
RUN_TIME = datetime.strptime("17:00:00", "%H:%M:%S").time()




def run_eod(db, start_date : date, cob_date : date, tplus_one : date):

    logger.info(f"⏱️ End-of-Day (EOD) {cob_date} tasks")
    


    start_date = db_utils.get_current_cob_date(db)  # t-1
    cob_date = db_utils.get_next_cob_date(db)     # t+0
    tplus_one = db_utils.get_t_plus_one_cob_date(db) # t+1



    logger.info("Loading transaction data")
    eod_tasks.task_load_transactions_data(db, cob_date=cob_date)

    logger.info("Loading div/splits data")
    eod_tasks.task_div_splits_data(db, start_date=start_date, cob_date=cob_date, tplus_one=tplus_one)

    logger.info("Loading stock/options market data")
    eod_tasks.task_load_market_data(db, start_date=start_date, cob_date=cob_date, tplus_one=tplus_one)

    logger.info("Loading FX market data")
    eod_tasks.task_load_fx_rates_data(db, start_date=start_date, cob_date=cob_date, tplus_one=tplus_one)
    
    eod_tasks.task_compute_daily_positions(db, asof_date=cob_date, prev_asof_date=start_date)
    
    logger.info("Updating calendar data")
    eod_tasks.task_update_calendar_data(db, end_date=tplus_one)

    # try:
    #     logger.info("Loading AI P&L analysis")
    #     eod_tasks.task_ai_pnl_analysis(db, cob_date=cob_date)
    # except Exception as e:
    #     logger.warning(f"Error loading AI P&L analysis: {e}")
        
    # try:
    #     logger.info("Loading AI market commentary")
    #     eod_tasks.task_ai_market_commentary(db, cob_date=cob_date)
    # except Exception as e:
    #     logger.warning(f"Error loading AI market commentary: {e}")
    
    # try:
    #     logger.info("Loading AI earnings analysis")
    #     eod_tasks.task_ai_earnings_analysis(db, cob_date=cob_date)
    # except Exception as e:
    #     logger.warning(f"Error loading AI earnings analysis: {e}")
    
    # try:
    #     logger.info("Loading AI technical analysis")
    #     eod_tasks.task_ai_technical_analysis(db, cob_date=cob_date)
    # except Exception as e:
    #     logger.warning(f"Error loading AI technical analysis: {e}")
    
    
    logger.info("Sending EOD risk report")
    #eod_tasks.task_send_eod_risk_report(db, cob_date)

    
    logger.info(f"🎉 End-of-Day (EOD) tasks completed for {cob_date}")


if __name__ == "__main__":
    rich.print(f"Python : {sys.version}")
    rich.print(f"log handlers: {logger._core.handlers}")
    print('\n')

    logger.info("This is ⋈ dxdy v1.0.0 Scheduler Module - 🍝 Spaghetti Software Inc")

    parser = argparse.ArgumentParser()

    parser.add_argument('-i', '--info', action='store_true', help='Display information about the scheduler and exit')
    parser.add_argument('-e', '--email', action='store_true', help='Email EOD reports and exit')

    args = parser.parse_args()
    
    
    cur_cob_date = db_utils.get_current_cob_date()  # t-1
    nxt_cob_date = db_utils.get_next_cob_date()     # t+0
    tplus_one_cob_date = db_utils.get_t_plus_one_cob_date() # t+1

    today = date.today()

    logger.info(f"Today is {today.strftime('%A, %B %d, %Y')}.")

    logger.info(f"Close-of-Business (COB) (T-1) date: {cur_cob_date.strftime('%A, %B %d, %Y')}")
    logger.info(f"Next COB (T0) date: {nxt_cob_date.strftime('%A, %B %d, %Y')}")
    logger.info(f"T+1 date: {tplus_one_cob_date.strftime('%A, %B %d, %Y')}")
    

    # display information and exit
    if args.info is True:
        exit(0)
        
    if args.email is True:
        logger.info("Emailing EOD reports")
        with Settings().get_db_connection(readonly=True) as db:
            eod_tasks.task_send_eod_risk_report(db, cur_cob_date)
            exit(0)
    
    next_run = datetime.combine(nxt_cob_date, RUN_TIME)
    now = datetime.now()
    if now < next_run:
        logger.info(f"Next run scheduled for {next_run.strftime('%A, %B %d, %Y %I:%M %p')}")
        logger.info("Exiting scheduler.")
        exit(0)

    logger.info("Backing up database")
    eod_tasks.task_backup_database(cur_cob_date)

    with Settings().get_db_connection(readonly=False) as db:
        run_eod(db, cur_cob_date, nxt_cob_date, tplus_one_cob_date)






