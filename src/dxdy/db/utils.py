# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

import functools

import duckdb
import pandas as pd
import pandas_market_calendars as mkt_cal
from datetime import datetime, date, timedelta

from ..settings import Settings
from ..saas_settings import SaaSConfig

from loguru import logger
import rich

def log_event(task : str, message : str, is_error : bool = False):
    conn = duckdb.connect(Settings().get_log_file())
    conn.execute(f"INSERT INTO dxdy_log (task, message, is_error) VALUES ('{task}', '{message}', {is_error})")
    conn.close()

class DuckDBTemporaryTable:
    def __init__(self, conn, table_name, data_source):
        """
        Initialize the context manager for registering and unregistering a temporary table.

        Args:
            conn: A DuckDBPyConnection instance.
            table_name: The name of the temporary table.
            data_source: The data source to register (e.g., CSV path, Pandas DataFrame).
        """
        self.conn = conn
        self.table_name = table_name
        self.data_source = data_source

    def __enter__(self):
        """
        Register the temporary table when entering the context.
        """
        self.conn.register(self.table_name, self.data_source)
        return self.table_name  # Return the table name for use in queries.

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Unregister the temporary table when exiting the context.
        """
        self.conn.unregister(self.table_name)
        # Handle exceptions, if any, during the context execution.
        return False  # Do not suppress exceptions.



def get_current_cob_date(db_conn=None) -> date:
    close_db = False
    if db_conn is None:
        db_conn = Settings().get_db_connection()
        close_db = True

    qry = f"""
    SELECT
        MAX(market_close) AS cob_date
    FROM
        (
        SELECT
            MAX(market_close) AS market_close
        FROM
            calendar_data
        GROUP BY
            year(market_close), month(market_close), day(market_close)
        )
    WHERE
        market_close < (SELECT MAX(market_close) AS cob_date FROM calendar_data)
    """
    df = db_conn.execute(qry).fetchdf()
    df['cob_date'] = df['cob_date'].dt.date
    

    if close_db:
        db_conn.close()    
    
    return df.loc[0, 'cob_date']

        

def get_next_cob_date(db_conn=None) -> date:
    close_db = False
    if db_conn is None:
        db_conn = Settings().get_db_connection()
        close_db = True
        
    qry = f"""
    SELECT
        MAX(market_close) AS cob_date
    FROM
        (
        SELECT
            MAX(market_close) AS market_close
        FROM
            calendar_data
        GROUP BY
            year(market_close), month(market_close), day(market_close)
        )
    """
    df = db_conn.execute(qry).fetchdf()
    df['cob_date'] = df['cob_date'].dt.date

    if close_db:
        db_conn.close()
            
    return df.loc[0, 'cob_date']

@functools.cache
def get_calendar_obj(calendar_name):
    return mkt_cal.get_calendar(calendar_name)


def insert_calendar_data(db_conn, end_date = None) -> pd.DataFrame:
    logger.info(f"Inserting calendar data up to {end_date}")
    # if db_conn is None:
    #     db_conn = Settings().get_db_connection(readonly=False)
    
    qry = f"""
    SELECT
        MAX(cob_date) AS cob_date
    FROM 
        calendar
    ;
    """
    calendar_df = db_conn.execute(qry).fetchdf()
    calendar_df['cob_date'] = calendar_df['cob_date'].dt.date

    rich.print(calendar_df)
    
    if calendar_df.empty:
        start_date = SaaSConfig().get_reporting_start_date()
        logger.warning("No calendar data found. Loading from start date.")
        
    elif pd.isnull(calendar_df.iloc[0]['cob_date']):
        start_date = SaaSConfig().get_reporting_start_date()
        logger.warning("No calendar data found. Loading from start date.")
        
    else:
        start_date = calendar_df.iloc[0]['cob_date']
    
    if end_date is None:
        raise Exception("end date not specified")
    
    logger.info(f"Rolling calendar forward from {start_date} to {end_date}")
    
    calendars = Settings().get_calendars()

    schedules = []
    for calendar in calendars:
        calendar_obj = get_calendar_obj(calendar)
        sched = calendar_obj.schedule(start_date=start_date, end_date=end_date)
        sched['exchange'] = calendar
        schedules.append(sched)
    
    schedules = [sched for sched in schedules if not sched.empty and not sched.isna().all().all()]

    if len(schedules) == 0:
        logger.warning("No calendar data found.")
        return None

    updates = pd.concat(schedules).sort_values(by='market_open').reset_index(drop=True)
    updates['cob_date'] = updates['market_open'].dt.date
    
    #logger.debug(f"Loading calendar data: {updates} ")
    
    # register the new data
    db_conn.register('tmp_updates', updates)
    
    qry = f"""
    INSERT INTO 
        calendar_data (exchange, market_open, market_close)
    SELECT
        u.exchange, u.market_open, u.market_close
    FROM
        tmp_updates u
    LEFT JOIN
        calendar_data c
    ON
        u.exchange = c.exchange
    AND
        u.market_close = c.market_close
    WHERE
        c.exchange IS NULL
    ;"""
    db_conn.execute(qry)
    
    db_conn.unregister('tmp_updates')
    
    
    db_conn.commit()

    #logger.debug(f"Inserted calendar data: {updates}")

    # db_conn.close()
    
    return updates


def get_t_plus_one_cob_date(db_conn=None) -> date:
    close_db = False
    if db_conn is None:
        db_conn = Settings().get_db_connection()
        close_db = True
        
    cur_cob_date = get_next_cob_date(db_conn)
    calendars = Settings().get_calendars()
    
    next_cob_date = cur_cob_date
    
    n = 1
    while next_cob_date == cur_cob_date:        
        next_cob_datetimes = []
        for cal_name in calendars:
            
            calendar_obj = mkt_cal.get_calendar(cal_name)
            
            # Make sure the schedule includes guess_next_cob_date
            sched = calendar_obj.schedule(
                start_date=cur_cob_date, 
                end_date=next_cob_date + timedelta(days=n)
            )
            sched["cob_date"] = sched["market_close"].dt.date

            # The last row in the schedule will give us the next (or same) market_close date
            next_cob_datetime = sched.iloc[-1]["cob_date"]  
            next_cob_datetimes.append(next_cob_datetime)
            
        # If your goal is the earliest date valid for ALL calendars,
        # you typically need the maximum among them. 
        next_cob_date = max(next_cob_datetimes)
        n += 1


    if close_db:
        db_conn.close()
        
    return next_cob_date