# Limitations:
#
# Yahoo! Finance API only returns adjusted close prices. This means that the 
# close price is adjusted for dividends and splits.
#
# This is incorrect. Unadjusted close prices should be used for P&L calculations,
# since the immutable transaction quantity is based on the unadjusted close price.
#
# The Yahoo! Finance API does not provide unadjusted close prices.
#
# Who still uses Yahoo! Finance API? 


import math
import random
import time
from datetime import date

import duckdb
import numpy as np
import pandas as pd

import yfinance as yf

from loguru import logger
import rich

import dxdy.db.reference_data as ref_data
import dxdy.db.utils as db_utils
from dxdy.settings import Settings

test_data_dir = Settings().get_test_data_dir() 

PERIOD = '2y'


def init_options(db) -> None:
    # load the historical market data for the given figis
    qry = """
    SELECT
        *
    FROM
        securities
    WHERE
        security_type_2 = 'Common Stock'
    """
    stocks = db.execute(qry).fetch_df()
    



def init_database(db, end_date) -> None:
        
    ################# setup currencies ################
    # USD
    qry = f"INSERT INTO currencies (ccy, currency_name) VALUES ('USD', 'US Dollar')"
    db.execute(qry)
    db.commit()

    # CAD
    qry = f"INSERT INTO currencies (ccy, currency_name) VALUES ('CAD', 'Canadian Dollar')"
    db.execute(qry)
                
    
    logger.info("Currencies initialized")

    ref_data.initialize_stocks(db)


    ################# setup dates table ################
    db_utils.insert_calendar_data(db, end_date)
    


    # load the historical market data for the given figis
    qry = """
    SELECT
        base_ticker
    FROM
        securities
    """
    tickers = db.execute(qry).fetch_df()['base_ticker'].tolist()
        
    
    ################ init security ################
    for ticker in tickers: # underlying security ticker
        logger.info(f"Initializing divs/splits data for {ticker}")
        
        try:
            dat = yf.Ticker(ticker)
            rich.print(dat)
            #time.sleep(random.uniform(0.5, 4))
        except:
            logger.warning(f"Failed to initialize {ticker}")
            continue
        
        ts = dat.history(period=PERIOD)
        ts = ts.reset_index()
        
        if "Dividends" in ts.columns:
            divs = ts[ts['Dividends'] != 0]
            rich.print(divs)
            
            try:
                with db_utils.DuckDBTemporaryTable(db, "tmp_divs", divs) as table:
                    qry = f"""
                    INSERT INTO
                        dividends (security_id, ex_dividend_date, cash_amount, ccy)
                    SELECT
                        s.security_id, Date, Dividends, 'USD'
                    FROM
                        tmp_divs tmp
                    LEFT JOIN
                        securities s
                    ON
                        s.base_ticker = '{ticker}'
                    """
                    db.execute(qry)
                    db.commit()
            except Exception as e:
                logger.error(f"Error loading divs for {ticker}: {e}")
            
        if "Stock Splits" in ts.columns:
            
            try:
                splits = ts[ts['Stock Splits'] != 0]
                rich.print(splits)
                
                with db_utils.DuckDBTemporaryTable(db, "tmp_splits", splits) as table:
                    qry = f"""
                    INSERT INTO
                        stock_splits (security_id, split_date, split_from, split_to)
                    SELECT
                        s.security_id, Date, 1, "Stock Splits", 
                    FROM
                        tmp_splits tmp
                    LEFT JOIN
                        securities s
                    ON
                        s.base_ticker = '{ticker}'
                    """
                    db.execute(qry)
                    db.commit()
            except Exception as e:
                logger.error(f"Error loading splits for {ticker}: {e}")
            

def timeseries_market_data_api(db, tickers, start_date : date, end_date : date) -> None:
    # # load the historical market data for the given figis
    # mkt_data = pd.read_parquet(test_data_dir / 'market_data.parquet')
    
    # start_date_str = start_date.strftime("%Y-%m-%d")
    # end_date_str = end_date.strftime("%Y-%m-%d")
    
    #rich.print(tickers)
    # rich.print(type(tickers ))
    
    logger.info(f"Loading historical market data from {start_date} to {end_date}")

    for ticker in tickers:
        try:
            df = yf.download(tickers = ticker, start = start_date, end = end_date, auto_adjust=False)
            rich.print(df)
        except:
            logger.warning(f"Failed to initialize {ticker}")
            continue
        
        df = df.stack(level=1, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1)
        df.reset_index(inplace=True)
        
        # rich.print(df)
        
        #df_wide = df.Close.reset_index()
        #rich.print(df_wide)
        
        # df_tall = pd.melt(df_wide, id_vars='Date', var_name='yf_ticker', value_name='close_price')
        # rich.print(df_tall)
        
        
        with db_utils.DuckDBTemporaryTable(db, "tmp_mkt_data", df) as tmp_mkt_data:
            # qry = f"""
            # DELETE FROM 
            #     market_data
            # WHERE
            #     security_id IN (SELECT security_id FROM securities WHERE base_ticker = '{ticker}')
            # AND
            #     trade_date BETWEEN '{start_date}' AND '{end_date}'
            # """
            # db.execute(qry)
            # db.commit()
            
            
            qry = f"""
            INSERT INTO
                market_data (security_id, trade_date, open_price, high_price, low_price, close_price, volume)
            SELECT
                s.security_id, Date, Open, High, Low, Close, Volume
            FROM
                tmp_mkt_data m
            LEFT JOIN
                securities s
            ON
                s.base_ticker = '{ticker}'
                
            """
            db.execute(qry)
            db.commit()

def timeseries_div_splits_data_api(db, figis, start_date : date, end_date : date) -> None:
    pass

def timeseries_fx_rates_data_api(db, start_date : date, end_date : date) -> None:

    #start_date = SaaSConfig().get_reporting_start_date()

    logger.info(f"Requesting FX data from {start_date} to {end_date}")
    with Settings().get_db_connection(readonly=False) as db:
        qry = """
        SELECT
            ccy
        FROM
            currencies
        WHERE
            ccy != 'USD'
        """
        curncies = db.execute(qry).fetch_df()
        curncies['ticker'] = curncies['ccy'] + 'USD=X'
        
        rich.print(curncies)

        for index, row in curncies.iterrows():
            logger.info(f"Downloading FX rates for {row.ticker} from {start_date} to {end_date}")
            
            df = yf.download(tickers = row.ticker, start = start_date, end   = end_date)
            df = df.stack(level=1, future_stack=True).rename_axis(['Date', 'Ticker']).reset_index(level=1)
            df.reset_index(inplace=True)
          
            rich.print(f"\n{df}")
            
            with db_utils.DuckDBTemporaryTable(db, "tmp_fx_data", df) as tmp_fx_data:
                qry = f"""
                INSERT INTO
                    fx_rates_data (fx_date, ccy, fx_rate)
                SELECT
                    Date, '{row.ccy}', Close
                FROM
                    tmp_fx_data m
                WHERE
                    Date = '{end_date}'    
                """
                db.execute(qry)
                db.commit()
                
        qry = f"""
        INSERT INTO
            fx_rates_data (fx_date, ccy, fx_rate)
        SELECT
            cob_date, 'USD', 1.0
        FROM
            calendar
        WHERE
            cob_date = '{end_date}'
        """
        db.execute(qry)
        db.commit()