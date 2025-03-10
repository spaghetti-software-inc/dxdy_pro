import math
import random
import time
from collections import OrderedDict
from datetime import date

import duckdb
import numpy as np
import pandas as pd

from loguru import logger
import rich

import dxdy.db.utils as db_utils
from dxdy.settings import Settings

test_data_dir = Settings().get_test_data_dir() 

cache = OrderedDict()

import numpy as np

def compute_intraday_volatility(sigma_annual: float, dt_seconds: float = 0.1, kappa: float = 2.0):
    """
    Computes the intraday volatility over a given time step (default 0.1 seconds) from annualized volatility.
    
    Parameters:
        sigma_annual (float): Annualized volatility (e.g., 0.2 for 20%).
        dt_seconds (float): Time step in seconds (default is 0.1s).
        kappa (float): Noise amplification factor (default 2.0; adjust based on market microstructure).

    Returns:
        float: Intraday volatility for the given time step.
    """
    # Trading year assumptions: 252 days, 6.5 trading hours per day
    trading_seconds_per_year = 252 * 6.5 * 60 * 60  # 5,884,800 seconds
    
    # Golden ratio constant
    phi = (1 + np.sqrt(5)) / 2
    
    dt = dt_seconds / trading_seconds_per_year * phi
    # Compute the volatility for the given time step
    sigma_dt = sigma_annual * np.sqrt(dt)
    
    # Adjust for microstructure noise (bid-ask spreads, liquidity effects)
    sigma_dt_adjusted = kappa * sigma_dt
    
    return dt, sigma_dt_adjusted


# choose a random ticker and update the price based on a random walk
def real_time_api(positions_df):
    correlation_ids = positions_df['figi'].unique()

    dt_seconds = 1/10
    sigma = 0.2 # drift and vol
    dt, sigma_dt = compute_intraday_volatility(sigma, dt_seconds)
    mu = 0.07 * dt
    
    while(True):
        ticker_idx = random.randint(0, len(correlation_ids)-1)
        cid = correlation_ids[ticker_idx]
        
        if cid in cache:
            last_price = cache[cid]
        else:
            close_price = positions_df[positions_df["figi"] == cid]["close_price"].values[0]
            last_price = close_price
        
        last_price = last_price*math.exp(random.gauss(mu, sigma_dt))
        bid_ask_spread = random.choices([0.02, 0.04, 0.06], weights=[0.5, 0.3, 0.2])[0]
        bid_price = last_price - bid_ask_spread/2
        ask_price = last_price + bid_ask_spread/2

        cache[cid] = last_price
        
        time.sleep(dt_seconds)
        yield cid, last_price, bid_price, ask_price
        


def timeseries_market_data_api(db, figis, start_date : date, end_date : date) -> None:
    # load the historical market data for the given figis
    mkt_data = pd.read_parquet(test_data_dir / 'market_data.parquet')
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    
    with db_utils.DuckDBTemporaryTable(db, 'tmp_mkt_data', mkt_data) as tmp_table_name:
        qry = f"""
        INSERT INTO
            market_data (security_id, trade_date, close_price)
        SELECT
            s.security_id, m.trade_date, m.close_price
        FROM
            tmp_mkt_data m
        LEFT JOIN
            securities s
        ON
            m.figi = s.figi
        WHERE
            m.trade_date >= '{start_date_str}' AND m.trade_date <= '{end_date_str}'
        AND
            s.security_id IS NOT NULL
        """
        db.execute(qry)
        db.commit()
        
        logger.debug(f"Inserted historical market data for {end_date_str}")
        

def timeseries_fx_rates_data_api(db, start_date : date, end_date : date) -> None:
    # load the historical market data for the given figis
    fx_rates = pd.read_parquet(test_data_dir / 'fx_rates.parquet')
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    
    with db_utils.DuckDBTemporaryTable(db, 'tmp_fx_rates', fx_rates) as tmp_table_name:
        qry = f"""
        INSERT INTO
            fx_rates_data (fx_date, ccy, fx_rate)
        SELECT
            r.fx_date, r.ccy, r.fx_rate
        FROM
            tmp_fx_rates r
        WHERE
            r.fx_date > '{start_date_str}' AND r.fx_date <= '{end_date_str}'
        """
        db.execute(qry)
        db.commit()
        
        logger.debug(f"Inserted historical FX data for {end_date_str}")
        
        
def load_sector_mappings_data_api(db, figis) -> None:
    sectors_test_data = pd.read_csv(test_data_dir / 'sector_mappings.csv')
    res = sectors_test_data[sectors_test_data['figi'].isin(figis)]

    with db_utils.DuckDBTemporaryTable(db, 'tmp_sector_mappings', res) as tmp_table_name:
        qry = f"""
        INSERT INTO 
            sectors (sector_name, updated_by)
        SELECT 
            DISTINCT sector_name, 'SPGI-Q API'
        FROM 
            tmp_sector_mappings
        WHERE
            sector_name NOT IN (SELECT sector_name FROM sectors)
        """
        db.execute(qry)
        db.commit()
        
        qry = f"""
        INSERT INTO
            sector_mappings (security_id, sector_id, updated_by)
        SELECT
            s.security_id, sec.sector_id, 'SPGI-Q API'
        FROM
            tmp_sector_mappings smap
        LEFT JOIN
            securities s
        ON
            smap.figi = s.figi
        LEFT JOIN
            sectors sec
        ON
            smap.sector_name = sec.sector_name
        WHERE
            s.security_id NOT IN (SELECT security_id FROM sector_mappings)
        """
        db.execute(qry)
        db.commit()
        
        logger.debug(f"Inserted new sectors data for {res}")



def load_new_options_data_api(db, securities_data) -> None:
    options_test_data = pd.read_csv(test_data_dir / 'options.csv')
    res = options_test_data[options_test_data['figi'].isin(securities_data['figi'])]
    
    with db_utils.DuckDBTemporaryTable(db, 'tmp_spgi_options', res) as tmp_table_name:
        options_qry = f"""
        SELECT
            o.*,
            s.security_id,
            s.base_ticker,
            s.security_description,
            us.security_id AS underlying_security_id,
            us.base_ticker AS underlying_ticker
        FROM
            tmp_spgi_options o
        LEFT JOIN
            securities s
        ON
            -- o.security = s.figi (BLP API)
            o.figi = s.figi
        LEFT JOIN
            securities us
        ON
            o.underlying_figi = us.figi
        """
        option_securities = db.execute(options_qry).fetch_df()

        # find the options in the securities table with missing underlying securities
        na_securities = option_securities[option_securities.underlying_security_id.isna()]
        
        securities_test_data = pd.read_csv(test_data_dir / 'securities.csv')
        secs = securities_test_data[securities_test_data['figi'].isin(na_securities['underlying_figi'])]
        
        if secs.shape[0] == 0 and na_securities.shape[0] > 0:
            raise ValueError(f"Underlying securities not found for {na_securities['underlying_figi'].unique()}")
        
        load_new_securities_data_api(db, secs['figi'].unique())

        # re-run query
        option_securities = db.execute(options_qry).fetch_df()
        
        
        with db_utils.DuckDBTemporaryTable(db, 'tmp_options', option_securities) as tmp_table_name:
            qry = f"""
            INSERT INTO
                options (security_id, underlying_security_id, ticker, opra_symbol, figi, underlying_figi, contract_type, exercise_style, exch_code, shares_per_contract, strike_price, expiration_date, ccy, updated_by)
            SELECT
                o.security_id, underlying_security_id, o.ticker, opra_symbol, o.figi, underlying_figi, contract_type, exercise_style, o.exch_code, shares_per_contract, strike_price, expiration_date, o.ccy, 'SPGI-Q API'
            FROM
                tmp_options o
            LEFT JOIN
                securities s
            ON
                s.figi = o.figi
            WHERE
                s.figi NOT IN (SELECT figi FROM options)
            """
            db.execute(qry)
            db.commit()
        
        
        
        
        
def load_new_securities_data_api(db, figis) -> None:
    securities_test_data = pd.read_csv(test_data_dir / 'securities.csv')
    secs = securities_test_data[securities_test_data['figi'].isin(figis)]
    
    with db_utils.DuckDBTemporaryTable(db, 'tmp_securities', secs) as tmp_table_name:
        qry = f"""
        INSERT INTO 
            securities (base_ticker, exch_code, security_type_2, name, security_description, figi, isin, sedol, ccy, updated_by)
        SELECT 
            base_ticker, exch_code, security_type_2, name, security_description, figi, isin, sedol, ccy, 'SPGI-Q API'
        FROM 
            tmp_securities
        WHERE
            figi NOT IN (SELECT figi FROM securities)
        """
        db.execute(qry)
        db.commit()
        
        logger.debug(f"Inserted new securities data for {secs}")
        
    
    options = secs[secs['security_type_2'] == 'Option']
    
    if options.shape[0] > 0:
        load_new_options_data_api(db, options)
    
    cash_instruments = secs[secs['security_type_2'] != 'Option']
    load_sector_mappings_data_api(db, cash_instruments['figi'])
    


def load_trade_blotter_api(db, cob_date : date) -> None:
    date_str = cob_date.strftime("%Y-%m-%d")
    csv_file = test_data_dir / 'trade_blotter' / f'{date_str}.csv'
    
    # check if the file exists
    if not csv_file.exists():
        raise FileNotFoundError(f"Trade blotter file not found: {csv_file}")
    
    new_trades = pd.read_csv(csv_file)
    
    logger.debug(f"Loaded trades: {new_trades}")
    
    load_new_securities_data_api(db, new_trades['figi'].unique())
    
    # insert the new trades into the trades table
    with db_utils.DuckDBTemporaryTable(db, 'tmp_trades', new_trades) as tmp_table_name:
        qry = f"""
        INSERT INTO
            trades (trade_date, portfolio_id, security_id, quantity, price, updated_by)
        SELECT
            trade_date, portfolio_id, s.security_id, quantity, price, 'SPGI-Q API'
        FROM
            tmp_trades t
        LEFT JOIN
            securities s
        ON
            t.figi = s.figi
        WHERE
            s.security_id IS NOT NULL
        AND
            trade_date = '{date_str}'
        """
        db.execute(qry)
        db.commit()
        
        logger.debug(f"Inserted new trades data for {new_trades}")

def load_intraday_trade_blotter_api(cob_date : date) -> None:
    date_str = cob_date.strftime("%Y-%m-%d")
    csv_file = test_data_dir / 'trade_blotter' / f'{date_str}.csv'
    
    # check if the file exists
    if not csv_file.exists():
        return
    
    new_trades = pd.read_csv(csv_file)
    
    logger.debug(f"Loaded trades: {new_trades}")
    
    with Settings().get_db_connection(readonly=False) as db:
    
        load_new_securities_data_api(db, new_trades['figi'].unique())
    
        qry = f"""
        DELETE FROM
            trades
        WHERE
            trade_date = '{date_str}'
        AND
            updated_by = 'INTRADAY'
        """
        db.execute(qry)
        db.commit()
        
        # insert the new trades into the trades table
        with db_utils.DuckDBTemporaryTable(db, 'tmp_trades', new_trades) as tmp_table_name:
            qry = f"""
            INSERT INTO
                trades (trade_date, portfolio_id, security_id, quantity, price, updated_by)
            SELECT
                trade_date, portfolio_id, s.security_id, quantity, price, 'INTRADAY'
            FROM
                tmp_trades t
            LEFT JOIN
                securities s
            ON
                t.figi = s.figi
            WHERE
                s.security_id IS NOT NULL
            """
            db.execute(qry)
            db.commit()
            
            logger.debug(f"Inserted new trades data for {new_trades}")    