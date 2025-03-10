import blpapi
import rich

from datetime import date
import pandas as pd

from blp import blp

import dxdy.db.utils as db_utils
from dxdy.settings import Settings
from dxdy.saas_settings import SaaSConfig

from loguru import logger

emsx_data_dir = SaaSConfig().get_emsx_directory()


MAX_BQRY_RETRY = 3

def get_bqry_session():
    for attempt in range(MAX_BQRY_RETRY):
        try:
            bqry = blp.BlpQuery(timeout = 90000).start()
            return bqry
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed to start the BlpQuery session: {e}")
            if attempt < MAX_BQRY_RETRY - 1:
                continue
            else:
                logger.error("All retry attempts failed.")
                return None
    

def real_time_api(tickers):

    # with Settings().get_db_connection(readonly=True) as db:
    #     """
    #     correlation_ids = db.execute(qry).fetch_df()
        
    correlation_ids = tickers['figi'].unique()
    
    req = {}
    
    for figi in correlation_ids:
        req[figi] = {"fields": ["LAST_PRICE", "BID", "ASK"]}
    
    #rich.print(req)
    
    while(True):
        with blp.BlpStream() as rt_blp:
            res = rt_blp.subscribe(req)
            #rich.print(res)

            # n = 0
            try:
                events =  rt_blp.events(timeout=1)
                rich.print(str(events))

                for ev in events:
                    # todo: check if elements in ev

                    utc_timestamp = ev['timeReceived']

                    correlation_ids = ev['correlationIds']
                    cid = correlation_ids[0] # ?

                    rt_mkt_data = ev['element']['MarketDataEvents']
                    #if not 'LAST_PRICE' in rt_mkt_data:
                    #    continue

                    last_price, bid_price, ask_price = None, None, None

                    if 'LAST_PRICE' in rt_mkt_data:
                        last_price = rt_mkt_data['LAST_PRICE']

                    if 'BID' in rt_mkt_data:
                        bid_price  = rt_mkt_data['BID']

                    if 'ASK' in rt_mkt_data:
                        ask_price  = rt_mkt_data['ASK']

                    # rich.print(ev)
                    # rich.print(f"{utc_timestamp} {ticker} {rt_quote}")
                    # n += 1
                    # if n > 2:
                    #     break

                    yield cid, last_price, bid_price, ask_price

            except Exception as e:
                yield cid, None, None, None
                #raise e
            


def timeseries_market_data_api(db, figis, start_date : date, end_date : date) -> None:
    #start_date = SaaSConfig().get_reporting_start_date()

    logger.info(f"Requesting market data from {start_date} to {end_date}")


    start_dt = start_date.strftime('%Y%m%d')
    end_dt   = end_date.strftime('%Y%m%d')

    ###################################################################
    bqry = blp.BlpQuery().start()
    
    mkt_data = bqry.bdh(figis, ["PX_LAST"], start_date=start_dt, end_date=end_dt, options={"adjustmentSplit": False})
    ###################################################################

    #logger.debug(mkt_data)

    # qry = "DELETE FROM market_data"
    # db.execute(qry)

    with db_utils.DuckDBTemporaryTable(db, 'tmp_mkt_data', mkt_data):
        qry = f"""
        SELECT
            d.date AS trade_date,
            s.security_id,
            d.PX_LAST AS close_price
        FROM
            tmp_mkt_data d
        LEFT JOIN
            securities s
        ON
            d.security = s.figi
        WHERE
            d.date NOT IN (SELECT DISTINCT(trade_date) FROM market_data)
        """
        mmkt_data = db.execute(qry).fetch_df()
        #rich.print(mmkt_data)

        with db_utils.DuckDBTemporaryTable(db, 'tmp_mmkt_data', mmkt_data):
            qry = f"""
            INSERT INTO
                market_data (security_id, trade_date, close_price)
            SELECT
                security_id, trade_date, close_price
            FROM
                tmp_mmkt_data m
            """
            db.execute(qry)
            db.commit()

def timeseries_div_splits_data_api(db, figis, start_date : date, end_date : date) -> None:


    start_dt = start_date.strftime('%Y%m%d')
    end_dt   = end_date.strftime('%Y%m%d')


    # dividends / splits
    ######################################################################

    logger.info(f"Requesting divs/splits reference data: {start_date} to {end_date}")

    bqry = blp.BlpQuery().start()

    divs_qry = blp.create_reference_query(figis,
                    ["DVD_HIST_ALL"], overrides=[("DVD_START_DT", start_dt), ("DVD_END_DT", end_dt)] )
    responses = bqry.query(divs_qry)

    dfs = []
    for response in responses:
        cid = response['security']
        data = response['data']['DVD_HIST_ALL']
        if not data is None:
            df = pd.json_normalize(data)
            df['cid'] = cid
            dfs.append(df)

    if len(dfs) == 0:
        return

    df = pd.concat(dfs)
    ######################################################################

    rich.print(df)

    with db_utils.DuckDBTemporaryTable(db, "tmp_divs", df) as tmp_table_name:

        # dividends
        qry = f"""
        INSERT INTO
            dividends (security_id, ex_dividend_date, record_date, pay_date, cash_amount, ccy, dividend_type)
        SELECT
            s.security_id, "Ex-Date", "Record Date", "Payable Date", "Dividend Amount", s.ccy, "Dividend Type"
        FROM
            tmp_divs d
        LEFT JOIN
            securities s
        ON
            d.cid = s.figi
        WHERE
            "Dividend Type" = 'Regular Cash'
        AND
            "Ex-Date" NOT IN (SELECT DISTINCT ex_dividend_date FROM dividends)
        """
        db.execute(qry)
        db.commit()


        # stock splits
        splits_qry = f"""
        SELECT
            "Ex-Date",
            "Record Date",
            "Payable Date",
            "Dividend Frequency",
            "Dividend Amount",
            "Dividend Type" ,
            CASE
                WHEN "Dividend Amount" < 1.0 THEN 1 / "Dividend Amount"
                ELSE 1.0
            END AS split_from,
            CASE
                WHEN "Dividend Amount" < 1.0 THEN 1.0
                ELSE "Dividend Amount"
            END AS split_to        
        FROM 
            tmp_divs
        WHERE
            "Dividend Type" = 'Stock Split'
        """
        splits_df = db.execute(splits_qry).fetch_df()
        if splits_df.shape[0] == 0:
            return

        with db_utils.DuckDBTemporaryTable(db, "tmp_splits", splits_df) as tmp_table_name2:
            qry = f"""
            INSERT INTO
                stock_splits (security_id, split_date, split_from, split_to)
            SELECT
                s.security_id, "Ex-Date", split_from, split_to
            FROM
                tmp_splits d
            LEFT JOIN
                securities s
            ON
                d.cid = s.figi
            WHERE
                "Ex-Date" NOT IN (SELECT DISTINCT split_date FROM stock_splits)
            """
            db.execute(qry)
            db.commit()
        

        # stock dividends
        stock_divs_qry = f"""
        SELECT
            "Ex-Date",
            "Record Date",
            "Payable Date",
            "Dividend Frequency",
            "Dividend Amount",
            "Dividend Type" ,
            1.0 AS split_from,
            ("Dividend Amount" + 1.0) AS split_to        
        FROM 
            tmp_divs
        WHERE
            "Ex-Date" > (SELECT MAX(ex_dividend_date) FROM dividends)
        AND
            "Dividend Type" = 'Stock Dividend'
        """
        stock_divs_df = db.execute(stock_divs_qry).fetch_df()
        if splits_df.shape[0] == 0:
            return

        with db_utils.DuckDBTemporaryTable(db, "tmp_stock_divs", stock_divs_df) as tmp_table_name3:
            qry = f"""
            INSERT INTO
                stock_splits (security_id, split_date, split_from, split_to)
            SELECT
                s.security_id, "Ex-Date", split_from, split_to
            FROM
                tmp_stock_divs
            """
            db.execute(qry)
            db.commmit()


def timeseries_fx_rates_data_api(db, start_date : date, end_date : date) -> None:

    #start_date = SaaSConfig().get_reporting_start_date()

    logger.info(f"Requesting FX data from {start_date} to {end_date}")

    qry = """
    SELECT
        ccy,
        CASE
            WHEN ccy = 'USD' THEN 'USD Curncy'
            WHEN ccy = 'CAD' THEN 'CADUSD Curncy'
            ELSE CONCAT('USD', ccy, ' Curncy')
        END AS ticker
    FROM
        currencies
    """
    curncies = db.execute(qry).fetch_df()
    #curncies['curncy'] = 'USD' + curncies['curncy'] + ' Curncy'
    tickers = curncies['ticker'].tolist()
    
    ######################################################################
    bqry = blp.BlpQuery().start()
    ######################################################################

    start_dt = start_date.strftime('%Y%m%d')
    end_dt   = (end_date).strftime('%Y%m%d')
    
    
    mkt_data = bqry.bdh(tickers, ["PX_LAST"], start_date=start_dt, end_date=end_dt)
    if mkt_data.shape[0] == 0:
        return


    mkt_data['trade_date'] = mkt_data['date'].dt.date
    #mkt_data['ccy'] = mkt_data['security'].str.replace(' Curncy', '')

    #rich.print(mkt_data)

    #qry = "DELETE FROM fx_rates_data"
    #db.execute(qry)

    with db_utils.DuckDBTemporaryTable(db, "tmp_currencies", curncies):
        with db_utils.DuckDBTemporaryTable(db, "tmp_mkt_data", mkt_data):
            qry = """
            INSERT INTO
                fx_rates_data (fx_date, ccy, fx_rate)
            SELECT
                trade_date, ccy, PX_LAST
            FROM
                tmp_mkt_data m
            LEFT JOIN
                tmp_currencies c
            ON
                m.security = c.ticker
            WHERE
                m.trade_date NOT IN (SELECT DISTINCT(fx_date) FROM fx_rates_data)
            """
            db.execute(qry)
            db.commit()
        
        
def load_new_options_data_api(db, figis) -> None:
    
    ###################################################################    
    bqry = blp.BlpQuery().start()
    blp_data = bqry.bdp(figis, 
                        ["ID_FULL_EXCHANGE_SYMBOL", "OPRA_SYMBOL", "ID_BB_GLOBAL", "UNDERLYING_ISIN", "UNDERLYING_SECURITY_DES", "OPT_PUT_CALL", "OPT_EXER_TYP", "EXCH_CODE", "OPT_MULTIPLIER", "OPT_STRIKE_PX", "OPT_EXPIRE_DT", "CRNCY"])

    ###################################################################   
     
        
    with db_utils.DuckDBTemporaryTable(db, 'tmp_blp_options', blp_data) as tmp_table_name:
        options_qry = f"""
        SELECT
            o.*,
            s.security_id,
            s.base_ticker,
            s.security_description,
            s.figi,
            us.security_id AS underlying_security_id,
            us.figi AS underlying_figi
        FROM
            tmp_blp_options o
        LEFT JOIN
            securities s
        ON
            o.ID_BB_GLOBAL = s.figi
        LEFT JOIN
            securities us
        ON
            o.UNDERLYING_ISIN = us.isin
        """
        option_securities = db.execute(options_qry).fetch_df()

        # find the options in the securities table with missing underlying securities
        na_securities = option_securities[option_securities.underlying_security_id.isna()]
        
        if na_securities.shape[0] > 0:
            ######################################################################
            df = bqry.bdp(na_securities['UNDERLYING_SECURITY_DES'].unique(), 
                        ["TICKER", "EXCH_CODE", "SECURITY_TYP2", "NAME", "SECURITY_DES", "ID_BB_GLOBAL", "ID_ISIN", "ID_SEDOL1", "CRNCY"]) 
            ######################################################################
            
            #rich.print(df)
            
            load_new_securities_data_api(db, df['ID_BB_GLOBAL'].unique())
            
        # re-run query
        option_securities = db.execute(options_qry).fetch_df()
        
        
        with db_utils.DuckDBTemporaryTable(db, 'tmp_options', option_securities) as tmp_table_name:
            qry = f"""
            INSERT INTO
                options (security_id, underlying_security_id, ticker, opra_symbol, figi, underlying_figi, contract_type, exercise_style, exch_code, shares_per_contract, strike_price, expiration_date, ccy, created_by)
            SELECT
                o.security_id, underlying_security_id, o.ID_FULL_EXCHANGE_SYMBOL, o.OPRA_SYMBOL, o.ID_BB_GLOBAL, underlying_figi, OPT_PUT_CALL, OPT_EXER_TYP, o.EXCH_CODE, OPT_MULTIPLIER, OPT_STRIKE_PX, OPT_EXPIRE_DT, o.CRNCY, 'dxdy'
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



def load_sector_mappings_data_api(db, figis) -> None:
    if len(figis) == 0:
        return

    ###################################################################    
    bqry = blp.BlpQuery().start()
    blp_data = bqry.bdp(figis,  ["ID_BB_GLOBAL", "GICS_SECTOR_NAME"])

    ###################################################################   

    with db_utils.DuckDBTemporaryTable(db, 'tmp_sector_mappings', blp_data) as tmp_table_name:
        qry = f"""
        INSERT INTO 
            sectors (sector_name, created_by)
        SELECT 
            DISTINCT GICS_SECTOR_NAME, 'dxdy'
        FROM 
            tmp_sector_mappings
        WHERE
            GICS_SECTOR_NAME NOT IN (SELECT sector_name FROM sectors)
        """
        db.execute(qry)
        db.commit()
        
        qry = f"""
        INSERT INTO
            sector_mappings (security_id, sector_id, created_by)
        SELECT
            s.security_id, sec.sector_id, 'dxdy'
        FROM
            tmp_sector_mappings smap
        LEFT JOIN
            securities s
        ON
            smap.ID_BB_GLOBAL = s.figi
        LEFT JOIN
            sectors sec
        ON
            smap.GICS_SECTOR_NAME = sec.sector_name
        WHERE
            s.security_id NOT IN (SELECT security_id FROM sector_mappings)
        """
        db.execute(qry)
        db.commit()
        
        #logger.debug(f"Inserted new sectors data for {blp_data}")
        
def load_new_securities_data_api(db, figis) -> None:

    ######################################################################
    bqry = blp.BlpQuery().start()
    secs = bqry.bdp(figis, ["TICKER", "EXCH_CODE", "MARKET_SECTOR_DES", "SECURITY_TYP2", "NAME", "SECURITY_DES", "ID_BB_GLOBAL", "ID_ISIN", "ID_SEDOL1", "CRNCY"])
    secs['full_ticker'] = secs['TICKER'] + ' ' + secs['EXCH_CODE'] + ' ' + secs['MARKET_SECTOR_DES']
    #secs.to_clipboard(index=False)
    
    #uhoh = bqry.bdp((secs['TICKER']+ ' ' + secs['EXCH_CODE'] +' Equity').tolist(), ["TICKER", "EXCH_CODE", "SECURITY_TYP2", "NAME", "SECURITY_DES", "ID_BB_GLOBAL", "ID_ISIN", "ID_SEDOL1", "CRNCY"])
    #uhoh.to_clipboard(index=False)   
    ######################################################################


    with db_utils.DuckDBTemporaryTable(db, 'tmp_securities', secs) as tmp_table_name:
        qry = f"""
        INSERT INTO 
            securities (base_ticker, exch_code, ticker, security_type_2, name, security_description, figi, isin, sedol, ccy, created_by)
        SELECT 
            TICKER, EXCH_CODE, full_ticker, SECURITY_TYP2, NAME, SECURITY_DES, ID_BB_GLOBAL, ID_ISIN, ID_SEDOL1, CRNCY, 'dxdy'
        FROM 
            tmp_securities
        WHERE
            ID_BB_GLOBAL NOT IN (SELECT figi FROM securities)
        """
        db.execute(qry)
        db.commit()
        
        #logger.debug(f"Inserted new securities data for {secs}")
        
    
    options = secs[secs['SECURITY_TYP2'] == 'Option']
    
    if options.shape[0] > 0:
        load_new_options_data_api(db, options['ID_BB_GLOBAL'])
    
    cash_instruments = secs[secs['SECURITY_TYP2'] != 'Option']
    load_sector_mappings_data_api(db, cash_instruments['ID_BB_GLOBAL'])
    




def load_trade_blotter_api(db, trade_date : date) -> None:
    logger.info(f"Loading trade blotter for {trade_date}")

    trade_blotter_files = SaaSConfig().get_emsx_csv_files(trade_date)
    if len(trade_blotter_files) == 0:
        return

    # rich.print(trade_blotter_files)
    
    # check if the files exist
    fills = []
    for file in trade_blotter_files:
        assert file.exists()
        fills.append(pd.read_csv(file))
    
    fills = [fi for fi in fills if not fi.empty and not fi.isna().all().all()]

    # TODO: delete trades for trade date before inserting

    if len(fills) == 0:
        return
    
    trade_blotter_df = pd.concat(fills).reset_index(drop=True)

    trade_blotter_df['Exec Date'] = pd.to_datetime(trade_blotter_df['Exec Date'], format="mixed")
    trade_blotter_df['trade_date'] = trade_blotter_df['Exec Date'].dt.date
    trade_blotter_df['SEDOL'] = trade_blotter_df['SEDOL'].astype(str).str.replace(r'\.0$', '', regex=True)

    
    rich.print(trade_blotter_df)
    # rich.print(trade_blotter_df.dtypes)

    stock_broker = SaaSConfig().get_emsx_stock_broker()
    options_broker = SaaSConfig().get_emsx_options_broker()

    with db_utils.DuckDBTemporaryTable(db, "tmp_trade_blotter", trade_blotter_df):
        qry = f"""
        SELECT
            t.*,
            CONCAT('/sedol/', t.SEDOL) AS cid
        FROM
            tmp_trade_blotter t
        JOIN
            (SELECT
                "Order Number" AS order_number,
                MAX("Exec Seq Number") AS exec_seq_number
            FROM
                tmp_trade_blotter
            WHERE
                Broker = '{stock_broker}'
            GROUP BY
                "Order Number") f
        ON
            t."Order Number" = f.order_number
        AND
            t."Exec Seq Number" = f.exec_seq_number
        WHERE
            t.Broker = '{stock_broker}'
        ;"""
        stock_trades_df = db.execute(qry).fetchdf()

        qry = f"""
        SELECT
            t.*
        FROM
            tmp_trade_blotter t
        JOIN
            (SELECT
                "Order Number" AS order_number,
                MAX("Exec Seq Number") AS exec_seq_number
            FROM
                tmp_trade_blotter
            WHERE
                Broker = '{options_broker}'
            GROUP BY
                "Order Number") f
        ON
            t."Order Number" = f.order_number
        AND
            t."Exec Seq Number" = f.exec_seq_number
        WHERE
            t.Broker = '{options_broker}'
        ;"""
        options_trades_df = db.execute(qry).fetchdf()

    logger.debug(f"Loading new stock trades:\n {stock_trades_df}")
    logger.debug(f"Loading new option trades:\n {options_trades_df}")


    ###################################################################    
    bqry = blp.BlpQuery().start()
    if stock_trades_df.shape[0] > 0:
        stocks_figis = bqry.bdp(stock_trades_df['cid'].unique(), ["ID_SEDOL1", "ID_BB_GLOBAL", "NAME", "TICKER_AND_EXCH_CODE", "CRNCY"])
        #stocks_figis.to_clipboard(index=False)
    

        # merge stock_trades_df with stocks_figis
        stock_trades_df = stock_trades_df.merge(stocks_figis, left_on='SEDOL', right_on='ID_SEDOL1', how='left')

    if options_trades_df.shape[0] > 0:
        options_trades_df['cid'] = options_trades_df['Ticker'] + ' Equity'
        options_figis = bqry.bdp(options_trades_df['cid'].unique(), ["Ticker", "ID_BB_GLOBAL"])

        # merge options_trades_df with options_figis
        options_trades_df = options_trades_df.merge(options_figis, left_on='cid', right_on='security', how='left')

    ###################################################################    

    
    # concatenate the two dataframes
    if stock_trades_df.shape[0] > 0 and options_trades_df.shape[0] > 0:
        new_trades = pd.concat([stock_trades_df, options_trades_df]).reset_index(drop=True)
    elif stock_trades_df.shape[0] > 0:
        new_trades = stock_trades_df
    elif options_trades_df.shape[0] > 0:
        new_trades = options_trades_df

    

    load_new_securities_data_api(db, new_trades['ID_BB_GLOBAL'].unique())
    

    # insert the new trades into the trades table
    with db_utils.DuckDBTemporaryTable(db, 'tmp_trades', new_trades) as tmp_table_name:
        qry = f"""
        INSERT INTO
            trades (trade_date, portfolio_id, security_id, quantity, price, created_by)
        SELECT
            trade_date, 
            portfolio_id, 
            s.security_id, 
            CASE 
                WHEN Side = 'B' THEN "Day Fill Amount" 
                WHEN Side = 'BS' THEN "Day Fill Amount"
                WHEN Side = 'S' THEN -1 * "Day Fill Amount"
                WHEN Side = 'SS' THEN -1 * "Day Fill Amount"
                ELSE "Day Fill Amount" 
            END AS quantity,
            "Day Avg Price",
            'dxdy'
        FROM
            tmp_trades t
        LEFT JOIN
            portfolios p
        ON
            t."Tran Account" = p.portfolio_name
        LEFT JOIN
            securities s
        ON
            t.ID_BB_GLOBAL = s.figi
        WHERE
            s.security_id IS NOT NULL
        """
        db.execute(qry)
        db.commit()
        
        # logger.debug(f"Inserted new trades data for {new_trades}")

def load_intraday_trade_blotter_api(cob_date : date) -> None:
    logger.info(f"Loading intraday trade blotter for {cob_date}")

    intraday_trade_blotter_files = SaaSConfig().get_emsx_csv_files(cob_date)
    if len(intraday_trade_blotter_files) == 0:
        return

    # check if the files exist
    fills = []
    for file in intraday_trade_blotter_files:
        assert file.exists()
        fills.append(pd.read_csv(file))
    
    fills = [fi for fi in fills if not fi.empty and not fi.isna().all().all()]
    

    if len(fills) == 0:
        return
    
    trade_blotter_df = pd.concat(fills).reset_index(drop=True)

    trade_blotter_df['Exec Date'] = pd.to_datetime(trade_blotter_df['Exec Date'], format="mixed")
    trade_blotter_df['trade_date'] = trade_blotter_df['Exec Date'].dt.date
    trade_blotter_df['SEDOL'] = trade_blotter_df['SEDOL'].astype(str).str.replace(r'\.0$', '', regex=True)
    
    with Settings().get_db_connection(readonly=False) as db:
    
        load_new_securities_data_api(db, new_trades['figi'].unique())
    
        qry = f"""
        DELETE FROM
            trades
        WHERE
            trade_date = '{cob_date}'
        AND
            updated_by = 'INTRADAY'
        """
        db.execute(qry)
        db.commit()
        
        stock_broker = SaaSConfig().get_emsx_stock_broker()
        options_broker = SaaSConfig().get_emsx_options_broker()

        with db_utils.DuckDBTemporaryTable(db, "tmp_trade_blotter", trade_blotter_df):
            qry = f"""
            SELECT
                t.*,
                CONCAT('/sedol/', t.SEDOL) AS cid
            FROM
                tmp_trade_blotter t
            JOIN
                (SELECT
                    "Order Number" AS order_number,
                    MAX("Exec Seq Number") AS exec_seq_number
                FROM
                    tmp_trade_blotter
                WHERE
                    Broker = '{stock_broker}'
                GROUP BY
                    "Order Number") f
            ON
                t."Order Number" = f.order_number
            AND
                t."Exec Seq Number" = f.exec_seq_number
            ;"""
            stock_trades_df = db.execute(qry).fetchdf()

            qry = f"""
            SELECT
                t.*
            FROM
                tmp_trade_blotter t
            JOIN
                (SELECT
                    "Order Number" AS order_number,
                    MAX("Exec Seq Number") AS exec_seq_number
                FROM
                    tmp_trade_blotter
                WHERE
                    Broker = '{options_broker}'
                GROUP BY
                    "Order Number") f
            ON
                t."Order Number" = f.order_number
            AND
                t."Exec Seq Number" = f.exec_seq_number
            ;"""
            options_trades_df = db.execute(qry).fetchdf()

        logger.debug(f"Loading new stock trades:\n {stock_trades_df}")
        logger.debug(f"Loading new option trades:\n {options_trades_df}")


        ###################################################################    
        bqry = blp.BlpQuery().start()
        if stock_trades_df.shape[0] > 0:
            stocks_figis = bqry.bdp(stock_trades_df['cid'].unique(), ["ID_SEDOL1", "ID_BB_GLOBAL", "NAME", "TICKER_AND_EXCH_CODE", "CRNCY"])
            #stocks_figis.to_clipboard(index=False)
        

            # merge stock_trades_df with stocks_figis
            stock_trades_df = stock_trades_df.merge(stocks_figis, left_on='SEDOL', right_on='ID_SEDOL1', how='left')

        if options_trades_df.shape[0] > 0:
            options_trades_df['cid'] = options_trades_df['Ticker'] + ' Equity'
            options_figis = bqry.bdp(options_trades_df['cid'].unique(), ["Ticker", "ID_BB_GLOBAL"])

            # merge options_trades_df with options_figis
            options_trades_df = options_trades_df.merge(options_figis, left_on='cid', right_on='security', how='left')

        ###################################################################    

        
        # concatenate the two dataframes
        if stock_trades_df.shape[0] > 0 and options_trades_df.shape[0] > 0:
            new_trades = pd.concat([stock_trades_df, options_trades_df]).reset_index(drop=True)
        elif stock_trades_df.shape[0] > 0:
            new_trades = stock_trades_df
        elif options_trades_df.shape[0] > 0:
            new_trades = options_trades_df

        

        load_new_securities_data_api(db, new_trades['ID_BB_GLOBAL'].unique())
        

        # insert the new trades into the trades table
        with db_utils.DuckDBTemporaryTable(db, 'tmp_trades', new_trades) as tmp_table_name:
            qry = f"""
            INSERT INTO
                trades (trade_date, portfolio_id, security_id, quantity, price, created_by)
            SELECT
                trade_date, 
                portfolio_id, 
                s.security_id, 
                CASE 
                    WHEN Side = 'B' THEN "Day Fill Amount" 
                    WHEN Side = 'BS' THEN "Day Fill Amount"
                    WHEN Side = 'S' THEN -1 * "Day Fill Amount"
                    WHEN Side = 'SS' THEN -1 * "Day Fill Amount"
                    ELSE "Day Fill Amount" 
                END AS quantity,                
                "Day Avg Price", 
                'INTRADAY'
            FROM
                tmp_trades t
            LEFT JOIN
                portfolios p
            ON
                t."Tran Account" = p.portfolio_name
            LEFT JOIN
                securities s
            ON
                t.ID_BB_GLOBAL = s.figi
            WHERE
                s.security_id IS NOT NULL
            """
            db.execute(qry)
            db.commit()
            