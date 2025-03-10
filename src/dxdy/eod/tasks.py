# Copyright (C) 2024-2025 Spaghetti Software Inc. (SPGI)

import shutil
from datetime import date

import pandas as pd

import rich
from loguru import logger



from dxdy.db.market_data import MarketDataApi, MarketDataApiFactory
from dxdy.email.reports import send_eod_risk_report
from dxdy.settings import Settings
from dxdy.saas_settings import SaaSConfig
import dxdy.db.utils as db_utils
import dxdy.dx_edgar as ed

import dxdy.quant.ai as ai

API_SELECTION = "bbg"
#API_SELECTION = "spgi"
#API_SELECTION = "yahoo"


API : MarketDataApi = MarketDataApiFactory().get_api(API_SELECTION)
CID = API.securities_identifier()

def task_update_calendar_data(db, end_date) -> None:
    db_utils.insert_calendar_data(db, end_date)


def task_load_transactions_data(db, cob_date : date) -> None:
    try:
        API.load_trade_blotter_api(db, cob_date)
        
        logger.debug(f"Loaded trade blotter for {cob_date}")
        
    except FileNotFoundError as e:
        logger.debug(f"{e}")
    
    


def task_load_market_data(db, start_date : date, cob_date : date, tplus_one : date) -> None:
    with Settings().get_db_connection(readonly=False) as db:
        
        
        qry = f"""
        SELECT
            {CID}
        FROM
            securities
        """
        securities_df = db.execute(qry).fetch_df()
        if securities_df.shape[0] == 0:
            return
                
        try:
            API.timeseries_market_data_api(db, securities_df[CID].unique(), start_date, cob_date, tplus_one)
            logger.debug(f"Loaded market data for {cob_date}")
        except Exception as e:
            logger.debug(f"Error loading market data for {cob_date}: {e}")
            raise e
        

def task_div_splits_data(db, start_date : date, cob_date : date, tplus_one : date):
    with Settings().get_db_connection(readonly=False) as db:
        
        qry = f"""
        SELECT
            {CID}
        FROM
            securities
        """
        securities_df = db.execute(qry).fetch_df()
        if securities_df.shape[0] == 0:
            return

        try:
            API.timeseries_div_splits_data_api(db, securities_df[CID].unique(), start_date, cob_date, tplus_one)
            logger.debug(f"Loaded divs/splits data for {cob_date}")
            
        except Exception as e:
            logger.debug(f"Error loading div/splits data for {cob_date}: {e}")
            raise e


def task_load_fx_rates_data(db, start_date : date, cob_date : date, tplus_one : date):
    with Settings().get_db_connection(readonly=False) as db:
        
        try:
            API.timeseries_fx_rates_data_api(db, start_date, cob_date, tplus_one)
            logger.debug(f"Loaded FX data for {cob_date}")
            
        except Exception as e:
            logger.debug(f"Error loading FX data for {cob_date}: {e}")
            raise e


def compute_positions_asof_date(db, asof_date, prev_asof_date):
    """
    Computes the final EOD snapshot of positions for each (portfolio_id, security_id)
    as of the given 'asof_date'. 
    Returns a DataFrame with one row per portfolio/security final position state:
        portfolio_id, security_id, quantity, avg_cost, realized_pnl_to_date
    """

    logger.info(f"Computing EOD positions for {asof_date}")

    if pd.isna(prev_asof_date):
        prev_asof_date = asof_date

    # ----------------------------------------------------------------------
    # Step 1: Pull trades UP TO asof_date
    # ----------------------------------------------------------------------
    query = f"""
        WITH psn AS (
            SELECT
                '{asof_date}' AS cob_date,
                '{prev_asof_date}' AS prev_cob_date,
                net_qty.*
            FROM (
                SELECT
                    portfolio_id,
                    security_id,
                    SUM(quantity) AS net_quantity
                FROM 
                    adj_trades
                WHERE 
                    trade_date <= '{asof_date}'
                GROUP BY
                    portfolio_id,
                    security_id
            ) net_qty
        ),

        -- --------------------------------------------
        -- CTE to get last non-null close_price ON OR BEFORE {asof_date}
        -- --------------------------------------------
        md_cob AS (
            SELECT
                security_id,
                last_value(close_price IGNORE NULLS)
                    OVER (
                        PARTITION BY security_id
                        ORDER BY trade_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS close_price
            FROM market_data_view
            WHERE trade_date <= '{asof_date}'
            -- QUALIFY: pick the single most recent row for each security
            QUALIFY ROW_NUMBER()
                OVER (
                    PARTITION BY security_id
                    ORDER BY trade_date DESC
                ) = 1
        ),

        -- --------------------------------------------
        -- CTE to get last non-null close_price ON OR BEFORE {prev_asof_date}
        -- --------------------------------------------
        md_prev AS (
            SELECT
                security_id,
                last_value(close_price IGNORE NULLS)
                    OVER (
                        PARTITION BY security_id
                        ORDER BY trade_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS close_price
            FROM market_data_view
            WHERE trade_date <= '{prev_asof_date}'
            QUALIFY ROW_NUMBER()
                OVER (
                    PARTITION BY security_id
                    ORDER BY trade_date DESC
                ) = 1
        ),

        fx_rates_cob AS (
            SELECT
                ccy,
                last_value(fx_rate IGNORE NULLS)
                    OVER (
                        PARTITION BY ccy
                        ORDER BY fx_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS fx_rate
            FROM fx_rates_data
            WHERE fx_date <= '{asof_date}'
            QUALIFY ROW_NUMBER()
                OVER (
                    PARTITION BY ccy
                    ORDER BY fx_date DESC
                ) = 1
        )

        SELECT
            psn.*,
            md_cob.close_price AS close_price,
            md_prev.close_price AS prev_close_price,
            fx_sec.fx_rate / fx_port.fx_rate AS cob_fx_rate,
            COALESCE(o.shares_per_contract, 1) AS multiplier,
            COALESCE(d.cash_amount, 0) * net_quantity AS dividend_amount_local_ccy,
            COALESCE(
                net_quantity * (md_cob.close_price - md_prev.close_price) * multiplier,
                0
            ) AS unrealized_dod_pnl_local_ccy
            -- plus the rest of your columns/expressions ...
        FROM psn
        LEFT JOIN portfolios p
            ON p.portfolio_id = psn.portfolio_id
        LEFT JOIN securities s
            ON s.security_id = psn.security_id
        LEFT JOIN options o
            ON o.security_id = psn.security_id

        -- JOIN the last-known close prices:
        LEFT JOIN md_cob
            ON md_cob.security_id = psn.security_id
        LEFT JOIN md_prev
            ON md_prev.security_id = psn.security_id

        LEFT JOIN dividends d
            ON d.security_id = psn.security_id
        AND d.ex_dividend_date = '{asof_date}'

        LEFT JOIN fx_rates_cob fx_sec
            ON fx_sec.ccy = s.ccy

        LEFT JOIN fx_rates_cob fx_port
            ON fx_port.ccy = p.portfolio_ccy
        ;
    """
    df_positions_asof = db.execute(query).fetch_df()

    # ------------------------------------------------------------------------
    # Step 2: Define a helper to compute average cost basis for each position
    # ------------------------------------------------------------------------
    def compute_final_position_for_group(df, pid, sid):
        # get all trades for this group
        qry = f"""
        SELECT
            *
        FROM
            adj_trades
        LEFT JOIN
            market_data_view md
        ON
            md.security_id = {sid} AND md.trade_date = '{asof_date}'
        WHERE
            adj_trades.portfolio_id = {pid} AND adj_trades.security_id = {sid}
        AND
            adj_trades.trade_date <= '{asof_date}'
        ORDER BY
            adj_trades.trade_date
        """
        trades_df = db.execute(qry).fetch_df()
        
        current_qty = 0.0
        current_avg_cost = 0.0
        realized_pnl = 0.0
        intraday_pnl_local_ccy = 0.0

        for _, row in trades_df.iterrows():
            if row['trade_date'] == asof_date:
                intraday_pnl_local_ccy += row['multiplier'] * row['quantity'] * (row['close_price'] - row['price'])

            qty_change = row['quantity']
            trade_price = row['price']
            close_price = row['close_price']
            commission = row['commission']

            old_qty = current_qty
            old_cost = current_avg_cost
            new_qty = old_qty + qty_change

            # Check if crossing zero
            if old_qty * new_qty < 0:
                # crossing from long to short or short to long in one trade
                closed_qty = -old_qty  # fully close old_qty
                # Realized portion
                realized_pnl += closed_qty * (trade_price - old_cost)
                # Subtract commission from realized PnL
                realized_pnl -= commission

                # Remainder = new_qty after fully closing old
                remainder = qty_change + old_qty  # e.g. -5 if we had 10 and sold 15
                current_qty = remainder
                # new position cost basis
                current_avg_cost = trade_price if remainder != 0 else 0.0

            else:
                # same side (increasing or decreasing but not crossing zero)
                if old_qty == 0:
                    # opening from zero
                    current_qty = new_qty
                    current_avg_cost = trade_price
                    # Optionally capitalize commission into avg cost
                    # (common if it's an "opening trade")
                    if current_qty != 0:
                        current_avg_cost = ((current_avg_cost * abs(current_qty)) + commission) / abs(current_qty)

                elif (old_qty * new_qty) > 0:
                    # partial close or add
                    if abs(new_qty) > abs(old_qty):
                        # net add to position => recalc weighted avg cost
                        total_old_cost = old_qty * old_cost
                        total_new_cost = qty_change * trade_price
                        # If you prefer to add commission to new cost:
                        total_new_cost += commission
                        current_avg_cost = (total_old_cost + total_new_cost) / new_qty
                        current_qty = new_qty
                    else:
                        # partial close (realize PnL on the closed portion)
                        closed_qty = old_qty - new_qty  # e.g. close 4 if old=10,new=6
                        realized_pnl += closed_qty * (trade_price - old_cost)
                        realized_pnl -= commission  # subtract commission from realized
                        current_qty = new_qty
                        # cost basis stays the same if partial close
                else:
                    # new_qty == 0 => fully closed
                    closed_qty = old_qty
                    realized_pnl += closed_qty * (trade_price - old_cost)
                    realized_pnl -= commission
                    current_qty = 0.0
                    current_avg_cost = 0.0

        return {
            'computed_net_quantity': current_qty,
            'avg_cost': current_avg_cost,
            'intraday_pnl_local_ccy': intraday_pnl_local_ccy,
            # 'realized_pnl_to_date': realized_pnl
        }

    results = []
    grouped = df_positions_asof.groupby(['portfolio_id', 'security_id'], group_keys=True)
    for (pid, sid), group_df in grouped:
        pos_dict = compute_final_position_for_group(group_df, pid, sid)

        
        pos_dict['portfolio_id'] = pid
        pos_dict['security_id'] = sid

        # print(group_df[['portfolio_id', 'security_id','net_quantity']])
        # print(pos_dict)

        results.append(pos_dict)

    # Convert results to DataFrame
    df_avg_costs = pd.DataFrame(results)

    # Merge with df_positions_asof
    df_positions_asof = df_positions_asof.merge(df_avg_costs, on=['portfolio_id', 'security_id'], how='left')
    df_positions_asof['total_dod_pnl_local_ccy'] = df_positions_asof['intraday_pnl_local_ccy'] + df_positions_asof['unrealized_dod_pnl_local_ccy'] + df_positions_asof['dividend_amount_local_ccy']
    df_positions_asof['total_dod_pnl_portfolio_ccy'] = df_positions_asof['total_dod_pnl_local_ccy'] * df_positions_asof['cob_fx_rate']



    return df_positions_asof

def task_compute_daily_positions(db, asof_date : date, prev_asof_date : date):
    df_positions_asof = compute_positions_asof_date(db, asof_date, prev_asof_date)
    
    # check that `computed_net_quantity` is the same as `net_quantity`
    #assert (df_positions_asof['computed_net_quantity'] == df_positions_asof['net_quantity']).all()

    with db_utils.DuckDBTemporaryTable(db, 'tmp_daily_positions', df_positions_asof) as temp_table:
        qry = f"""
        INSERT INTO 
            daily_positions (portfolio_id, security_id, cob_date, prev_cob_date, net_quantity, multiplier, avg_cost, close_price, prev_close_price, cob_fx_rate, intraday_pnl_local_ccy, unrealized_dod_pnl_local_ccy, dividend_amount_local_ccy, total_dod_pnl_local_ccy, total_dod_pnl_portfolio_ccy, created_by)
        SELECT
            portfolio_id, security_id, cob_date, prev_cob_date, net_quantity, multiplier, avg_cost, close_price, prev_close_price, cob_fx_rate, intraday_pnl_local_ccy, unrealized_dod_pnl_local_ccy, dividend_amount_local_ccy, total_dod_pnl_local_ccy, total_dod_pnl_portfolio_ccy, 'dxdy' AS created_by
        FROM 
            tmp_daily_positions
        """
        db.execute(qry)
        db.commit()
        

def task_load_intraday_transactions_data(cob_date : date, prev_cob_date : date) -> None:
    API.load_intraday_trade_blotter_api(cob_date)
    
    with Settings().get_db_connection(readonly=False) as db:
        qry = f"""
            DELETE FROM
                daily_positions
            WHERE
                cob_date = '{cob_date}'
            AND
                created_by = 'INTRADAY'
            """
        db.execute(qry)
        db.commit()
        logger.debug(f"Deleted intraday positions for {cob_date}")

        df_positions_asof = compute_positions_asof_date(db, cob_date, prev_cob_date)
        
        # check that `computed_net_quantity` is the same as `net_quantity`
        #assert (df_positions_asof['computed_net_quantity'] == df_positions_asof['net_quantity']).all()

        df_positions_asof.to_clipboard(index=False, header=True)

        with db_utils.DuckDBTemporaryTable(db, 'tmp_daily_positions', df_positions_asof) as temp_table:
            qry = f"""
            INSERT INTO 
                daily_positions (portfolio_id, security_id, cob_date, prev_cob_date, net_quantity, multiplier, avg_cost, close_price, prev_close_price, cob_fx_rate, intraday_pnl_local_ccy, unrealized_dod_pnl_local_ccy, dividend_amount_local_ccy, total_dod_pnl_local_ccy, total_dod_pnl_portfolio_ccy, created_by)
            SELECT
                portfolio_id, security_id, cob_date, prev_cob_date, net_quantity, multiplier, avg_cost, close_price, prev_close_price, cob_fx_rate, intraday_pnl_local_ccy, unrealized_dod_pnl_local_ccy, dividend_amount_local_ccy, total_dod_pnl_local_ccy, total_dod_pnl_portfolio_ccy, 'INTRADAY' AS created_by
            FROM 
                tmp_daily_positions
            """
            db.execute(qry)
            db.commit()  
            logger.debug(f"Inserted intraday positions for {cob_date}")

        
def task_send_eod_risk_report(db, cob_date : date): 
    send_eod_risk_report(db, cob_date)
    logger.debug(f"Sent EOD risk report for {cob_date}")
    
def task_backup_database(cob_date : date):
    db_file = Settings()._get_db_file()
    backup_file = Settings().get_db_backup_file(cob_date)
    shutil.copyfile( db_file, backup_file )

    logger.info(f"Backed up database to {backup_file}")







def task_ai_pnl_analysis(db, cob_date : date):
    qry = f"""
    SELECT
        portfolio_id
    FROM
        portfolios
    ORDER BY
        portfolio_id
    """
    portfolios_df = db.execute(qry).fetch_df()
    portfolio_ids = portfolios_df['portfolio_id'].to_list()
    
    for portfolio_id in portfolio_ids:
        res = ai.get_daily_pnl_commentary(db, cob_date, portfolio_id)
        res_str = res.model_dump_json()
        
        rich.print(res)
        
        qry = f"""
        INSERT INTO
            ai_analysis (cob_date, portfolio_id, agent, analysis)
        VALUES
            (?, ?, ?, ?)
        """
        db.execute(qry, (cob_date, portfolio_id, "agent_pnl_summary", res_str))
        db.commit()
        
def task_ai_market_commentary(db, cob_date : date):
    res = ai.get_daily_market_commentary(db, cob_date)
    res_str = res.model_dump_json()
    
    rich.print(res)
    
    qry = f"""
    INSERT INTO
        ai_analysis (cob_date, agent, analysis)
    VALUES
        (?, ?, ?)
    """
    db.execute(qry, (cob_date, "agent_market_summary", res_str))
    db.commit()

def task_ai_technical_analysis(db, cob_date : date):
    qry = f"""
    SELECT
        s.*
    FROM
        securities s
    WHERE
        s.security_id IN (SELECT security_id FROM positions('{cob_date}'))        
    AND
        s.security_type_2 = 'Common Stock'
    ORDER BY
        s.ticker
    """
    securities_df = db.execute(qry).fetch_df()
    
    for index, security in securities_df.iterrows():
        res = ai.get_technical_analysis(db, security)
        res_str = res.model_dump_json()
        
        rich.print(res)
        
        qry = f"""
        INSERT INTO
            ai_analysis (cob_date, security_id, agent, analysis)
        VALUES
            (?, ?, ?, ?)
        """
        db.execute(qry, (cob_date, security.security_id, "agent_technical_analyst", res_str))
        db.commit()
        
def task_ai_earnings_analysis(db, cob_date : date):
    latest_filings = ed.get_latest_filings(db, cob_date)
    
    for filing in latest_filings:
        res = ai.get_earnings_analysis(db, filing)
        res_str = res.model_dump_json()
        
        rich.print(res)
        
        qry = f"""
        INSERT INTO
            ai_analysis (cob_date, security_id, agent, analysis)
        VALUES
            (?, ?, ?, ?)
        """
        db.execute(qry, (filing['latest_filing_date'], filing['security_id'], "agent_earnings_analyst", res_str))
        db.commit()