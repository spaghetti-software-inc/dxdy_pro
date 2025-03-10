# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

import duckdb
import argparse
from pathlib import Path


from ..settings import Settings

from ..ducklog import DuckDBLogger
logger = DuckDBLogger()

def get_duckdb_connection(db_path : Path):
    # Connect to the DuckDB database
    try:
        conn = duckdb.connect(database=db_path, read_only=False)
        logger.info(f"Connected to database at {db_path}")
        return conn
    except Exception as e:
        logger.error(f"Failed to connect to database at {db_path}: {e}")
        raise e
    
def create_database_views(db_path : Path):
    """
    Creates the DuckDB database views for Project dxdy v1.0
    """
    
    # Connect to the DuckDB database
    conn = get_duckdb_connection(db_path)
    
    # Begin a transaction
    cursor = conn.cursor()
    cursor.execute('BEGIN TRANSACTION;')
    
    try:
        # calendar view
        sql = """
        CREATE OR REPLACE VIEW calendar AS (
            SELECT
                DISTINCT date_trunc('d', market_close) AS cob_date
            FROM
                calendar_data
            ORDER BY
                cob_date
        );
        """
        cursor.execute(sql)
        logger.info("Created calendar view.")

        # day-over-day calendar view
        sql = """
        CREATE OR REPLACE VIEW calendar_dod_view AS (
            SELECT *
            FROM
                (SELECT 
                    cob_date,
                    LAG(cob_date) OVER (ORDER BY cob_date) AS prev_cob_date
                FROM calendar) lag_view
            ORDER BY
                cob_date
        );
        """
        cursor.execute(sql)
        logger.info("Created calendar_dod_view view.")
        
        
        # end-of-month calendar view
        sql = """
        CREATE OR REPLACE VIEW calendar_eom_view AS (
            SELECT
                MAX(cob_date) AS cob_date
            FROM
                (SELECT
                    cob_date,
                    year(cob_date) AS date_yr,
                    month(cob_date) AS date_mth
                FROM
                    calendar
                WHERE
                    cob_date < (SELECT MAX(cob_date) FROM calendar)) c
            GROUP BY
                date_yr,
                date_mth
        );
        """
        cursor.execute(sql)
        logger.info("Created calendar_eom_view view.")


        # monthly granularity calendar view
        sql = """
        CREATE OR REPLACE VIEW calendar_months AS (
            SELECT
                MIN(cob_date) AS som_cob_date,
                MAX(cob_date) AS eom_cob_date
            FROM
                (SELECT
                    cob_date,
                    year(cob_date) AS date_yr,
                    month(cob_date) AS date_mth
                FROM
                    calendar) c
            GROUP BY
                date_yr,
                date_mth
        );
        """
        cursor.execute(sql)
        logger.info("Created calendar_months view.")
        
        # yearly granularity calendar view
        sql = """
        CREATE OR REPLACE VIEW calendar_years AS (
        SELECT
            MAX(cob_date) AS cob_date
        FROM
            (
            SELECT
                cob_date,
                year(cob_date) AS date_yr
            FROM
                calendar
            WHERE
                cob_date < (
                SELECT
                    MAX(cob_date)
                FROM
                    calendar)) c
        GROUP BY
            date_yr
        );
        """
        cursor.execute(sql)
        logger.info("Created calendar_months view.")        
        
        # fx rates views
        sql = """
        CREATE OR REPLACE VIEW fx_rates AS (
            SELECT
                cal.cob_date,
                cur.ccy,
                LAST_VALUE(fx.fx_rate IGNORE NULLS) OVER (
                    PARTITION BY cur.ccy ORDER BY cal.cob_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS fx_rate
            FROM
                calendar cal
            CROSS JOIN
                currencies cur
            LEFT JOIN
                fx_rates_data fx
            ON
                fx.ccy = cur.ccy AND fx.fx_date = cal.cob_date
        );
        """
        cursor.execute(sql)
        logger.info("Created fx_rates view.")

        #  traded securities view
        sql = """
        CREATE OR REPLACE VIEW traded_securities AS (
            SELECT DISTINCT 
                trades.portfolio_id,
                trades.security_id,
                securities.*
            FROM 
                trades
            LEFT JOIN
                securities
            ON
                trades.security_id = securities.security_id
        );
        """
        cursor.execute(sql)
        logger.info("Created traded_securities view.")
        
        # split adjusted market data view
        sql = """
        CREATE OR REPLACE VIEW market_data_view AS
        WITH adjustment_factors AS (
            SELECT 
                m.security_id, 
                m.trade_date,
                COALESCE(
                    EXP(SUM(LN(s.split_from::DOUBLE PRECISION / s.split_to::DOUBLE PRECISION))), 
                    1
                ) AS adj_factor
            FROM 
                market_data m
            --INNER JOIN
            --    (SELECT DISTINCT security_id FROM traded_securities) ts
            --ON 
            --    m.security_id = ts.security_id
            LEFT JOIN 
                stock_splits s 
            ON 
                s.security_id = m.security_id AND s.split_date > m.trade_date
            GROUP BY 
                m.security_id, 
                m.trade_date
        )
        SELECT 
            m.market_data_id,
            m.security_id,
            m.trade_date,
            m.close_price * af.adj_factor AS close_price,
            m.volume * af.adj_factor AS volume
        FROM 
            market_data m
        --INNER JOIN
        --    (SELECT DISTINCT security_id FROM traded_securities) ts
        --ON 
        --    m.security_id = ts.security_id
        JOIN 
            adjustment_factors af 
        ON 
            m.security_id = af.security_id AND m.trade_date = af.trade_date;
        """
        cursor.execute(sql)
        logger.info("Created market_data_view view.")


        # split adjusted trades view
        sql = """
        CREATE OR REPLACE VIEW adj_trades AS (
        SELECT
            t.trade_id,
            t.portfolio_id,
            s.security_id,
            t.trade_date,
            t.quantity AS unadj_quantity,
            t.price AS unadj_price,
            t.commission AS unadj_commission,
            COALESCE(adj.adjustment_factor, 1) AS adjustment_factor,
            COALESCE(o.shares_per_contract, 1) AS multiplier,
            t.quantity * COALESCE(adj.adjustment_factor, 1) AS quantity,
            t.price / COALESCE(adj.adjustment_factor, 1) AS price,
            t.commission / COALESCE(adj.adjustment_factor, 1) AS commission,
            s.ccy AS quote_ccy,
            p.portfolio_ccy
        FROM
            trades t
        JOIN
            portfolios p ON t.portfolio_id = p.portfolio_id
        JOIN
            securities s ON t.security_id = s.security_id
        LEFT JOIN
            options o ON s.security_id = o.security_id
        LEFT JOIN (
            SELECT
                t.trade_id,
                EXP(SUM(LN(sp.split_to * 1.0 / sp.split_from))) AS adjustment_factor
            FROM
                trades t
            JOIN
                securities s ON t.security_id = s.security_id
            JOIN
                stock_splits sp ON s.security_id = sp.security_id AND sp.split_date > t.trade_date
            GROUP BY
                t.trade_id
        ) adj ON t.trade_id = adj.trade_id
        ORDER BY 
            t.trade_date,
            t.security_id
        );
        """
        cursor.execute(sql)
        logger.info("Created adj_trades view.")







        # Trades view
        sql = """
        CREATE OR REPLACE VIEW trades_view AS (
        SELECT
            t.trade_id,
            t.trade_date,

            t.portfolio_id,
            s.security_id,

            s.base_ticker,
            s.exch_code,
            s.security_type_2,
            
            t.unadj_quantity,
            t.unadj_price,
            t.unadj_commission,

            t.quantity,
            t.price,
            t.commission,
        FROM
            adj_trades t
        JOIN
            securities s
        ON
            t.security_id = s.security_id
        ORDER BY
            t.trade_date,
            t.security_id
        );
        """
        cursor.execute(sql)
        logger.info("Created trades_view view.")
    

        

        # Market data view
        sql = """
        CREATE OR REPLACE VIEW market_daily_returns AS (
        WITH daily_returns AS (
        SELECT
                md.security_id,
                trade_date,
                close_price,
                volume,
                
                LAG(close_price) OVER (
                    PARTITION BY md.security_id
                    ORDER BY trade_date
                ) AS previous_close_price,
                
                LAG(volume) OVER (
                    PARTITION BY md.security_id
                    ORDER BY trade_date
                ) AS previous_volume
            FROM
                market_data_view md
        )
        SELECT
            daily_returns.security_id,
            securities.base_ticker,
            securities.name,
            trade_date,
            close_price,
            previous_close_price,
            volume,
            previous_volume,
            CASE
                WHEN previous_close_price IS NULL THEN NULL
                ELSE (close_price - previous_close_price) / previous_close_price
            END AS daily_return,
            CASE
                WHEN previous_volume IS NULL THEN NULL
                ELSE (volume - previous_volume) / previous_volume
            END AS daily_volume_change_pct
        FROM
            daily_returns
        JOIN
            securities
        ON
            daily_returns.security_id = securities.security_id
            );
        """
        cursor.execute(sql)
        logger.info("Created market_daily_returns view.")



        # analytics
        
        # Sector allocations view
        sql = """
        CREATE OR REPLACE VIEW sector_allocations AS (
            SELECT
                portfolio_id,
                cob_date,
                sm.sector_id,
                sector_name,
                SUM(net_quantity * multiplier * close_price * cob_fx_rate) AS mkt_value_portfolio_ccy
            FROM
                daily_positions psn
            LEFT JOIN
                securities sec
            ON
                sec.security_id = psn.security_id
            LEFT JOIN
                sector_mappings sm
            ON
                sec.security_id = sm.security_id
            LEFT JOIN
                sectors s
            ON
                sm.sector_id = s.sector_id
            WHERE
                sec.security_type_2 != 'Option'	
            GROUP BY
                portfolio_id,
                cob_date,
                sector_name,
                sm.sector_id
            ORDER BY
                cob_date,
                portfolio_id,
                sector_name
        );
        """
        cursor.execute(sql)
        logger.info("Created sector_allocations view.")
        

        # Currency allocations view
        sql = """
        CREATE OR REPLACE VIEW fx_allocations AS (
            SELECT
                portfolio_id,
                cob_date,
                sec.ccy AS security_ccy,
                SUM(net_quantity * multiplier * close_price * cob_fx_rate) AS mkt_value_portfolio_ccy
            FROM
                daily_positions psn
            LEFT JOIN
                securities sec
            ON
                sec.security_id = psn.security_id
            GROUP BY
                portfolio_id,
                cob_date,
                sec.ccy,
            ORDER BY
                cob_date,
                portfolio_id,
                sec.ccy
        );
        """
        cursor.execute(sql)
        logger.info("Created fx_allocations view.")
        
        # Currency allocations view
        sql = """
        CREATE OR REPLACE VIEW strategy_allocations AS (
            SELECT
                psn.cob_date,
                psn.portfolio_id,
                s.security_type_2,
                CASE WHEN net_quantity >= 0 THEN 'Long' ELSE 'Short' END AS position_type,
                SUM(net_quantity * multiplier * close_price * cob_fx_rate) AS mkt_value_portfolio_ccy
            FROM
                daily_positions psn
            LEFT JOIN
                securities s
            ON
                psn.security_id = s.security_id
            GROUP BY
                psn.cob_date,
                psn.portfolio_id,
                s.security_type_2,
                position_type
        );
        """
        cursor.execute(sql)
        logger.info("Created strategy_allocations view.")        
        
        
        # 2-year historical VaR view (TODO: organize by portfolio_id, parameterize horizon)
        # sql = """
        # CREATE OR REPLACE VIEW hsim_var_view AS (
        #     SELECT
        #         m.trade_date AS hsim_date,
        #         SUM(quantity * close_price * daily_return * sqrt(252)) AS hsim_var_1yr
        #     FROM
        #         market_daily_returns m 
        #     LEFT JOIN
        #         positions('2024-11-06') p
        #     ON
        #         m.security_id = p.security_id
        #     WHERE
        #         trade_date > '2022-11-09'
        #     GROUP BY
        #         m.trade_date
        #     ORDER BY
        #         hsim_var_1yr
        # );
        # """
        # cursor.execute(sql)
        # logger.info("Created hsim_var_view view.")
        



        # macros

        # Positions table macro
        # sql = """
        # CREATE OR REPLACE MACRO positions(asof_date) AS TABLE
        # SELECT
        #     portfolio_id,
        #     securities.security_id,
        #     figi,
        #     securities.base_ticker,
        #     securities.exch_code,
        #     securities.security_type_2,
        #     quantity,
        #     --multiplier, 
        # FROM
        #     security_level_pnl 
        # LEFT JOIN
        #     securities
        # ON
        #     securities.security_id = security_level_pnl.security_id
        # WHERE
        #     cob_date = asof_date
        # """
        # cursor.execute(sql)
        # logger.info("Created positions macro.")

        # adjusted cash movements
        sql = """
        CREATE OR REPLACE VIEW adj_cash_transactions AS
        SELECT
            c.cash_movement_id,
            c.portfolio_id,
            c.cash_date AS cob_date,
            c.cash_amount,
            c.ccy,
            CASE c.cash_type WHEN 1 THEN 'AUM' WHEN 2 THEN 'Dividend' WHEN 3 THEN 'Expense' ELSE 'Other' END AS cash_type,
            cash_amount * (f1.fx_rate / f2.fx_rate) AS cash_amount_portfolio_ccy
        FROM
            cash_transactions c
        LEFT JOIN
            portfolios p
        ON
            c.portfolio_id = p.portfolio_id
        LEFT JOIN 
            fx_rates f1  -- fx rate for the movement_ccy
        ON 
            f1.cob_date = c.cash_date
        AND 
            f1.ccy      = p.portfolio_ccy
        LEFT JOIN 
            fx_rates f2  -- fx rate for the portfolio_ccy
        ON 
            f2.cob_date = c.cash_date
        AND 
            f2.ccy      = p.portfolio_ccy
        ORDER BY
            c.cash_date, c.portfolio_id
        ;
        """
        cursor.execute(sql)
        logger.info("Created adj_cash_transactions view.")
        
        
        # cash running balance view
        sql = """
        CREATE OR REPLACE VIEW cash_balance_view AS
        WITH all_dates AS (
            -- Get every date from the calendar for every portfolio
            SELECT
                cal.cob_date,
                p.portfolio_id
            FROM
                calendar cal
            CROSS JOIN
                portfolios p
        ),
        daily_cash AS (
            -- Aggregate daily cash flow for each portfolio on each date
            SELECT
                ad.portfolio_id,
                ad.cob_date,
                COALESCE(SUM(act.cash_amount_portfolio_ccy), 0) AS daily_net_cash_flow
            FROM
                all_dates ad
            LEFT JOIN
                adj_cash_transactions act 
                ON ad.portfolio_id = act.portfolio_id 
                AND ad.cob_date = act.cob_date
            GROUP BY
                ad.portfolio_id, 
                ad.cob_date
        )
        -- Calculate the running cash balance per portfolio
        SELECT
            portfolio_id,
            cob_date,
            SUM(daily_net_cash_flow) OVER (
                PARTITION BY portfolio_id
                ORDER BY cob_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS cash_balance_portfolio_ccy
        FROM daily_cash
        ORDER BY portfolio_id, cob_date;

        """
        cursor.execute(sql)
        logger.info("Created cash_balance_view view.")
        

        # latest cash balance view
        sql = """
        CREATE OR REPLACE VIEW latest_cash_balance_view AS
        WITH ranked AS (
            SELECT 
                portfolio_id, 
                cob_date, 
                cash_balance_portfolio_ccy,
                ROW_NUMBER() OVER (PARTITION BY portfolio_id ORDER BY cob_date DESC) AS rn
            FROM 
                cash_balance_view
        )
        SELECT 
            portfolio_id, 
            cash_balance_portfolio_ccy AS latest_cash_balance
        FROM ranked
        WHERE rn = 1;
        """
        cursor.execute(sql) 
        logger.info("Created latest_cash_balance_view view.")
        

        # cash balance as of
        qry = f"""
        CREATE OR REPLACE MACRO cash_balance_as_of(asof_date) AS TABLE
        WITH latest_dates AS (
            SELECT 
                portfolio_id, 
                MAX(cob_date) AS max_date
            FROM 
                cash_balance_view
            WHERE 
                cob_date <= asof_date  -- asof_date is the input parameter
            GROUP BY 
                portfolio_id
        )
        SELECT 
            cbv.portfolio_id,
            cbv.cash_balance_portfolio_ccy AS latest_cash_balance,
            cbv.cob_date AS latest_cash_balance_date
        FROM 
            cash_balance_view cbv
        JOIN latest_dates ld 
            ON cbv.portfolio_id = ld.portfolio_id 
            AND cbv.cob_date = ld.max_date;
        """
        cursor.execute(qry)
        logger.info("Created cash_balance_as_of macro.")


        # Portfolio-level P&L detail view
        sql = """
        CREATE OR REPLACE VIEW portfolio_level_pnl AS (
            SELECT
                pnl.portfolio_id,
                pnl.cob_date,
                pnl.total_dod_pnl_portfolio_ccy,
                SUM(pnl.total_dod_pnl_portfolio_ccy) OVER (
                        PARTITION BY pnl.portfolio_id
                        ORDER BY pnl.cob_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS total_pnl_portfolio_ccy,
                    total_dod_pnl_portfolio_ccy/cbv.cash_balance_portfolio_ccy AS pct_aum
            FROM
                (SELECT
                    portfolio_id,
                    cob_date,
                    SUM(total_dod_pnl_portfolio_ccy) AS total_dod_pnl_portfolio_ccy
                FROM 
                    daily_positions
                GROUP BY
                    portfolio_id,
                    cob_date) AS pnl
            LEFT JOIN
                cash_balance_view cbv
            ON
                pnl.portfolio_id = cbv.portfolio_id
                AND pnl.cob_date = cbv.cob_date
            ORDER BY 
                pnl.portfolio_id, 
                pnl.cob_date
        );
        """
        cursor.execute(sql)
        logger.info("Created portfolio_level_pnl view.")


        # Security-level P&L detail view
        sql = """
        CREATE OR REPLACE VIEW security_level_pnl AS (
            SELECT
                portfolio_id,
                security_id,
                cob_date,
                net_quantity AS quantity,
                total_dod_pnl_portfolio_ccy,
                SUM(pnl.total_dod_pnl_portfolio_ccy) OVER (
                        PARTITION BY portfolio_id, security_id
                        ORDER BY cob_date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS total_pnl_portfolio_ccy
            FROM
                daily_positions pnl
            ORDER BY 
                portfolio_id, 
                security_id,
                cob_date
        );
        """
        cursor.execute(sql)
        logger.info("Created security_level_pnl view.")

        # Commit the transaction
        cursor.execute('COMMIT;')
        logger.info("Database schema created and committed successfully.")
        
    except Exception as e:
        # Rollback the transaction in case of error
        cursor.execute('ROLLBACK;')
        logger.error("An error occurred while creating the database schema:")
        logger.error(e)
        
    finally:
        # Close the connection
        conn.close()
        logger.info("Database connection closed.")

def build_db_views():
    settings = Settings()
    db_file = settings._get_db_file()
    create_database_views(db_file)
