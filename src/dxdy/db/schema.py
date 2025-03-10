# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)


import duckdb
import pandas as pd

import argparse
from pathlib import Path


from ..settings import Settings

from ..ducklog import DuckDBLogger
logger = DuckDBLogger()

def load_csv_file(db_path : Path, csv_file : Path, table_name : str):
    db_conn = duckdb.connect(database=db_path, read_only=False)
    cursor = db_conn.cursor()
    cursor.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_csv_auto('{csv_file}');")
    cursor.commit()
    db_conn.close()

def create_database_schema(db_path : Path):
    """
    Creates the DuckDB database schema for Project dxdy v1.0 Phase 1.
    """
    
    # Connect to the DuckDB database
    db_conn = duckdb.connect(database=db_path, read_only=False)
    logger.info(f"Connected to database at {db_path}")
    
    # Begin a transaction
    cursor = db_conn.cursor()
    cursor.execute('BEGIN TRANSACTION;')
    
    try:
        # Create sequences
        sequences_sql = """
        CREATE SEQUENCE IF NOT EXISTS seq_sector_id START 1;
        

        CREATE SEQUENCE IF NOT EXISTS seq_security_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_portfolio_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_trade_id START 1;

        CREATE SEQUENCE IF NOT EXISTS seq_currency_id START 1;        
        CREATE SEQUENCE IF NOT EXISTS seq_cash_movement_id START 1;

        CREATE SEQUENCE IF NOT EXISTS seq_stock_split_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_dividend_id START 1;

        CREATE SEQUENCE IF NOT EXISTS seq_option_id START 1;

        CREATE SEQUENCE IF NOT EXISTS seq_option_trade_id START 1;

        CREATE SEQUENCE IF NOT EXISTS seq_fx_rate_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_market_data_id START 1;
        

        CREATE SEQUENCE IF NOT EXISTS seq_corporate_action_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_equity_position_id START 1;
        CREATE SEQUENCE IF NOT EXISTS seq_option_position_id START 1;

        CREATE SEQUENCE IF NOT EXISTS seq_daily_position_id START 1;
        """
        cursor.execute(sequences_sql)
        logger.info("Sequences created successfully.")
        

        # Create 'cob_dates' table
        calendar_data_table_sql = """
        CREATE TABLE IF NOT EXISTS calendar_data (
            exchange TEXT NOT NULL,
            market_open TIMESTAMP NOT NULL,
            market_close TIMESTAMP NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (exchange, market_open)  -- Ensures one record per exchange and market_open
        );
        """
        cursor.execute(calendar_data_table_sql)
        logger.info("Table 'calendar_data' created successfully.")



        # Create 'currencies' table
        currencies_table_sql = """
        CREATE TABLE IF NOT EXISTS currencies (
            currency_id INTEGER DEFAULT NEXTVAL('seq_currency_id'),
            ccy TEXT PRIMARY KEY NOT NULL,
            currency_name TEXT NOT NULL,
        );
        """
        cursor.execute(currencies_table_sql)
        logger.info("Table 'currencies' created successfully.")


        # Create 'securities' table
        # TODO: fix NOT NULL constraints
        securities_table_sql = """
        CREATE TABLE IF NOT EXISTS securities (
            security_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_security_id'),
            base_ticker TEXT NOT NULL,
            exch_code TEXT NOT NULL,
            security_type_2 TEXT NOT NULL,
            ticker TEXT NOT NULL,
            name TEXT,
            security_description TEXT,
            figi TEXT NOT NULL,
            isin TEXT,
            sedol TEXT,
            ccy TEXT NOT NULL REFERENCES currencies(ccy),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT
            --UNIQUE (figi)  -- Ensures one record per Financial Instrument Global Identifier (FIGI) [TODO: handle ticker symbol changes]
        );
        """
        cursor.execute(securities_table_sql)
        logger.info("Table 'securities' created successfully.")
    


        # Create 'sectors' table
        sectors_table_sql = """
        CREATE TABLE IF NOT EXISTS sectors (
            sector_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_sector_id'),
            sector_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (sector_name)  -- Ensures one record per sector_name 
        );
        """
        cursor.execute(sectors_table_sql)
        logger.info("Table 'sectors' created successfully.")

        # Create 'sector_mappings' table
        sector_mappings_table_sql = """
        CREATE TABLE IF NOT EXISTS sector_mappings (
            security_id INTEGER NOT NULL REFERENCES securities(security_id),
            sector_id INTEGER NOT NULL REFERENCES sectors(sector_id),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (security_id, sector_id)  -- Ensures one record per security_id and sector_id
        );
        """
        cursor.execute(sector_mappings_table_sql)
        logger.info("Table 'sector_mappings' created successfully.")
        

        # Create 'options' table
        options_table_sql = """
        CREATE TABLE IF NOT EXISTS options (
            security_id INTEGER PRIMARY KEY REFERENCES securities(security_id),
            underlying_security_id INTEGER NOT NULL REFERENCES securities(security_id),
            ticker TEXT NOT NULL,
            opra_symbol TEXT NOT NULL,
            figi TEXT NOT NULL,
            underlying_figi TEXT,
            contract_type TEXT NOT NULL CHECK (contract_type IN ('Call', 'Put')),
            exercise_style TEXT NOT NULL CHECK (exercise_style IN ('American', 'European')),
            exch_code TEXT NOT NULL,
            shares_per_contract INTEGER NOT NULL,
            strike_price DOUBLE NOT NULL,
            expiration_date DATE NOT NULL,
            ccy TEXT NOT NULL REFERENCES currencies(ccy),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT
            --UNIQUE (security_id, ticker, exch_code)  -- Ensures one record per security_id
        );
        """
        cursor.execute(options_table_sql)
        logger.info("Table 'options' created successfully.")



        # Create 'portfolios' table
        portfolios_table_sql = """
        CREATE TABLE IF NOT EXISTS portfolios (
            portfolio_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_portfolio_id'),
            portfolio_name TEXT NOT NULL,
            description TEXT,
            portfolio_ccy TEXT NOT NULL REFERENCES currencies(ccy),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (portfolio_name)  -- Ensures one record per portfolio_name
        );
        """
        cursor.execute(portfolios_table_sql)
        logger.info("Table 'portfolios' created successfully.")

        # Create 'trades' table
        trades_table_sql = """
        CREATE TABLE IF NOT EXISTS trades (
            trade_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_trade_id'),
            portfolio_id INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
            security_id INTEGER NOT NULL REFERENCES securities(security_id),
            trade_date DATE NOT NULL,
            settlement_date DATE,
            quantity INTEGER NOT NULL,
            price DOUBLE NOT NULL,
            commission DOUBLE DEFAULT 0.0,
            external_trade_id TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT
        );
        """
        cursor.execute(trades_table_sql)
        logger.info("Table 'trades' created successfully.")

        # Create 'fx_rates' table
        fx_rates_table_sql = """
        CREATE TABLE IF NOT EXISTS fx_rates_data (
            fx_rate_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_fx_rate_id'),
            fx_date DATE NOT NULL,
            ccy TEXT NOT NULL REFERENCES currencies(ccy),
            fx_rate DOUBLE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (fx_date, ccy)  -- Ensures one record per ccy and fx_date
        );
        """
        cursor.execute(fx_rates_table_sql)
        logger.info("Table 'fx_rates_data' created successfully.")


        # Create 'market_data' table
        market_data_table_sql = """
        CREATE TABLE IF NOT EXISTS market_data (
            market_data_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_market_data_id'),
            security_id INTEGER NOT NULL REFERENCES securities(security_id),
            trade_date DATE NOT NULL,
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE NOT NULL,
            volume INTEGER,
            UNIQUE (security_id, trade_date)  -- Ensures one record per security_id and trade_date
        );
        """
        cursor.execute(market_data_table_sql)
        logger.info("Table 'market_data' created successfully.")

        # Create the 'stock_splits' table
        stock_splits_table_sql = """
        CREATE TABLE IF NOT EXISTS stock_splits (
            stock_split_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_stock_split_id'),
            security_id INTEGER NOT NULL REFERENCES securities(security_id),
            split_date DATE NOT NULL,
            split_from INTEGER NOT NULL,
            split_to INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (security_id, split_date)  -- Ensures one record per security_id and split_date
        );
        """
        cursor.execute(stock_splits_table_sql)
        logger.info("Table 'stock_splits' created successfully.")


        # Create the 'dividends' table
        # cash_amount currency declaration_date dividend_type ex_dividend_date  frequency    pay_date record_date ticker

        dividends_table_sql = """
        CREATE TABLE IF NOT EXISTS dividends (
            dividend_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_dividend_id'),
            security_id INTEGER NOT NULL REFERENCES securities(security_id),
            ex_dividend_date DATE NOT NULL,
            record_date DATE,
            pay_date DATE,
            cash_amount DOUBLE NOT NULL,
            ccy TEXT NOT NULL REFERENCES currencies(ccy),
            dividend_type TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (security_id, ex_dividend_date)  -- Ensures one record per security_id and ex_dividend_date
        );
        """
        cursor.execute(dividends_table_sql)
        logger.info("Table 'dividends' created successfully.")

        # Create the 'cash_transactions' table
        cash_transactions_table_sql = """
        CREATE TABLE IF NOT EXISTS cash_transactions (
            cash_movement_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_cash_movement_id'),
            portfolio_id     INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
            cash_date        DATE    NOT NULL,        -- The effective date of the cash in/out
            cash_amount      DOUBLE  NOT NULL,        -- Positive = inflow/deposit, Negative = outflow/expense
            ccy              TEXT    NOT NULL REFERENCES currencies(ccy),
            cash_type        TEXT    NOT NULL, 
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at       TIMESTAMP,
            created_by       TEXT,
            updated_by       TEXT
        );
        """
        cursor.execute(cash_transactions_table_sql)
        logger.info("Table 'cash_transactions' created successfully.")

        # Create the 'daily_positions' table
        daily_positions_table_sql = """
        CREATE TABLE IF NOT EXISTS daily_positions (
            daily_position_id INTEGER PRIMARY KEY DEFAULT NEXTVAL('seq_daily_position_id'),
            portfolio_id INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
            security_id INTEGER NOT NULL REFERENCES securities(security_id),
            cob_date DATE NOT NULL,
            prev_cob_date DATE NOT NULL,
            net_quantity INTEGER NOT NULL,
            multiplier INTEGER NOT NULL,
            avg_cost DOUBLE NOT NULL,
            close_price DOUBLE,
            prev_close_price DOUBLE,
            cob_fx_rate DOUBLE,
            intraday_pnl_local_ccy DOUBLE NOT NULL,
            unrealized_dod_pnl_local_ccy DOUBLE NOT NULL,
            dividend_amount_local_ccy DOUBLE NOT NULL,
            total_dod_pnl_local_ccy DOUBLE NOT NULL,
            total_dod_pnl_portfolio_ccy DOUBLE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP,
            created_by TEXT,
            updated_by TEXT,
            UNIQUE (portfolio_id, security_id, cob_date)  -- Ensures one record per portfolio_id, security_id, and trade_date
        );
        """
        cursor.execute(daily_positions_table_sql)
        logger.info("Table 'daily_positions' created successfully.")

        ai_table_sql = """
        CREATE TABLE IF NOT EXISTS ai_analysis (
            cob_date DATE,
            portfolio_id INTEGER REFERENCES portfolios(portfolio_id),
            security_id INTEGER REFERENCES securities(security_id),
            agent TEXT,
            analysis TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(ai_table_sql)
        logger.info("Table 'ai_analysis' created successfully.")
        
        # Commit the transaction
        cursor.execute('COMMIT;')
        logger.info("Database schema created and committed successfully.")
        
    except Exception as e:
        # Rollback the transaction in case of error
        cursor.execute('ROLLBACK;')
        logger.error("An error occurred while creating the database schema:")
        logger.error(e)
        raise e
        
    finally:
        # Close the connection
        db_conn.close()
        logger.info("Database connection closed.")

def build_db():
    settings = Settings()
    db_file = settings._get_db_file()
    create_database_schema(db_file)


def get_database_schema() -> pd.DataFrame:
    settings = Settings()
    db_conn = settings.get_db_connection()
    qry = "SELECT * FROM duckdb_columns() WHERE database_name = 'dxdy' AND schema_name = 'main' AND internal = false"
    res = db_conn.execute(qry).fetch_df()
    return res


# if __name__ == "__main__":
#     settings = Settings()
#     db_file = settings.get_db_file()
#     parser = argparse.ArgumentParser(description='Create the DuckDB database schema for Project dxdy v1.0 Phase 1.')
#     parser.add_argument('--db-path', type=str, default=str(db_file), help='Path to the DuckDB database file.')
#     args = parser.parse_args()
    
#     create_database_schema(db_path=args.db_path)
