# Copyright (C) 2024-2025 Spaghetti Software Inc. (SPGI)
import edgar as ed


import functools

import duckdb
import pandas as pd
import pandas_market_calendars as mkt_cal
import time
from datetime import datetime, date, timedelta

from .settings import Settings
from .saas_settings import SaaSConfig

from loguru import logger
from rich import print

def get_latest_filings(db, cob_date: date) -> list:
    user_agent = SaaSConfig().get_edgar_user_agent()
    ed.set_identity(user_agent)
    
    qry = f"""
    SELECT
        *
    FROM
        securities s
    WHERE
        s.security_id IN (SELECT security_id FROM positions('{cob_date}'))
    AND
        security_type_2 = 'Common Stock'
    AND
        exch_code = 'US'
    ORDER BY
        ticker
    """
    securities_df = db.execute(qry).fetch_df()
    
    filings = []
    
    for index, row in securities_df.iterrows():
        try:
            security_id = row.security_id
            figi = row.figi
            ticker = row.base_ticker
            company_name = row.name
            
            logger.info(f"Getting latest filings for {ticker} from EDGAR")
            company = ed.Company(ticker)
            
            tenQ_filings = company.get_filings(form='10-Q', is_xbrl=True)
            tenQ_latest_filing = tenQ_filings.latest()
            tenQ_filing_date = tenQ_latest_filing.filing_date
                        
            tenK_filings = company.get_filings(form='10-K', is_xbrl=True)
            tenK_latest_filing = tenK_filings.latest()
            tenK_filing_date = tenK_latest_filing.filing_date
            
            # which is the latest?
            latest_form = "10-Q" if tenQ_filing_date > tenK_filing_date else "10-K"
            latest_filing_date = tenQ_filing_date if tenQ_filing_date > tenK_filing_date else tenK_filing_date
            latest_filing = tenQ_latest_filing if tenQ_filing_date > tenK_filing_date else tenK_latest_filing
            
            filings.append({
                "security_id": security_id,
                "figi": figi,
                "ticker": ticker,
                "company_name": company_name,
                "latest_form": latest_form,
                "latest_filing_date": latest_filing_date,
                "latest_filing": latest_filing.obj()
            })
            
            time.sleep(1)
        except Exception as e:
            logger.warning(f"EDGAR filings not found for {ticker}: {e}")
            
    return filings