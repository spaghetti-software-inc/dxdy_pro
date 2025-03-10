# 
#  securities identifier data lookup process  
#
#  ------------------------
#  |  external sources    |
#  |     [Wikipedia]      | ---> [Ticker, Exchange, Security Type] ---> [OpenFIGI]
#  | [Manual Data Entry]  |                                                |
#  ------------------------                                                |
#                                                                          |
#                                 ----------------------                   |
#                                 |  securities table  |  <----------------|
#                                 ----------------------                   

import duckdb
import argparse
import time

from pathlib import Path


import json
import urllib.request
import urllib.parse

import pandas as pd
import pandas_market_calendars as mkt_cal


from ..db.utils import DuckDBTemporaryTable
from ..settings import Settings



from loguru import logger
import rich




def get_trading_calendar(start_date, end_date) -> pd.DataFrame:
    calendars = Settings().get_calendars()
    schedules = []
    for calendar in calendars:
        calendar_obj = mkt_cal.get_calendar(calendar)
        sched = calendar_obj.schedule(start_date=start_date, end_date=end_date)
        sched['exchange'] = calendar
        schedules.append(sched)
    combined_schedule = pd.concat(schedules).sort_values(by='market_open').reset_index(drop=True)
    return combined_schedule




def get_spx_companies() -> pd.DataFrame:
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    df = pd.read_html(url)[0]

    attribute_to_column = {
        'Symbol': 'base_ticker',
        'Security': 'company_name',
        'GICS Sector': 'sector',
        'GICS Sub-Industry': 'gics_sub_industry',
        'Headquarters Location': 'headquarters_location',
        'Date added': 'date_added',
        'CIK': 'cik',
        'Founded': 'founded'
    }

    # Rename columns
    df.rename(columns=attribute_to_column, inplace=True)

    # replace . with - in the base_ticker
    df['base_ticker'] = df['base_ticker'].str.replace('.', '-')

    df['exch_code'] = 'US'
    df['security_type_2'] = 'Common Stock'
    
    df.loc[df['gics_sub_industry'].str.contains('REIT', na=False),'security_type_2'] = 'REIT'

    return df[['base_ticker', 'exch_code', 'security_type_2', 'company_name', 'sector']]



def get_tsx_companies() -> pd.DataFrame:
    url = 'https://en.wikipedia.org/wiki/S%26P/TSX_60'
    df = pd.read_html(url)[0]

    attribute_to_column = {
        'Symbol': 'base_ticker',
        'Company': 'company_name',
        'Sector': 'sector'
    }

    # Rename columns
    df.rename(columns=attribute_to_column, inplace=True)

    # replace . with - in the base_ticker
    df['base_ticker'] = df['base_ticker'].str.replace('.', '/')

    df['exch_code'] = 'CN'
    df['security_type_2'] = 'Common Stock'
    
    # df.loc[df['gics_sub_industry'].str.contains('REIT', na=False),'security_type_2'] = 'REIT'

    return df[['base_ticker', 'exch_code', 'security_type_2', 'company_name', 'sector']]


def get_ftse_companies() -> pd.DataFrame:
    url = 'https://en.wikipedia.org/wiki/FTSE_100_Index'
    df = pd.read_html(url)[4]    
    
    attribute_to_column = {
        'Ticker': 'base_ticker',
        'Company': 'company_name',
        'FTSE industry classification benchmark sector[25]': 'sector'
    }
    # Rename columns
    df.rename(columns=attribute_to_column, inplace=True)

    df['exch_code'] = 'GB'
    df['security_type_2'] = 'Common Stock'
    
    return df[['base_ticker', 'exch_code', 'security_type_2', 'company_name', 'sector']]

def get_cac40_companies() -> pd.DataFrame:
    url = 'https://en.wikipedia.org/wiki/CAC_40'
    df = pd.read_html(url)[4]    
    
    attribute_to_column = {
        'Ticker': 'base_ticker',
        'Company': 'company_name',
        'Sector': 'sector'
    }
    # # Rename columns
    df.rename(columns=attribute_to_column, inplace=True)

    # remove the trailing '.PA' from the base_ticker
    df['base_ticker'] = df['base_ticker'].str.replace('.PA', '')

    df['exch_code'] = 'FP'
    df['security_type_2'] = 'Common Stock'
    
    return df[['base_ticker', 'exch_code', 'security_type_2', 'company_name', 'sector']]

def get_dax_companies() -> pd.DataFrame:
    #https://en.wikipedia.org/wiki/DAX
    
    url = 'https://en.wikipedia.org/wiki/DAX'
    
    df = pd.read_html(url)[4]    
    
    attribute_to_column = {
        'Ticker': 'base_ticker',
        'Company': 'company_name',
        'Prime Standard Sector': 'sector'
    }
    # # Rename columns
    df.rename(columns=attribute_to_column, inplace=True)

    df['exch_code'] = 'DE'
    df['security_type_2'] = 'Common Stock'
    
    return df[['base_ticker', 'exch_code', 'security_type_2', 'company_name', 'sector']]
    




"""
See https://www.openfigi.com/api for more information.

This script is written to be run by python3 - tested with python3.12 - without any external libraries.
For more involved use cases, consider using open source packages: https://pypi.org/
"""

JsonType = None | int | str | bool | list["JsonType"] | dict[str, "JsonType"]

# OPENFIGI_API_KEY = os.environ.get(
#     "OPENFIGI_API_KEY", None
# )  # Put your API key here or in env var

OPENFIGI_API_KEY = "fa1baca6-3d87-46aa-aa64-0ca475d7b472"

OPENFIGI_BASE_URL = "https://api.openfigi.com"


def open_figi_api_call(
    path: str,
    data: dict | None = None,
    method: str = "POST",
) -> JsonType:
    """
    Make an api call to `api.openfigi.com`.
    Uses builtin `urllib` library, end users may prefer to
    swap out this function with another library of their choice

    Args:
        path (str): API endpoint, for example "search"
        method (str, optional): HTTP request method. Defaults to "POST".
        data (dict | None, optional): HTTP request data. Defaults to None.

    Returns:
        JsonType: Response of the api call parsed as a JSON object
    """

    headers = {"Content-Type": "application/json"}
    if OPENFIGI_API_KEY:
        headers |= {"X-OPENFIGI-APIKEY": OPENFIGI_API_KEY}

    request = urllib.request.Request(
        url=urllib.parse.urljoin(OPENFIGI_BASE_URL, path),
        data=data and bytes(json.dumps(data), encoding="utf-8"),
        headers=headers,
        method=method,
    )

    with urllib.request.urlopen(request) as response:
        json_response_as_string = response.read().decode("utf-8")
        json_obj = json.loads(json_response_as_string)
        return json_obj
    

def get_openfigi_mapping_jobs(df : pd.DataFrame, chunk_size : int) -> list:
    logger.info(f"Creating mapping jobs for {len(df)} securities")
    tickers = []
    jobs = []
    for i in range(0, len(df), chunk_size):
        chunk = df[i:i+chunk_size]
        chunk_tickers = []
        mapping_jobs = []
        for row in chunk.itertuples():
            ticker = row.base_ticker
            exch_code = row.exch_code
            security_type_2 = row.security_type_2

            chunk_tickers.append(row.base_ticker)
            mapping_jobs.append({'idType': 'TICKER', 'idValue': ticker, 'exchCode': exch_code, 'securityType2': security_type_2})
        
        tickers.append(chunk_tickers)
        jobs.append(mapping_jobs) 
    return jobs


def get_open_figi(base_ticker : str, exch_code : str, security_type_2 : str):
    mapping_request = [
        {
            "idType": "BASE_TICKER",
            "idValue": base_ticker,
            "exchCode": exch_code,
            "securityType2": security_type_2,
        }
    ]
    
    mapping_response = open_figi_api_call("/v3/mapping", mapping_request)    
    return mapping_response[0]["data"][0]["figi"]


def initialize_stocks(db) -> None:
    #spx = get_spx_companies()
    #tsx = get_tsx_companies()
    #companies = pd.concat([spx, tsx]).reset_index(drop=True)
    companies = get_spx_companies()
    
    # take the first 9 companies
    companies = companies.head(9)
    
    # drop companies with missing base_ticker
    companies = companies.dropna(subset=['base_ticker'])
    
    mapping_request_jobs = get_openfigi_mapping_jobs(companies, chunk_size=100)
    
    responses = []
    for job_chunk in mapping_request_jobs:
        mapping_response = open_figi_api_call("/v3/mapping", job_chunk)
        
        for response in mapping_response:
            if 'data' in response:
                responses.append(response['data'][0])
        
        time.sleep(0.5)

    responses_df = pd.json_normalize(responses)
    rich.print(responses_df)
    
    
    responses_df.loc[responses_df['exchCode'] == 'US', 'ccy'] = 'USD'
    responses_df.loc[responses_df['exchCode'] == 'CN', 'ccy'] = 'CAD'
    
    # responses_df['full_ticker'] = responses_df['ticker'] \
    #                             + ' ' + responses_df['exchCode'] \
    #                             + ' ' + responses_df['marketSector']
    
    responses_df['full_ticker'] = responses_df['ticker']
    
    # merge the responses with the companies
    responses_df = responses_df.merge(companies, 
                                left_on=['ticker', 'exchCode'],
                                right_on=['base_ticker', 'exch_code'],
                                how='left')
    
    # find rows in responses_df with duplicate figi
    #responses_df = responses_df[responses_df.duplicated(subset=['compositeFIGI'], keep=False)]
    
    if responses_df.duplicated(subset=['compositeFIGI']).any():
        duplicates = responses_df[responses_df.duplicated(subset=['compositeFIGI'], keep=False)]
        logger.warning(f"Found {len(duplicates)} duplicate FIGIs")
    
    rich.print(responses_df)
    
    with DuckDBTemporaryTable(db, 'tmp_securities', responses_df) as table_name:
        db.execute(f"""
        INSERT INTO
            securities (base_ticker, exch_code, security_type_2, ticker, name, figi, ccy)
        SELECT
            ticker, exchCode, securityType2, full_ticker, name, compositeFIGI, ccy
        FROM
            tmp_securities
                   
        """)
        db.commit()
        
        logger.info(f"Inserted {len(responses_df)} records into securities table")

        ################# init sectors ################
        qry = f"""
        INSERT INTO
            sectors (sector_name)
        SELECT
            DISTINCT sector
        FROM
            tmp_securities
        WHERE
            sector NOT IN (SELECT sector_name FROM sectors)
        """
        db.execute(qry)
        db.commit()
        logger.info("Sectors initialized")
    
        ################# init sector mappings ################
        qry = f"""
        INSERT INTO
            sector_mappings (security_id, sector_id)
        SELECT
            s.security_id, sec.sector_id
        FROM
            securities s
        JOIN
            tmp_securities tmp
        ON
            s.base_ticker = tmp.base_ticker
        AND
            s.exch_code = tmp.exchCode
        AND
            s.security_type_2 = tmp.securityType2
        JOIN
            sectors sec
        ON
            tmp.sector = sec.sector_name
        """
        db.execute(qry)
        db.commit()
        
        logger.info("Sector mappings initialized")