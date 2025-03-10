from datetime import date
import pandas as pd
from tabulate import tabulate

from openai import OpenAI
#client = OpenAI()

from pydantic import BaseModel

from dxdy.settings import Settings
from dxdy.saas_settings import SaaSConfig

import rich




def daily_market_snapshot(db, cob_date : date) -> pd.DataFrame:
    qry = f"""
    SELECT
        m.base_ticker AS ticker,
        m.name AS company_name,
        st.sector_name AS sector,
        ROUND(m.daily_return * 100, 2) AS daily_return_pct, 
        ROUND(daily_volume_change_pct * 100, 2) AS daily_volume_change_pct
    FROM 
        market_daily_returns m
    LEFT JOIN
        sector_mappings smap
    ON
        m.security_id = smap.security_id
    LEFT JOIN
        sectors st
    ON
        st.sector_id = smap.sector_id
    WHERE 
        trade_date = '{cob_date}'
    ORDER BY
        daily_return_pct ASC,
        daily_volume_change_pct ASC,
        ticker
    """
    df = db.execute(qry).fetch_df()
    return df

class Hypothesis(BaseModel):
    hypothesis: str
    data: str
    
class MarketCommentary(BaseModel):
    comments: list[Hypothesis]
    

def get_daily_market_commentary(db, cob_date : date) -> MarketCommentary:
    
    client = OpenAI(api_key=SaaSConfig().get_openai_key())
    
    
    snapshot = daily_market_snapshot(db, cob_date)
    market_data_str = tabulate(snapshot, headers='keys', tablefmt='github', showindex=False)
    
    #rich.print(market_data_str)

    system_prompt = f"You are a helpful stock market analyst. You are analyzing the day's market returns, to identify predictable patterns and provide market commentary and analysis."
    user_prompt = f"Following is the market data for {cob_date}: ```{market_data_str}```. Please provide your analysis."

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=MarketCommentary,
    )

    output = completion.choices[0].message.parsed

    return output





def daily_portfolio_snapshot(db, cob_date : date, portfolio_id : int) -> pd.DataFrame:
    qry = f"""
    SELECT
        s.ticker,
        s.name AS company_name,
        st.sector_name,
        psn.quantity,
        ROUND(pnl.close_price,2) AS close_price,
        ROUND(psn.quantity * pnl.close_price) AS market_value_local_ccy,
        ROUND(pnl.delta_total_pnl_local,2) AS day_over_day_total_pnl_local_ccy,
        ROUND(pnl.delta_pnl_portfolio_ccy,2) AS day_over_day_pnl_portfolio_ccy
    FROM
        security_level_pnl_daily_delta pnl
    LEFT JOIN
        positions('{cob_date}') psn
    ON
        pnl.security_id = psn.security_id
    LEFT JOIN
        securities s
    ON
        s.security_id = pnl.security_id
    LEFT JOIN
        sector_mappings smap
    ON
        pnl.security_id = smap.security_id
    LEFT JOIN
        sectors st
    ON
        st.sector_id = smap.sector_id
    WHERE
        cob_date = '{cob_date}'
    AND
        pnl.portfolio_id = {portfolio_id}
    ORDER BY
        day_over_day_total_pnl_local_ccy ASC
    """
    df = db.execute(qry).fetch_df()
    return df


class PnLAnalysis(BaseModel):
    observation: str
    data: str
    
class PnLCommentary(BaseModel):
    comments: list[PnLAnalysis]


def get_daily_pnl_commentary(db, cob_date : date, portfolio_id : int) -> MarketCommentary:
    
    client = OpenAI(api_key=SaaSConfig().get_openai_key())
    
    
    market_snapshot = daily_market_snapshot(db, cob_date)
    market_data_str = tabulate(market_snapshot, headers='keys', tablefmt='github', showindex=False)
    
    #rich.print(market_data_str)
    
    portfolio_snapshot = daily_portfolio_snapshot(db, cob_date, portfolio_id)
    pnl_data_str = tabulate(portfolio_snapshot, headers='keys', tablefmt='github', showindex=False)

    system_prompt = f"You are a helpful stock portfolio P&L analyst. You are analyzing the day's market returns and portfolio P&L, to provide market P&L analysis and analyze the portfolio positioning. This is a long/short equities hedge fund portfolio. Negative quantities indicate short positions."
    user_prompt = f"Following is the market data for {cob_date}: ```{market_data_str}```. Following is the day's P&L for the portfolio: {pnl_data_str} Please provide your analysis."

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=PnLCommentary,
    )

    output = completion.choices[0].message.parsed

    return output



class TechnicalPattern(BaseModel):
    date_range: str
    pattern_type: str
    commentary: str
    sentiment: str

class TechnicalAnalysis(BaseModel):
    date_range: str
    patterns: list[TechnicalPattern]
    
def technical_analysis_snapshot(db, security_id : int) -> pd.DataFrame:
    qry = f"""
    SELECT
        trade_date AS date,
        ROUND(open_price,2) AS open,
        ROUND(high_price,2) AS high,
        ROUND(low_price,2) AS low,
        ROUND(close_price,2) as close,
        volume
    FROM
        market_data m
    WHERE
        security_id = {security_id}
    ORDER BY
        trade_date DESC
    LIMIT 30
    """
    df = db.execute(qry).fetch_df()
    return df


def get_technical_analysis(db, security) -> MarketCommentary:
    client = OpenAI(api_key=SaaSConfig().get_openai_key())
    
    market_snapshot = technical_analysis_snapshot(db, security.security_id)
    market_data_str = tabulate(market_snapshot, headers='keys', tablefmt='github', showindex=False)
    
    system_prompt = f"You are a helpful stock market technical analyst focused on {security.ticker} ({security['name']}). You are analyzing the stock market time-series data for a given date range, to identify predictable patterns and provide market commentary and analysis."
    user_prompt = f"Following is a time-series of the stock price: ```{market_data_str}```. Please provide your analysis."

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=TechnicalAnalysis,
    )

    output = completion.choices[0].message.parsed

    return output


class LiquidityRatios(BaseModel):
    current_ratio: float
    quick_ratio: float
    comments: str
    
class SolvencyRatios(BaseModel):
    debt_to_equity: float
    debt_to_assets: float
    comments: str
        
class ProfitabilityRatios(BaseModel):
    gross_margin: float
    operating_margin: float
    net_margin: float
    comments: str
    
class CashFlowAnalysis(BaseModel):
    operating_cash_flow: float
    investing_cash_flow: float
    financing_cash_flow: float
    free_cash_flow: float
    comments: str


class BalanceSheetAnalysis(BaseModel):
    date: str
    liquidity_ratios: LiquidityRatios
    solvency_ratios: SolvencyRatios
    analysis: str
    
class IncomeStatementAnalysis(BaseModel):
    date: str
    profitability_ratios: ProfitabilityRatios
    analysis: str
    
class CashFlowStatementAnalysis(BaseModel):
    date: str
    cashflow_analysis: CashFlowAnalysis
    analysis: str

class EarningsAnalysis(BaseModel):
    filing_date: str
    form: str
    
    balance_sheet: BalanceSheetAnalysis
    income_statement: IncomeStatementAnalysis
    cashflow_statement: CashFlowStatementAnalysis
    
    summary: str
    

def get_earnings_analysis(db, filing):
    financials = filing['latest_filing'].financials
    
    balance_sheet = financials.get_balance_sheet()                     # or financials.balance_sheet
    income_statement = financials.get_income_statement()               # or financials.income
    cashflow_statement = financials.get_cash_flow_statement()          # or financials.cashflow
    
    bs_df = balance_sheet.get_dataframe()
    is_df = income_statement.get_dataframe()
    cf_df = cashflow_statement.get_dataframe()
    
    bs_str = tabulate(bs_df, headers='keys', tablefmt='github', showindex=False)
    is_str = tabulate(is_df, headers='keys', tablefmt='github', showindex=False)
    cf_str = tabulate(cf_df, headers='keys', tablefmt='github', showindex=False)
    
    client = OpenAI(api_key=SaaSConfig().get_openai_key())
    
    
    system_prompt = f"You are a helpful equity analyst focused on {filing['ticker']} ({filing['company_name']}). You are analyzing the Financial Statements for the latest {filing['latest_form']} filing ({filing['latest_filing_date']}), to identify predictable patterns and provide market commentary and analysis."
    user_prompt = f"Following is the financial statements data. Balance Sheet: ```{bs_str}```, Income Statement: ```{is_str}```, Cashflow Statement: ```{cf_str}```.  Please provide your analysis."

    completion = client.beta.chat.completions.parse(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        response_format=EarningsAnalysis,
    )

    output = completion.choices[0].message.parsed

    return output   