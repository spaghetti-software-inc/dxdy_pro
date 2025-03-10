# Copyright (C) 2024-2025 Spaghetti Software Inc. (SPGI)

from dataclasses import dataclass, field
from typing import Dict



from textual.app import ComposeResult
from textual.screen import Screen
from textual.containers import Vertical
from textual.widget import Widget
from textual.widgets import ContentSwitcher, Header, Footer, Tree
from textual.events import Key
from textual.message import Message

import numpy as np
import plotext
from textual_plotext import PlotextPlot


from .db_screen import DuckDbTable

from ..settings import Settings
from ..saas_settings import SaaSConfig

from ..db.utils import get_current_cob_date, get_t_plus_one_cob_date, get_next_cob_date
from ..tui.tui_utils import format_currency


@dataclass
class ReportQueryPlan:
    sql_query: str = ""
    report_title: str = ""
    report_subtitle: str = ""
    
    summaries: Dict[str, float] = field(default_factory=dict)


def reporting_query_planner(query):
    report_type = query['type']
    security_id = query['security_id']
    portfolio_id = query['portfolio_id']
    cur_cob_date = query['cur_cob_date']
    
    bom_date = query['bom_date']
    eom_date = query['eom_date']
    
    soy_date = query['soy_date']
    ytd_date = query['ytd_date']
    
    if "ticker" in query:
        ticker = query['ticker']
    
    res = ReportQueryPlan()
    
    with Settings().get_db_connection() as db_conn:
        
        portfolio_df = f"""
                SELECT
                    *
                FROM
                    portfolios
                WHERE
                    portfolio_id = {portfolio_id}
                """    
        portfolio_df = db_conn.execute(portfolio_df).fetchdf()
        portfolio_name = portfolio_df.iloc[0]['portfolio_name']
        portfolio_ccy = portfolio_df.iloc[0]['portfolio_ccy']
    
    query["portfolio_name"] = portfolio_name
    query["portfolio_ccy"] = portfolio_ccy

    
    match report_type:
        case "pnl_security_level_report":
            res.sql_query = f"""
                SELECT
                    *
                FROM 
                    security_level_pnl
                WHERE
                    portfolio_id = {portfolio_id}
                AND
                    security_id = {security_id}
                ORDER BY
                    cob_date DESC
                """
            
            res.report_title = f"{portfolio_name}/[{security_id}]: {ticker}"
            res.report_subtitle = f"P&L Report ({portfolio_ccy})"  
            return res
        
        case "portfolio_pnl_report":
            res.sql_query = f"""
                SELECT
                    *
                FROM 
                    portfolio_level_pnl
                WHERE
                    portfolio_id = {portfolio_id}
                ORDER BY
                    cob_date DESC
                """
            
            res.report_title = f"{portfolio_name}"
            res.report_subtitle = f"P&L Report ({portfolio_ccy})"     
            return res

        case "portfolio_sector_report":
            res.sql_query = f"""
                SELECT
                    s.*,
                    mkt_value_portfolio_ccy / cb.latest_cash_balance AS pct_aum
                FROM 
                    sector_allocations s
                LEFT JOIN
                    cash_balance_as_of('{cur_cob_date}') cb
                ON
                    s.portfolio_id = cb.portfolio_id
                WHERE
                    s.portfolio_id = {portfolio_id}
                AND
                    cob_date = '{cur_cob_date}'
                ORDER BY
                    sector_name
                """
            
            res.report_title = f"{portfolio_name}"
            res.report_subtitle = f"Sector Allocations ({portfolio_ccy})"
            return res

        
        case "portfolio_strategy_report":
            res.sql_query = f"""
                PIVOT
                    (SELECT 
                        cob_date,
                        security_type_2,
                        position_type,
                        mkt_value_portfolio_ccy,
                        mkt_value_portfolio_ccy / cb.latest_cash_balance AS pct_aum
                    FROM 
                        strategy_allocations s
                    LEFT JOIN
                        cash_balance_as_of('{cur_cob_date}') cb
                    ON
                        s.portfolio_id = cb.portfolio_id
                    WHERE 
                        s.portfolio_id = {portfolio_id}
                    AND
                        cob_date = '{cur_cob_date}'
                    )
                ON
                    position_type
                USING
                    SUM(mkt_value_portfolio_ccy)
                ORDER BY
                    security_type_2
                """
            res.report_title = f"{portfolio_name}" 
            res.report_subtitle = f"Strategies ({portfolio_ccy})" 
            return res

            
        case "portfolio_fx_report":
            res.sql_query = f"""
                    SELECT 
                        cob_date,
                        security_ccy,
                        mkt_value_portfolio_ccy,
                        mkt_value_portfolio_ccy / cb.latest_cash_balance AS pct_aum
                    FROM 
                        fx_allocations fx
                    LEFT JOIN
                        cash_balance_as_of('{cur_cob_date}') cb
                    ON
                        fx.portfolio_id = cb.portfolio_id
                    WHERE 
                        fx.portfolio_id = {portfolio_id}
                    AND
                        cob_date = '{cur_cob_date}'
                    ORDER BY
                        security_ccy
                    """
            res.report_title = f"{portfolio_name}"
            res.report_subtitle = f"FX Exposure ({portfolio_ccy})"          
            return res
        
        case "pnl_drilldown_daily_report":
            res.sql_query = f"""
                SELECT
                    pnl.cob_date,
                    pnl.portfolio_id,
                    pnl.security_id,

                    CASE
                        WHEN s.security_type_2 = 'Option' THEN s.security_description
                        ELSE s.ticker
                    END AS  display_ticker,
                    
                    s.security_type_2,
                    pnl.net_quantity AS quantity,
                    pnl.avg_cost,
                    pnl.close_price,
                    pnl.prev_close_price,
                    pnl.cob_fx_rate,
                    pnl.intraday_pnl_local_ccy,
                    pnl.unrealized_dod_pnl_local_ccy,
                    pnl.dividend_amount_local_ccy,
                    pnl.total_dod_pnl_local_ccy,
                    pnl.total_dod_pnl_portfolio_ccy,
                    pnl.total_dod_pnl_portfolio_ccy/cb.latest_cash_balance AS dod_pnl_pct_aum
                FROM
                    daily_positions pnl
                LEFT JOIN
                    securities s
                ON
                    pnl.security_id = s.security_id
                LEFT JOIN
                    cash_balance_as_of('{cur_cob_date}') cb
                ON
                    pnl.portfolio_id = cb.portfolio_id
                WHERE
                    pnl.portfolio_id = {portfolio_id}
                AND
                    cob_date = '{cur_cob_date}'
                ORDER BY
                    security_type_2,
                    s.base_ticker ASC
                """
        
            with Settings().get_db_connection() as db:
                qry= f"""
                SELECT
                    SUM(total_dod_pnl_portfolio_ccy) as total_dod_pnl_portfolio_ccy,
                    SUM(pnl.total_dod_pnl_portfolio_ccy/cb.latest_cash_balance) AS dod_pnl_pct_aum,
                FROM 
                    daily_positions pnl
                LEFT JOIN
                    cash_balance_as_of('{cur_cob_date}') cb
                ON
                    pnl.portfolio_id = cb.portfolio_id
                WHERE
                    pnl.portfolio_id = {portfolio_id}
                AND
                    cob_date = '{cur_cob_date}'
                """
                tot_df = db.execute(qry).fetchdf()
                tot_delta_pnl = tot_df.iloc[0]['total_dod_pnl_portfolio_ccy']
                tot_delta_pnl_pct_aum = tot_df.iloc[0]['dod_pnl_pct_aum']
                
                res.summaries = {"tot_delta_pnl": tot_delta_pnl}
            
            res.report_title = f"Daily P&L Report {portfolio_name}"
            res.report_subtitle = f"{cur_cob_date.strftime('%Y-%m-%d')}: {format_currency(tot_delta_pnl)} {portfolio_ccy}"
            res.report_subtitle += " [{:.2%} AUM]".format(tot_delta_pnl_pct_aum)
            
            return res
        
        case "pnl_drilldown_mtd_report":
            res.sql_query = f"""
            SELECT
                pnl.portfolio_id,
                pnl.security_id,
                s.security_description AS display_ticker,
                s.security_type_2,
                SUM(pnl.intraday_pnl_local_ccy) AS intraday_pnl_local_ccy,
                SUM(pnl.unrealized_dod_pnl_local_ccy) AS unrealized_dod_pnl_local_ccy,
                SUM(pnl.dividend_amount_local_ccy) AS dividend_amount_local_ccy,
                SUM(pnl.total_dod_pnl_local_ccy) AS total_dod_pnl_local_ccy,
                SUM(pnl.total_dod_pnl_portfolio_ccy) AS total_dod_pnl_portfolio_ccy
            FROM
                daily_positions pnl
            LEFT JOIN
                securities s
            ON
                pnl.security_id = s.security_id
            WHERE
                pnl.portfolio_id = {portfolio_id}
            AND
                cob_date BETWEEN '{bom_date}' AND '{eom_date}'
            GROUP BY
                pnl.portfolio_id,
                pnl.security_id,
                s.security_description,
                s.security_type_2,	
            ORDER BY
                security_type_2,
                s.security_description ASC
                """
        
            with Settings().get_db_connection() as db:
                qry= f"""
                SELECT
                    SUM(total_dod_pnl_portfolio_ccy) as total_dod_pnl_portfolio_ccy,
                    SUM(pnl.total_dod_pnl_portfolio_ccy/cb.latest_cash_balance) AS dod_pnl_pct_aum,
                FROM 
                    daily_positions pnl
                LEFT JOIN
                    cash_balance_as_of('{cur_cob_date}') cb
                ON
                    pnl.portfolio_id = cb.portfolio_id
                WHERE
                    pnl.portfolio_id = {portfolio_id}
                AND
                    cob_date BETWEEN '{bom_date}' AND '{eom_date}'
                """
                tot_df = db.execute(qry).fetchdf()
                tot_delta_pnl = tot_df.iloc[0]['total_dod_pnl_portfolio_ccy']
                tot_delta_pnl_pct_aum = tot_df.iloc[0]['dod_pnl_pct_aum']
                
                res.summaries = {"tot_delta_pnl": tot_delta_pnl}
            
            res.report_title = f"MTD P&L Report {portfolio_name}"
            res.report_subtitle = f"{eom_date.strftime('%Y-%m-%d')}: {format_currency(tot_delta_pnl)} {portfolio_ccy}"
            res.report_subtitle += " [{:.2%} AUM]".format(tot_delta_pnl_pct_aum)
            
            return res        

        case "pnl_drilldown_ytd_report":
            res.sql_query = f"""
            SELECT
                pnl.portfolio_id,
                pnl.security_id,
                s.security_description AS display_ticker,
                s.security_type_2,
                SUM(pnl.intraday_pnl_local_ccy) AS intraday_pnl_local_ccy,
                SUM(pnl.unrealized_dod_pnl_local_ccy) AS unrealized_dod_pnl_local_ccy,
                SUM(pnl.dividend_amount_local_ccy) AS dividend_amount_local_ccy,
                SUM(pnl.total_dod_pnl_local_ccy) AS total_dod_pnl_local_ccy,
                SUM(pnl.total_dod_pnl_portfolio_ccy) AS total_dod_pnl_portfolio_ccy
            FROM
                daily_positions pnl
            LEFT JOIN
                securities s
            ON
                pnl.security_id = s.security_id
            WHERE
                pnl.portfolio_id = {portfolio_id}
            AND
                cob_date BETWEEN '{soy_date}' AND '{ytd_date}'
            GROUP BY
                pnl.portfolio_id,
                pnl.security_id,
                s.security_description,
                s.security_type_2,	
            ORDER BY
                security_type_2,
                s.security_description ASC
                """
        
            with Settings().get_db_connection() as db:
                qry= f"""
                SELECT
                    SUM(total_dod_pnl_portfolio_ccy) as total_dod_pnl_portfolio_ccy,
                    SUM(pnl.total_dod_pnl_portfolio_ccy/cb.latest_cash_balance) AS dod_pnl_pct_aum,
                FROM 
                    daily_positions pnl
                LEFT JOIN
                    cash_balance_as_of('{cur_cob_date}') cb
                ON
                    pnl.portfolio_id = cb.portfolio_id
                WHERE
                    pnl.portfolio_id = {portfolio_id}
                AND
                    cob_date BETWEEN '{soy_date}' AND '{ytd_date}'
                """
                tot_df = db.execute(qry).fetchdf()
                tot_delta_pnl = tot_df.iloc[0]['total_dod_pnl_portfolio_ccy']
                tot_delta_pnl_pct_aum = tot_df.iloc[0]['dod_pnl_pct_aum']
                
                res.summaries = {"tot_delta_pnl": tot_delta_pnl}
            
            res.report_title = f"YTD P&L Report {portfolio_name}"
            res.report_subtitle = f"{ytd_date.strftime('%Y-%m-%d')}: {format_currency(tot_delta_pnl)} {portfolio_ccy}"
            res.report_subtitle += " [{:.2%} AUM]".format(tot_delta_pnl_pct_aum)
            
            return res        
        
        case "cash_balance_report":
            res.sql_query = f"""
                SELECT
                    cob_date,
                    
                    cash_balance_portfolio_ccy
                FROM
                    cash_balance_view
                WHERE
                    portfolio_id = {portfolio_id}
                ORDER BY
                    cob_date DESC
                """
            
            res.report_title = f"{portfolio_name}"
            res.report_subtitle = f"Cash Balance ({portfolio_ccy})"
            return res
        
        case "divs_security_level_report":
            res.sql_query = f"""
                SELECT
                    ex_dividend_date,
                    cash_amount,
                    ccy
                FROM
                    dividends
                WHERE
                    security_id = {security_id}
                ORDER BY
                    ex_dividend_date DESC
                """
            res.report_title = f"{portfolio_name}/[{security_id}]: {ticker}"
            res.report_subtitle = f"Dividends Report"
            return res
        
        case "splits_security_level_report":
            res.sql_query = f"""
                SELECT
                    split_date,
                    split_from,
                    split_to
                FROM
                    stock_splits
                WHERE
                    security_id = {security_id}
                ORDER BY
                    split_date DESC
                """
            res.report_title = f"{portfolio_name}/[{security_id}]: {ticker}"
            res.report_subtitle = f"Splits Report"
            return res
        
        case "trades_report":
            res.sql_query = f"""
                SELECT
                    t.security_id,
                    t.trade_date AS cob_date,
                    s.ticker AS display_ticker,
                    s.exch_code,
                    s.name AS security_name,
                    t.quantity,
                    t.price
                FROM
                    trades t
                LEFT JOIN
                    securities s
                ON
                    t.security_id = s.security_id
                WHERE
                    portfolio_id = {portfolio_id}
                ORDER BY
                    trade_date DESC,
                    display_ticker
                """
            res.report_title = f"{portfolio_name}"
            res.report_subtitle = f"Trades Report"
            return res
        
        case "security_level_trades_report":
            res.sql_query = f"""
                SELECT
                    t.trade_date AS cob_date,
                    s.ticker AS display_ticker,
                    s.exch_code,
                    s.name AS security_name,
                    t.quantity,
                    t.price
                FROM
                    trades t
                LEFT JOIN
                    securities s
                ON
                    t.security_id = s.security_id
                WHERE
                    portfolio_id = {portfolio_id}
                AND
                    s.security_id = {security_id}
                ORDER BY
                    trade_date DESC
                """
            res.report_title = f"{portfolio_name}/[{security_id}]: {ticker}"
            res.report_subtitle = f"Trades Report"
            return res            
        
        case _:
            raise ValueError(f"Unknown report type: {report_type}")
    
class ReportsWidget(Widget):
    
    class ReportTitle(Message):
        def __init__(self, report_title: str, report_subtitle : str) -> None:
            self.report_title = report_title
            self.report_subtitle = report_subtitle
            super().__init__()
    
    
    active_report = None
    active_portfolio_id = None
    active_portfolio_name = None
    active_portfolio_crncy = None
    
    cur_cob_date = None # reporting Close of Business date
    cur_cob_date_idx = None
    dates = None
    table = None
    config = None
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        db_conn = Settings().get_db_connection()

        self.reporting_start_date = SaaSConfig().get_reporting_start_date()
        self.cur_cob_date = get_current_cob_date(db_conn)
        
        qry = f"""
        SELECT
            cob_date
        FROM
            calendar
        WHERE
            cob_date <= '{self.cur_cob_date}'
        ORDER BY
            cob_date
        """
        dates_df = db_conn.execute(qry).fetchdf()
        self.dates = dates_df['cob_date'].tolist()
        self.cob_date_idx = len(self.dates) - 1
        self.cob_date = self.dates[self.cob_date_idx]
                
        # end of month dates
        qry = f"""
        SELECT
            cob_date
        FROM
            calendar_eom_view
        ORDER BY
            cob_date
        """
        eom_dates_df = db_conn.execute(qry).fetchdf()
        self.eom_dates = eom_dates_df['cob_date'].tolist()
        self.eom_date_idx = len(self.eom_dates) - 1
        self.eom_date = self.eom_dates[self.eom_date_idx]
        
        # set the begining of the month date to the first of the month
        self.bom_date = self.eom_date.replace(day=1)
        
        # end of year dates 
        qry = f"""
        SELECT
            cob_date
        FROM
            calendar_years
        ORDER BY
            cob_date
        """
        ytd_dates_df = db_conn.execute(qry).fetchdf()
        
        self.ytd_dates = ytd_dates_df['cob_date'].tolist()
        self.ytd_date_idx = len(self.ytd_dates) - 1
        self.ytd_date = self.ytd_dates[self.ytd_date_idx]
        
        # set the begining of the year date to the first of the year
        self.soy_date = self.ytd_date.replace(month=1, day=1)
        
        
        
        
        
        self.config = Settings().get_ui_config_file()['reports']

           
           
           
    def compose(self) -> ComposeResult:
        tree: Tree[dict] = Tree("Reports", id="reports_selector", data={"type": "root"})
        tree.root.expand()
        tree.ICON_NODE = "📁 "
        tree.ICON_NODE_EXPANDED = "📁 "
        
        db_conn = Settings().get_db_connection()
        qry = """
        SELECT *
        FROM
            portfolios
        ORDER BY
            portfolio_name
        """
        portfolios_df = db_conn.execute(qry).fetchdf()

        #### Portfolio Node ####
        for row in portfolios_df.itertuples():
            portfolio = tree.root.add(row.portfolio_name, 
                                      data={"type": "portfolio", 
                                            "portfolio_id": row.portfolio_id,
                                            "security_id": None}, 
                                      expand=True)

            portfolio.add_leaf("P&L Report", 
                               data={"type": "portfolio_pnl_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})

            portfolio.add_leaf("P&L Chart", 
                               data={"type": "portfolio_pnl_chart", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})
            
            drilldown_node = portfolio.add("P&L Drilldown",
                                           data={"type": "portfolio_pnl_drilldown_node",
                                                 "portfolio_id": row.portfolio_id,
                                                 "security_id": None},
                                            expand=True)
            
            drilldown_node.add_leaf("Daily P&L", 
                               data={"type": "pnl_drilldown_daily_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})
            
            drilldown_node.add_leaf("MTD P&L", 
                               data={"type": "pnl_drilldown_mtd_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})
            
            drilldown_node.add_leaf("YTD P&L", 
                               data={"type": "pnl_drilldown_ytd_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})
            
            
            portfolio.add_leaf("◷ Sector Report", 
                               data={"type": "portfolio_sector_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})

            portfolio.add_leaf("◷ Strategy Report", 
                               data={"type": "portfolio_strategy_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})
            

            portfolio.add_leaf("◷ FX Report", 
                               data={"type": "portfolio_fx_report", 
                                     "portfolio_id": row.portfolio_id, 
                                     "security_id": None})
                        
            
            stocks_node = portfolio.add("Stocks", 
                                        data={"type": "stocks_node", 
                                              "portfolio_id": row.portfolio_id, 
                                              "security_id": None})
            
            qry = f"""
            SELECT
                * 
            FROM
                traded_securities
            WHERE
                security_type_2 = 'Common Stock'
            AND
                portfolio_id = {row.portfolio_id}
            ORDER BY 
                base_ticker
            """
            
            traded_securities_df = db_conn.execute(qry).fetchdf()
            
            #### Stocks Node ####
            for row in traded_securities_df.itertuples():
                
                stock_node = stocks_node.add(row.base_ticker, 
                                             data={"type": "stock_node", 
                                                   "portfolio_id": row.portfolio_id, 
                                                   "security_id": row.security_id})
                
                stock_node.add_leaf("P&L Report", 
                                    data={"type": "pnl_security_level_report", 
                                          "portfolio_id": row.portfolio_id, 
                                          "security_id": row.security_id,
                                          "ticker": row.base_ticker})
                
                stock_node.add_leaf("P&L Chart", 
                                    data={"type": "pnl_chart", 
                                          "portfolio_id": row.portfolio_id, 
                                          "security_id": row.security_id,
                                          "ticker": row.base_ticker}),
 
                stock_node.add_leaf("Structuring", 
                                    data={"type": "structuring_security_level_report", 
                                          "portfolio_id": row.portfolio_id, 
                                          "security_id": row.security_id,
                                          "ticker": row.base_ticker})
                
 
                stock_node.add_leaf("Dividends", 
                                    data={"type": "divs_security_level_report", 
                                          "portfolio_id": row.portfolio_id, 
                                          "security_id": row.security_id,
                                          "ticker": row.base_ticker})
            
                stock_node.add_leaf("Splits", 
                                    data={"type": "splits_security_level_report", 
                                          "portfolio_id": row.portfolio_id, 
                                          "security_id": row.security_id,
                                          "ticker": row.base_ticker})

    
                stock_node.add_leaf("🔵 Trades",
                                    data={"type": "security_level_trades_report", 
                                           "portfolio_id": row.portfolio_id, 
                                           "security_id": row.security_id,
                                           "ticker": row.base_ticker})


            ###### Options Node ######
            options_node = portfolio.add("Options", 
                                         data={"type": "options_node", 
                                               "portfolio_id": row.portfolio_id, 
                                               "security_id": None,
                                               "ticker": None})
            
            qry = f"""
            SELECT
                *
            FROM
                traded_securities s
            LEFT JOIN
                options o
            ON
                s.security_id = o.security_id
            WHERE
                security_type_2 = 'Option'
            AND
                portfolio_id = {row.portfolio_id}
            AND
                o.expiration_date >= '{self.cob_date}'
            ORDER BY
                s.security_description
            """
            
            traded_securities_df = db_conn.execute(qry).fetchdf()
            
            for row in traded_securities_df.itertuples():
                option_node = options_node.add(row.security_description, 
                                               data={"type": "option_node", 
                                                     "portfolio_id": row.portfolio_id, 
                                                     "security_id": row.security_id,
                                                     "ticker": row.base_ticker})
                
                option_node.add_leaf("P&L Report", 
                                     data={"type": "pnl_security_level_report", 
                                           "portfolio_id": row.portfolio_id, 
                                           "security_id": row.security_id,
                                           "ticker": row.security_description})
                
                option_node.add_leaf("P&L Chart", data={"type": "option_pnl_chart", 
                                                        "portfolio_id": row.portfolio_id, 
                                                        "security_id": row.security_id,
                                                        "ticker": row.security_description})

    
                option_node.add_leaf("🔵 Trades",
                                    data={"type": "security_level_trades_report", 
                                           "portfolio_id": row.portfolio_id, 
                                           "security_id": row.security_id,
                                           "ticker": row.security_description})

        
            portfolio.add_leaf("¢ Cash Report", 
                                data={"type": "cash_balance_report", 
                                        "portfolio_id": row.portfolio_id, 
                                        "security_id": None})
            
            
            trades_node = portfolio.add_leaf("🔵 Trades",
                                        data={"type": "trades_report", 
                                              "portfolio_id": row.portfolio_id, 
                                              "security_id": None})
            
        db_conn.close()
        
        
            
        
        self.table = DuckDbTable(id="pnl_report", table_format=self.config['pnl']['columns'])

        with Vertical(classes="reports_box1"):
            yield tree
        with ContentSwitcher(initial="pnl_report", id="content_switcher", classes="reports_box2"):
            yield self.table
            yield PlotextPlot(id="pnl_chart")
            
    
    def update_duckdb_table(self):
        rpt_plan = reporting_query_planner(self.query)
        #self.log(f"report_query_plan: {rpt_plan}")

        self.table.set_sql_query(rpt_plan.sql_query)

        self.post_message(self.ReportTitle(rpt_plan.report_title, rpt_plan.report_subtitle))
        self.query_one(ContentSwitcher).current = "pnl_report"
        
    
    def on_tree_node_selected(self, message: Tree.NodeSelected) -> None:
        #self.log(f"Tree Node Selected: {message.node.data}")
        
        if message.node.data["type"] == "root":
            return
        
        self.query = message.node.data
        
        
        portfolio_id = self.query["portfolio_id"]     
        security_id = self.query["security_id"]
        
        self.active_report = self.query["type"]
        self.active_portfolio_id = portfolio_id
            
        portfolio_df = f"""
                SELECT
                    *
                FROM
                    portfolios
                WHERE
                    portfolio_id = {portfolio_id}
                """
        db_conn = Settings().get_db_connection()
        portfolio_df = db_conn.execute(portfolio_df).fetchdf()
        portfolio_name = portfolio_df.iloc[0]['portfolio_name']
        portfolio_ccy = portfolio_df.iloc[0]['portfolio_ccy']
        db_conn.close()
        
        self.query["portfolio_name"] = portfolio_name
        self.query["portfolio_ccy"] = portfolio_ccy
        self.query["cur_cob_date"] = self.cur_cob_date
        
        self.query["bom_date"] = self.bom_date
        self.query["eom_date"] = self.eom_date
        
        self.query["soy_date"] = self.soy_date
        self.query["ytd_date"] = self.ytd_date
        
        
        if message.node.data["type"] in ["pnl_security_level_report",
                                         "portfolio_pnl_report",
                                         "portfolio_sector_report",
                                         "portfolio_strategy_report",
                                         "portfolio_fx_report",
                                         
                                         "pnl_drilldown_daily_report",
                                         "pnl_drilldown_mtd_report",
                                         "pnl_drilldown_ytd_report",
                                         
                                         "cash_balance_report",
                                         "divs_security_level_report",
                                         "splits_security_level_report",
                                         "trades_report",
                                         "security_level_trades_report"
                                         ]:
                        
            self.update_duckdb_table()
        
    
        elif message.node.data["type"] == "portfolio":
            if not message.node.is_expanded:
                message.node.expand()
        
        
        elif message.node.data["type"] == "pnl_chart":
            ticker = message.node.data["ticker"]
            
            db_conn = Settings().get_db_connection()
            qry = f"""
                SELECT
                    *
                FROM 
                    security_level_pnl
                WHERE
                    portfolio_id = {portfolio_id}
                AND
                    security_id = {security_id}
                ORDER BY
                    cob_date DESC
                """
            df = db_conn.execute(qry).fetchdf()
            db_conn.close()
            
            # filter out rows where NULL
            df = df[df['total_pnl_portfolio_ccy'].notnull()]
            
            plt_wrapper = self.query_one(PlotextPlot)
            plt = plt_wrapper.plt
            plt.clear_data()
            plt.clear_figure()
            dates = plotext.datetimes_to_string(df['cob_date'])
            
            # The Plotext "High definition" "hd" and "fhd" markers are available, including "braille".
            plt.plot(dates, df['total_pnl_portfolio_ccy'], marker='braille')
            plt.title(f"{portfolio_name} Total P&L")
            plt_wrapper.refresh()
            
            self.query_one(ContentSwitcher).current = "pnl_chart"
            self.post_message(self.ReportTitle(f"{portfolio_name}/[{security_id}]: {ticker}", 
                                               f"P&L Chart"))

        elif message.node.data["type"] == "option_pnl_chart":
            ticker = message.node.data["ticker"]
            
            db_conn = Settings().get_db_connection()
            qry = f"""
                SELECT
                    s.*
                FROM 
                    security_level_pnl s
                LEFT JOIN
                    options o
                ON
                    s.security_id = o.security_id
                WHERE
                    portfolio_id = {portfolio_id}
                AND
                    s.security_id = {security_id}
                AND
                    cob_date <= o.expiration_date
                ORDER BY
                    cob_date ASC
                """
            df = db_conn.execute(qry).fetchdf()
            db_conn.close()
            
            # filter out rows where NULL
            df = df[df['total_pnl_portfolio_ccy'].notnull()]
            
            plt_wrapper = self.query_one(PlotextPlot)
            plt = plt_wrapper.plt
            plt.clear_data()
            plt.clear_figure()
            dates = plotext.datetimes_to_string(df['cob_date'])
            
            # The Plotext "High definition" "hd" and "fhd" markers are available, including "braille".
            plt.plot(dates, df['total_pnl_portfolio_ccy'], marker='braille')
            plt.title(f"{portfolio_name} Total P&L")
            plt_wrapper.refresh()
            
            self.query_one(ContentSwitcher).current = "pnl_chart"
            self.post_message(self.ReportTitle(f"{portfolio_name}/[{security_id}]: {ticker}", 
                                               f"P&L Chart"))
        
        
        elif message.node.data["type"] == "portfolio_pnl_chart":
            
            db_conn = Settings().get_db_connection()
            qry = f"""
                SELECT
                    *
                FROM 
                    portfolio_level_pnl
                WHERE
                    portfolio_id = {portfolio_id}
                ORDER BY
                    cob_date DESC
                """
            df = db_conn.execute(qry).fetchdf()
            db_conn.close()
            
            # filter out rows where NULL
            df = df[df['total_pnl_portfolio_ccy'].notnull()]
            
            plt_wrapper = self.query_one(PlotextPlot)
            plt = plt_wrapper.plt
            plt.clear_data()
            plt.clear_figure()
            dates = plotext.datetimes_to_string(df['cob_date'])
                        
            plt.plot(dates, df['total_pnl_portfolio_ccy'], marker='braille')
            plt.title(f"{portfolio_name} Total P&L ({portfolio_ccy})")
            plt_wrapper.refresh()
            
            self.query_one(ContentSwitcher).current = "pnl_chart"            
            self.post_message(self.ReportTitle(f"{portfolio_name}", 
                                               f"P&L Chart ({portfolio_ccy})"))
        
            
            
        elif message.node.data["type"] == "structuring_security_level_report":
            ticker = message.node.data["ticker"]

            with Settings().get_db_connection() as db:
                qry = f"""
                SELECT 
                    *,
                    quantity * COALESCE(shares_per_contract, 1.0) as notional_qty
                FROM
                    positions('{self.cob_date}') psn
                LEFT JOIN
                    options o
                ON
                    psn.security_id = o.security_id
                WHERE
                    psn.portfolio_id = {portfolio_id}
                AND
                (    psn.security_id = {security_id}
                OR
                    o.underlying_security_id = {security_id})
                """
                
                psn_df = db.execute(qry).fetch_df()      
                
                qry = f"""
                SELECT *
                FROM
                    market_data
                WHERE
                    security_id = {security_id}
                AND
                    trade_date = '{self.cob_date}'
                """
                mkt_df = db.execute(qry).fetch_df()
                
                close_price = mkt_df['close_price'][0]
                
                
                pct_grid = np.linspace(-0.2, 0.3, 100)
                price_grid = close_price * (1 + pct_grid)
                
                payoffs = []
                for index, row in psn_df.iterrows():
                    if row['security_type_2'] == 'Common Stock':
                        payoff = row['notional_qty'] * price_grid
                    elif row['security_type_2'] == 'Option':
                        if row['contract_type'] == 'Call':
                            payoff = row['notional_qty'] * np.maximum(price_grid - row['strike_price'], 0)
                            
                        elif row['contract_type'] == 'Put':
                            payoff = row['notional_qty'] * np.maximum(row['strike_price'] - price_grid, 0)
                            
                    payoffs.append(payoff)
                
                # sum up payoffs
                payoffs_sum = np.sum(payoffs, axis=0)
                
                plt_wrapper = self.query_one(PlotextPlot)
                plt = plt_wrapper.plt
                plt.clear_data()
                plt.clear_figure()
                            
                plt.plot(price_grid, payoffs_sum, marker='braille')
                plt.title(f"{ticker} Payoff ({portfolio_ccy})")
                plt_wrapper.refresh()
                
                self.query_one(ContentSwitcher).current = "pnl_chart"            
                self.post_message(self.ReportTitle(f"{portfolio_name}/[{security_id}]: {ticker}", 
                                                f"Options Structuring"))     
            
               
                

    def on_key(self, event: Key) -> None:
        if self.active_report in ["pnl_drilldown_daily_report", 
                                  "portfolio_sector_report",
                                  "portfolio_strategy_report",
                                  "portfolio_fx_report"]:
            if event.key == "comma":
                self.cob_date_idx -= 1
                if self.cob_date_idx < 0:
                    self.cob_date_idx = 0

                self.cur_cob_date = self.dates[self.cob_date_idx]
                self.query["cur_cob_date"] = self.cur_cob_date
                
            elif event.key == "full_stop":
                self.cob_date_idx += 1
                if self.cob_date_idx >= len(self.dates):
                    self.cob_date_idx = len(self.dates) - 1
                    
                self.cur_cob_date = self.dates[self.cob_date_idx]
                self.query["cur_cob_date"] = self.cur_cob_date
                
            self.update_duckdb_table()
        
        elif self.active_report == "pnl_drilldown_mtd_report":
            if event.key == "comma":
                self.eom_date_idx -= 1
                if self.eom_date_idx < 0:
                    self.eom_date_idx = 0

                self.eom_date = self.eom_dates[self.eom_date_idx]
                self.bom_date = self.eom_date.replace(day=1)
                self.query["eom_date"] = self.eom_date
                self.query["bom_date"] = self.bom_date
                
            elif event.key == "full_stop":
                self.eom_date_idx += 1
                if self.eom_date_idx >= len(self.eom_dates):
                    self.eom_date_idx = len(self.eom_dates) - 1
                    
                self.eom_date = self.eom_dates[self.eom_date_idx]
                self.bom_date = self.eom_date.replace(day=1)
                self.query["eom_date"] = self.eom_date
                self.query["bom_date"] = self.bom_date
                
            self.update_duckdb_table()

        elif self.active_report == "pnl_drilldown_ytd_report":
            if event.key == "comma":
                self.ytd_date_idx -= 1
                if self.ytd_date_idx < 0:
                    self.ytd_date_idx = 0

                self.ytd_date = self.ytd_dates[self.ytd_date_idx]
                self.soy_date = self.ytd_date.replace(month=1, day=1)
                self.query["ytd_date"] = self.ytd_date
                self.query["soy_date"] = self.soy_date
                
            elif event.key == "full_stop":
                self.ytd_date_idx += 1
                if self.ytd_date_idx >= len(self.ytd_dates):
                    self.ytd_date_idx = len(self.ytd_dates) - 1
                    
                self.ytd_date = self.ytd_dates[self.ytd_date_idx]
                self.soy_date = self.ytd_date.replace(month=1, day=1)
                self.query["ytd_date"] = self.ytd_date
                self.query["soy_date"] = self.soy_date
                
            self.update_duckdb_table()

        
    def on_duck_db_table_query_input(self, event: DuckDbTable.QueryInput) -> None:
        if event.date_query is not None and self.active_report == "pnl_drilldown_daily_report":
            self.cur_cob_date = event.date_query
            
            # find the index of the cob_date in the dates list self.dates
            try:
                self.cob_date_idx = self.dates.index(self.cur_cob_date)
                self.cur_cob_date = self.dates[self.cob_date_idx]
                self.query["cur_cob_date"] = self.cur_cob_date
                
                self.update_duckdb_table()
                self.table.clear_input_widget()
                
            except:
                return 
            
class ReportsScreen(Screen):
    def compose(self) -> ComposeResult:
        # cur_cob_date = get_current_cob_date()
        # cur_trading_date = get_next_cob_date()
        # next_cob_date = get_t_plus_one_cob_date()
        
        yield Header(id="Header", icon="⋈", show_clock=False)
        yield Footer(id="Footer")
        yield ReportsWidget()
