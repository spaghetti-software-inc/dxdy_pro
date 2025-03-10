# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

import json

from loguru import logger


from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, MarkdownViewer, TabbedContent, TabPane
from textual.containers import Vertical, Horizontal

from ..settings import Settings

class AiScreen(Screen):
    
    def get_market_commentary(self) -> str:
        with Settings().get_db_connection(readonly=True) as db:
            qry = f"""
            SELECT
                *
            FROM
                ai_analysis
            WHERE
                agent = 'agent_market_summary'
            ORDER BY
                cob_date DESC
            LIMIT 10
            """
            df = db.execute(qry).fetch_df()
            
            markdown_str = ""
            for index, row in df.iterrows():
                cob_date = row.cob_date.strftime('%Y-%m-%d')
                
                analysis_json = json.loads(row.analysis)
                comments = analysis_json['comments']
                
                markdown_str += f"# {cob_date}\n"
                
                for comment in comments:
                    hypothesis = comment['hypothesis']
                    data = comment['data']
                    markdown_str += f"* {hypothesis}\n"
                    markdown_str += f"\t + {data}\n\n"
                    
        return markdown_str
    
    
    def get_pnl_analysis(self) -> str:
        with Settings().get_db_connection(readonly=True) as db:
            qry = f"""
            SELECT
                *
            FROM
                portfolios
            """
            portfolios_df = db.execute(qry).fetch_df()
            
            markdown_str = ""
            for index, row in portfolios_df.iterrows():
                portfolio_id = row.portfolio_id
                
                markdown_str += f"# {row.portfolio_name}\n"
                
                qry = f"""
                SELECT
                    *
                FROM
                    ai_analysis
                WHERE
                    agent = 'agent_pnl_summary'
                AND 
                    portfolio_id = {portfolio_id}
                ORDER BY
                    cob_date DESC
                LIMIT 30
                """
                df = db.execute(qry).fetch_df()
                
                for index, row in df.iterrows():
                    cob_date = row.cob_date.strftime('%Y-%m-%d')
                    
                    analysis_json = json.loads(row.analysis)
                    comments = analysis_json['comments']
                    
                    markdown_str += f"# {cob_date}\n"
                    
                    for comment in comments:
                        observation = comment['observation']
                        data = comment['data']
                        markdown_str += f"* {observation}\n"
                        markdown_str += f"\t + {data}\n\n"
                        
        return markdown_str
    
    def get_earnings_analysis(self) -> str:
        with Settings().get_db_connection(readonly=False) as db:
            qry = f"""
            SELECT
                *
            FROM
                ai_analysis ai
            LEFT JOIN
                securities s
            ON
                ai.security_id = s.security_id
            WHERE
                agent = 'agent_earnings_analyst'
            AND 
                cob_date = (SELECT MAX(cob_date) FROM ai_analysis WHERE agent = 'agent_earnings_analyst' AND security_id = ai.security_id)
            ORDER BY
                cob_date DESC,
                s.ticker
            """
            ea_df = db.execute(qry).fetch_df()
            
            
            markdown_str = ""
            for index, ea in ea_df.iterrows():
                analysis_json = json.loads(ea.analysis)
                # self.log(analysis_json)
                
                filing_date = ea.cob_date.strftime('%Y-%m-%d')
                form = analysis_json['form']
                
                markdown_str += f"# {ea.ticker} {filing_date} ({form})\n"
                
                
                summary = analysis_json['summary']
                markdown_str += f"## Summary\n"
                markdown_str += f"* {summary}\n\n"
                
                income_statement = analysis_json['income_statement']
                analysis = income_statement['analysis']
                ratios = income_statement['profitability_ratios']
                comments = ratios['comments']
                
                markdown_str += f"## Income Statement\n"
                markdown_str += f"* {analysis}\n\n"
                markdown_str += f"* {comments}\n\n"
                
                balance_sheet = analysis_json['balance_sheet']
                analysis = balance_sheet['analysis']
                liquidity_ratios = balance_sheet['liquidity_ratios']
                liquidity_comments = liquidity_ratios['comments']
                solvency_ratios = balance_sheet['solvency_ratios']
                solvency_comments = solvency_ratios['comments']
                
                markdown_str += f"## Balance Sheet\n"
                markdown_str += f"* {analysis}\n\n"
                markdown_str += f"* {comments}\n\n"
                markdown_str += f"* {liquidity_comments}\n\n"
                markdown_str += f"* {solvency_comments}\n\n"
                
                cashflow_statement = analysis_json['cashflow_statement']
                analysis = cashflow_statement['analysis']
                cashflow_analysis = cashflow_statement['cashflow_analysis']
                comments = cashflow_analysis['comments']
                
                markdown_str += f"## Cashflow Statement\n"
                markdown_str += f"* {analysis}\n\n"
                markdown_str += f"* {comments}\n\n"
                
                
                
        return markdown_str
            
    
    def get_technical_analysis(self) -> str:
        with Settings().get_db_connection(readonly=False) as db:
            qry = f"""
            SELECT
                *
            FROM
                ai_analysis ai
            LEFT JOIN
                securities s
            ON
                ai.security_id = s.security_id
            WHERE
                agent = 'agent_technical_analyst'
            AND 
                cob_date = (SELECT MAX(cob_date) FROM ai_analysis WHERE agent = 'agent_technical_analyst' AND security_id = ai.security_id)
            ORDER BY
                s.ticker
            """
            ta_df = db.execute(qry).fetch_df()
            
            markdown_str = ""
            for index, ta in ta_df.iterrows():
                markdown_str += f"# {ta.ticker}: {ta['name']}\n"
                
                analysis_json = json.loads(ta.analysis)
                # self.log(analysis_json)
                
                patterns = analysis_json['patterns']
                patterns = sorted(analysis_json['patterns'], key=lambda x: x['date_range'])

                
                
                for analysis in patterns:
                    date_range = analysis['date_range']
                    pattern_type = analysis['pattern_type']
                    sentiment = analysis['sentiment']
                    commentary = analysis['commentary']
                    markdown_str += f"## {date_range}\n"
                    markdown_str += f"* {pattern_type}: {sentiment}\n"
                    markdown_str += f"\t + {commentary}\n\n"

        return markdown_str
    
    def compose(self) -> ComposeResult:
        markdown_str = self.get_market_commentary()
        self.market_overview = MarkdownViewer(markdown_str, show_table_of_contents=True)       

        markdown_str = self.get_pnl_analysis()
        self.pnl_analysis = MarkdownViewer(markdown_str, show_table_of_contents=True)

        markdown_str = self.get_technical_analysis()
        self.technical_analysis = MarkdownViewer(markdown_str, show_table_of_contents=True)
        
        # markdown_str = self.get_earnings_analysis()
        # self.earnings_analysis = MarkdownViewer(markdown_str, show_table_of_contents=True)
        
        yield Header(id="Header", icon="⋈", show_clock=False)
        with Vertical():
            with TabbedContent(id="tabbed_content"):
                with TabPane("Market Overview", id="market_overview"):
                    yield self.market_overview
                    
                with TabPane("P&L Analysis", id="pnl_analysis"):
                    yield self.pnl_analysis
                    
                # with TabPane("Earnings Analysis", id="earnings_analysis"):
                #     yield self.earnings_analysis

                with TabPane("Technical Analysis", id="technical_analysis"):
                    yield self.technical_analysis
                    

        yield Footer(id="Footer")




    # def log_msg(self, content):
    #     self.log_widget.write(content)
        