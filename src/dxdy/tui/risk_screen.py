# Copyright (C) 2024-2025  Spaghetti  Software  Inc.



import pandas as pd

from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widgets import Header, Footer, Tree, TabbedContent, TabPane
from textual.widget import Widget
from textual.events import Key

from textual_plotext import PlotextPlot
import plotext

from ..settings import Settings
from ..db.utils import get_current_cob_date
from .tui_utils import format_currency

from datetime import timedelta



class RiskScreen(Screen):
    cur_cob_date = None
    cob_date_idx = None
    dates = None
    portfolio_id = None
    portfolio_name = None
    portfolio_ccy = None
    active_tab_id = "sector_allocations"
    
    def __init__(self) -> None:
        super().__init__()
        self.cur_cob_date = get_current_cob_date()
        
        db_conn = Settings().get_db_connection()
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
        
        db_conn.close()
    
    def compose(self) -> ComposeResult:
        tree: Tree[dict] = Tree("Portfolios", id="reports_selector", classes="box1", data={"type": "root"})
        tree.root.expand()       
        
        tree.ICON_NODE = "📁 "
        tree.ICON_NODE_EXPANDED = "📁 "
        
        db_conn = Settings().get_db_connection()
        qry = """
        SELECT *
        FROM
            portfolios
        """
        df = db_conn.execute(qry).fetchdf()
        
        for row in df.itertuples():
            tree.root.add_leaf(row.portfolio_name, 
                               data={"portfolio_id": row.portfolio_id, 
                                     "portfolio_name": row.portfolio_name, 
                                     "portfolio_ccy": row.portfolio_ccy})
            
        db_conn.close()
        
        sector_chart = PlotextPlot(id="sector_chart")
        strategy_chart = PlotextPlot(id="strategy_chart")
        crncy_chart = PlotextPlot(id="crncy_chart")
        
        yield Header(id="Header", icon="∂x∂y", show_clock=True)
        yield tree
        with Vertical(classes="box2"):
            with TabbedContent(id="tabbed_content"):
                with TabPane("Sectors", id="sector_allocations"):
                    yield sector_chart
                    
                with TabPane("Strategies", id="strategy_allocations"):
                    yield strategy_chart
                    
                # with TabPane("FX", id="crncy_allocations"):
                #     yield crncy_chart
                    
           #yield chart
        yield Footer(id="Footer")
        
    def on_tabbed_content_tab_activated(self, event: TabbedContent.TabActivated) -> None:
        self.active_tab_id = event.tab.id.replace("--content-tab-", "")
        self.update_chart()
        
        
    def update_chart(self) -> None:
        self.log(f"Updating chart for {self.cob_date} - {self.portfolio_id} - {self.active_tab_id}")
        
        
        db_conn = Settings().get_db_connection()
        
        if self.active_tab_id == "sector_allocations":
            if self.portfolio_id is None:
                return
                
            else:
                qry = f"""
                SELECT
                    sector_name,
                    mkt_value_portfolio_ccy
                FROM
                    sector_allocations
                WHERE
                    cob_date = '{self.cob_date}'
                AND
                    portfolio_id = {self.portfolio_id}
                ORDER BY
                    sector_name
                """
                
                qry_min_max = f"""
                SELECT
                    MIN(mkt_value_portfolio_ccy) AS xmin,
                    MAX(mkt_value_portfolio_ccy) AS xmax
                FROM
                    sector_allocations
                WHERE
                    cob_date = '{self.cob_date}'
                AND
                    portfolio_id = {self.portfolio_id}
                """
            
            df = db_conn.execute(qry).fetchdf()
            df.fillna(0.00, inplace=True)
            if df.shape[0] < 2: # not enough data to plot
                self.log(f"Not enough data to plot: {df.shape[0]}")
                return
            
            if (df['mkt_value_portfolio_ccy'] == 0).all():
                self.log(f"All values are zero, not plotting")
                return
            
            
            min_max_df = db_conn.execute(qry_min_max).fetchdf()
            xmin = min_max_df.loc[0, 'xmin']
            xmax = min_max_df.loc[0, 'xmax']
            
            chart_elem = "#sector_chart"
            title_str = f"{self.cob_date.strftime('%Y-%m-%d')} - {self.portfolio_name} - {self.portfolio_ccy}"
            x_data = df['sector_name'].tolist()
            y_data = df['mkt_value_portfolio_ccy'].tolist()

            chart_orientation = "horizontal"
            
            plt_wrapper = self.query_one(chart_elem)
            plt = plt_wrapper.plt
            plt.clear_data()
            plt.clear_figure()
            
            self.log(f"Plotting chart with plotext: {(x_data)} x {(y_data)}")
            if len(x_data) == 1 and len(y_data) == 1:
                self.log(f"Not enough data to plot: {len(x_data)} x {len(y_data)}")
                return
            


            try:
                plt.bar(x_data, y_data, orientation = chart_orientation, width=1/128)
                
                # set the x-axis range to the min and max values of the data
                plt.xlim(xmin, xmax)

                plt.title(title_str)
                plt_wrapper.refresh()
                
                self.log(f"Plotted chart with plotext: {(x_data)} x {(y_data)}")
            except Exception as e:
                self.log(f"Error plotting chart: {e}")
            
        elif self.active_tab_id == "strategy_allocations":
            if self.portfolio_id is None:
                qry = f"""
                SELECT
                    security_type_2,
                    position_type,
                    SUM(mkt_value_portfolio_ccy) AS mkt_value_portfolio_ccy
                FROM
                    strategy_allocations
                WHERE
                    cob_date = '{self.cob_date}'
                GROUP BY
                    security_type_2,
                    position_type
                ORDER BY
                    security_type_2,
                    position_type
                """
                
                qry_min_max = f"""
                SELECT
                    MIN(mkt_value_portfolio_ccy) AS xmin,
                    MAX(mkt_value_portfolio_ccy) AS xmax
                FROM
                    strategy_allocations
                WHERE
                    cob_date = '{self.cob_date}'
                """
                                
            else:
                qry = f"""
                SELECT
                    security_type_2,
                    position_type,
                    mkt_value_portfolio_ccy
                FROM
                    strategy_allocations
                WHERE
                    cob_date = '{self.cob_date}'
                AND
                    portfolio_id = {self.portfolio_id}
                ORDER BY
                    security_type_2,
                    position_type
                """
                
                qry_min_max = f"""
                SELECT
                    MIN(mkt_value_portfolio_ccy) AS xmin,
                    MAX(mkt_value_portfolio_ccy) AS xmax
                FROM
                    strategy_allocations
                WHERE
                    cob_date = '{self.cob_date}'
                AND
                    portfolio_id = {self.portfolio_id}
                """
                
            df = db_conn.execute(qry).fetchdf()
            min_max_df = db_conn.execute(qry_min_max).fetchdf()
            
            
            xmin = min_max_df.loc[0, 'xmin']
            xmax = min_max_df.loc[0, 'xmax']
            
            # Pivot so that each security_type_2 is a row and columns are Long / Short
            df_pivot = df.pivot(
                index="security_type_2",
                columns="position_type",
                values="mkt_value_portfolio_ccy"
            ).fillna(0)
            
            self.log(df_pivot)

            # Prepare the data for plotext
            # (each label on the x-axis is a security type, 
            #  and we have separate y-values for Long and Short)
            
            chart_elem = "#strategy_chart"
            title_str = f"{self.cob_date.strftime('%Y-%m-%d')} - {self.portfolio_name} - {self.portfolio_ccy}"
            
            labels = df_pivot.index.astype(str).tolist()
            


            if "Long" not in df_pivot.columns:
                df_pivot["Long"] = 0
            long_values = df_pivot["Long"].tolist()  # may need .fillna(0) if missing
            
            if "Short" not in df_pivot.columns:
                df_pivot["Short"] = 0
            short_values = df_pivot["Short"].tolist()  # may need .fillna(0) if missing
            
            self.log(f"Plotting {self.cob_date} chart with plotext: {(labels)} x {(long_values)} x {(short_values)}")
            if len(labels) == 1:
                return

            
            chart_orientation = "horizontal"
            
            plt_wrapper = self.query_one(chart_elem)
            plt = plt_wrapper.plt
            plt.clear_data()
            plt.clear_figure()
            
            
            try:
                plt.multiple_bar(labels, [long_values, short_values], labels = ['Long', 'Short'])
                                #orientation = chart_orientation, width=1/128)
                
                # set the x-axis range to the min and max values of the data
                #plt.xlim(xmin, xmax)

                plt.title(title_str)
                plt_wrapper.refresh()
                
            except Exception as e:
                self.log(f"Error plotting chart: {e}")
                
            
        elif self.active_tab_id == "crncy_allocations":
            qry = f"""
            SELECT
                security_ccy,
                mkt_value_portfolio_ccy
            FROM
                fx_allocations
            WHERE
                cob_date = '{self.cob_date}'
                {filter}
            ORDER BY
                security_ccy
            """
            
            qry_min_max = f"""
            SELECT
                MIN(mkt_value_portfolio_ccy) AS xmin,
                MAX(mkt_value_portfolio_ccy) AS xmax
            FROM
                fx_allocations
            WHERE
                cob_date = '{self.cob_date}'
                {filter}
            """
            
            df = db_conn.execute(qry).fetchdf()
            min_max_df = db_conn.execute(qry_min_max).fetchdf()
            
            
            xmin = min_max_df.loc[0, 'xmin']
            xmax = min_max_df.loc[0, 'xmax']
            
            # TODO: get the currencies from the database
            qry = f"""
            SELECT DISTINCT
                ccy
            FROM
                currencies
            ORDER BY
                ccy
            """
            ccy_df = db_conn.execute(qry).fetchdf()
            ccy_list = ccy_df['ccy'].tolist()
            
            
            
            df["security_ccy"] = pd.Categorical(df["security_ccy"], 
                                                categories=ccy_list)

            pivot_df = df.pivot_table(
                            columns='security_ccy',
                            values='mkt_value_portfolio_ccy',
                            aggfunc='sum',
                            fill_value=0.0,
                            observed=False).reset_index()
            
            pivot_df = pivot_df.melt(id_vars='index', var_name='security_ccy', value_name='mkt_value_portfolio_ccy')
            
            self.log(pivot_df)
            
            chart_elem = "#crncy_chart"
            title_str = f"{self.cob_date.strftime('%Y-%m-%d')} - {self.portfolio_name} - {self.portfolio_ccy}"
            x_data = pivot_df['security_ccy'].tolist()
            y_data = pivot_df['mkt_value_portfolio_ccy'].tolist()            
            
            chart_orientation = "horizontal"
            
            plt_wrapper = self.query_one(chart_elem)
            plt = plt_wrapper.plt
            plt.clear_data()
            plt.clear_figure()
            
            plt.bar(x_data, y_data, orientation = chart_orientation, width=1/128)
            
            # set the x-axis range to the min and max values of the data
            plt.xlim(xmin, xmax)

            plt.title(title_str)
            plt_wrapper.refresh()
            
        db_conn.close()
        
        
        
        
        
    def on_mount(self) -> None:
        self.update_chart()
        
        
        

    def on_tree_node_selected(self, message: Tree.NodeSelected) -> None:
        self.log(f"Node selected: {message.node.data}")
        
        # if "portfolio_id" not in message.node.data:
        #     return
        
        if "portfolio_id" in message.node.data:
            self.portfolio_id = message.node.data["portfolio_id"]
            self.portfolio_name = message.node.data["portfolio_name"]
            self.portfolio_ccy = message.node.data["portfolio_ccy"]            
        else:
            self.portfolio_id = None
            self.portfolio_name = None
            self.portfolio_ccy = None # TODO: normalize to multi-portfolio currency
            
        
        self.update_chart()
            
            
    def on_key(self, event: Key) -> None:
        if event.key == "comma":
            self.cob_date_idx -= 1
            if self.cob_date_idx < 0:
                self.cob_date_idx = 0

            self.cob_date = self.dates[self.cob_date_idx]
            self.update_chart()
            
        elif event.key == "full_stop":
            self.cob_date_idx += 1
            if self.cob_date_idx >= len(self.dates):
                self.cob_date_idx = len(self.dates) - 1
                
            self.cob_date = self.dates[self.cob_date_idx]                
            self.update_chart()
            
        else:
            self.log(f"Key pressed: {event.key}")

    def on_screen_resume(self) -> None:
        self.cob_date_idx = len(self.dates) - 1
        self.cob_date = self.dates[self.cob_date_idx]
        # self.update_chart()




# "risk" measurement is like driving a car while only looking in the rear view mirror
#  NOTE: this model does not correctly account for optionality or non-linearities
# but still a regulatory requirement for many financial institutions
# class HVaRWidget(Widget):
#     def compose(self) -> ComposeResult:
#         yield Header(id="Header", icon="∂x∂y", show_clock=True)
#         yield Footer(id="Footer")
#         yield PlotextPlot(id="hvar_chart")

#     def on_mount(self) -> None:
#         db_conn = Settings().get_db_connection()
#         qry = """
#         SELECT *
#         FROM
#             hsim_var_view
#         """
#         df = db_conn.execute(qry).fetchdf()
#         db_conn.close()
        
#         plt = self.query_one(PlotextPlot).plt
#         plt.hist(df['hsim_var_1yr'], bins=100, color=80)
#         plt.title("1-yr Non-Parametric VaR [USD]")
