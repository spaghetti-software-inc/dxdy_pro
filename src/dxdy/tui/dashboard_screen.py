# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

from datetime import datetime
from collections import deque
import struct

import zmq

import ntplib

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Vertical
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import ContentSwitcher, DataTable, Digits, Footer, Tree
from textual.events import Key

import plotext
from textual_plotext import PlotextPlot

from textual import events

from ..settings import Settings
from ..db.utils import get_current_cob_date, get_next_cob_date, get_t_plus_one_cob_date
from ..rtd.rtd_calcs import get_rtd_positions, get_ntp_time

from .custom_header import CustomHeaderWidget
from .tui_utils import format_data_table_cell


from loguru import logger


class TotalIntradayPnLWidget(Widget):
    def compose(self) -> ComposeResult:
        yield Digits("", id="total_pnl_digits")

    def update(self, total_pnl: str) -> None:
        self.query_one(Digits).update(total_pnl)


class RealTimeViewerWidget(Widget):
    filter = {'portfolio_name': None, 'security_type_2': None}

    rtd_positions_df = None
    row_key_map = {}
    row_key_ticker_map = {}
    
    cur_cob_date = None
    next_cob_date = None # t0
    
    local_tz = None
    
    t_zero_ns = None
    
    
    def __init__(self, cur_cob_date, next_cob_date, **kwargs):
        super().__init__(**kwargs)
        
        self.config = Settings().get_ui_config_file()['dashboard']
        
        self.rtd_positions_df = None
        self.sub_socket = None
        self.poller = None
        self.portfolio_id = None
        self.total_pnl_widget = None
        self.table = None
        self.row_filter = None
        
        self.deque_maxlen = 10
        self.recent_updates = deque(maxlen=self.deque_maxlen)
        
        #self.cur_cob_date = get_current_cob_date()
        self.cur_cob_date = cur_cob_date
        self.next_cob_date = next_cob_date

        self.rtd_positions_df = get_rtd_positions(self.next_cob_date, self.cur_cob_date)
        self.log(f"{self.next_cob_date}, {self.cur_cob_date}")

        # get the security_id from the ticker
        self.update_row_filter()
        
        self.ntp_client = ntplib.NTPClient()
        self.local_tz = Settings().get_timezone()
    
        try:
            self.ntp_stats = self.ntp_client.request(Settings().get_ntp_server(), version=4)
        except:
            self.ntp_stats = None
        
        self.zmq_context = zmq.Context()
        

    async def on_key(self, event: events.Key) -> None:
        self.log(f"Key pressed: {event.key}")
        
        if event.key == "x":
            self.rtd_positions_df[self.row_filter].to_clipboard(index=False, header=True)
            logger.info("Dashboard data copied to clipboard")
        
    def update_row_filter(self):
        # Filter the dataframe
        if self.filter['portfolio_name'] is not None and self.filter['security_type_2'] is None:
            self.row_filter = self.rtd_positions_df['portfolio_name'] == self.filter['portfolio_name']   
                            
        elif self.filter['portfolio_name'] is not None and self.filter['security_type_2'] is not None:
            self.row_filter = (self.rtd_positions_df['portfolio_name'] == self.filter['portfolio_name']) & (self.rtd_positions_df['security_type_2'] == self.filter['security_type_2'])     
                    
        else:
            self.row_filter = self.rtd_positions_df['portfolio_name'] == self.rtd_positions_df['portfolio_name']   
                    

    def _init_table(self):
        self.log(f"Initializing table with {self.rtd_positions_df[self.row_filter].shape[0]} rows")
        
        self.table.clear(True)
        self.row_key_map = {}
        
        for col_name in self.config['columns']:
            col_display_name = self.config['columns'][col_name]['name']
            self.table.add_column(col_display_name, key=col_name)
            
        for idx, row in self.rtd_positions_df[self.row_filter].iterrows():
            styled_row = []
            for col_name in self.config['columns']:
                col_type = self.config['columns'][col_name]['type']
                if 'style' in self.config['columns'][col_name]:
                    col_style = self.config['columns'][col_name]['style']
                else:
                    col_style = None                
                
                value = row[col_name]
                cell = format_data_table_cell(col_type, value, col_style)
                styled_row.append(cell)
                
            row_key = self.table.add_row(*styled_row)
            self.row_key_map[row['row_num']] = row_key
            self.row_key_ticker_map[row['row_num']] = row['ticker']
        
        
        
    def format_cell(self, col_name: str, value) -> Text:
        col_type = self.config['columns'][col_name]['type']
        if 'style' in self.config['columns'][col_name]:
            col_style = self.config['columns'][col_name]['style']
        else:
            col_style = None
                
        cell = format_data_table_cell(col_type, value, col_style)
        return cell

 
    def refresh_dashboard(self) -> None:
        events = self.poller.poll(timeout=1.0)
        
        if not events:
            return
            #self.log("Poller timeout")
        
        for sock, _ in events:
            if sock == self.sub_socket:
                data = sock.recv()
            else:
                return
            
            local_time_ns, local_dt = get_ntp_time(self.local_tz, self.ntp_stats)
            
            wire_format_str = 'qiddddddddd'
            sentinel_format_str = 'i'
            sentinel_size = struct.calcsize(sentinel_format_str)
            bytes_per_row = struct.calcsize(wire_format_str)

            # Check for the sentinel flag at the start
            sentinel_bytes = data[:sentinel_size]
            sentinel_flag = struct.unpack(sentinel_format_str, sentinel_bytes)[0]

            if sentinel_flag != 0:
                # refresh the table
                self._init_table()
                return

            # Process the rest of the data
            for i in range(sentinel_size, len(data), bytes_per_row):
                chunk = data[i : i + bytes_per_row]
                msg_local_time_ns, row_num, quantity, price, bid, ask, mkt_value, pct_aum, gain_loss, pct_chg, pnl = struct.unpack(wire_format_str, chunk)
                
                # self.log(f"{row_num}, {quantity}, {price}, {bid}, {ask}")

                # TODO: add latency warning if latency > 100 ms               
                latency_ns = local_time_ns - msg_local_time_ns
                #self.log(f"Latency: {latency_ns / 1e6:.2f} ms")
                
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'quantity'] = quantity
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'price'] = price
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'bid'] = bid
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'ask'] = ask
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'mkt_value'] = mkt_value
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'pct_aum'] = pct_aum
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'gain_loss'] = gain_loss
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'pct_chg'] = pct_chg
                self.rtd_positions_df.loc[self.rtd_positions_df['row_num'] == row_num, 'pnl'] = pnl
                
                row_key = self.row_key_map.get(row_num)
                if row_key is None:
                    continue
                
                if len(self.recent_updates) == self.deque_maxlen:            
                    least_recent_row_num = self.recent_updates[0]
                    lr_row_key = self.row_key_map.get(least_recent_row_num)
                    if lr_row_key is not None:
                        lr_ticker = self.format_cell('ticker', self.row_key_ticker_map.get(least_recent_row_num))
                        self.table.update_cell(lr_row_key, 'ticker', lr_ticker, update_width=False)
                    
                self.recent_updates.append(row_num)
                
                ticker = self.format_cell('ticker', self.row_key_ticker_map.get(row_num))
                text_len = len(ticker)
                ticker.stylize("bold magenta", 0, text_len)
                self.table.update_cell(row_key, 'ticker', ticker, update_width=False)
                
                quantity_text = self.format_cell('quantity', quantity)
                self.table.update_cell(row_key, 'quantity', quantity_text, update_width=True)

                price_text = self.format_cell('price', price)
                self.table.update_cell(row_key, 'price', price_text, update_width=True)
                
                bid_text = self.format_cell('bid', bid)
                self.table.update_cell(row_key, 'bid', bid_text, update_width=True)
                
                ask_text = self.format_cell('ask', ask)
                self.table.update_cell(row_key, 'ask', ask_text, update_width=True)

                mkt_value_text = self.format_cell('mkt_value', mkt_value)
                self.table.update_cell(row_key, 'mkt_value', mkt_value_text, update_width=True)
                
                pct_aum_text = self.format_cell('pct_aum', pct_aum)
                self.table.update_cell(row_key, 'pct_aum', pct_aum_text, update_width=True)
                
                gain_loss_text = self.format_cell('gain_loss', gain_loss)
                self.table.update_cell(row_key, 'gain_loss', gain_loss_text, update_width=True)
                
                pct_chg_text = self.format_cell('pct_chg', pct_chg)
                self.table.update_cell(row_key, 'pct_chg', pct_chg_text, update_width=True)
                
                pnl_text = self.format_cell('pnl', pnl)
                self.table.update_cell(row_key, 'pnl', pnl_text, update_width=True)
                
        total_intraday_pnl = self.rtd_positions_df[self.row_filter]['pnl'].sum()
        #self.total_pnl_widget.update(f"{fmt_ccy_amt(total_intraday_pnl)}")
        
        total_intraday_pnl_str = f"{total_intraday_pnl:32,.2f}" 
        
        
        self.total_pnl_widget.update(f"{total_intraday_pnl_str}")
        
        
        
    def refresh_intraday_pnl_chart(self) -> None:
        file_path = Settings().get_intraday_pnl_files_dir() / f'intraday_pnl_{self.cur_cob_date}_{self.portfolio_id}.bin'
            
        # ensure file is created
        #assert file_path.exists()
        if not file_path.exists():
            return
            
        timestamps = []
        timestamp_ticks = []
        total_pnls = []           
        with open(file_path, 'rb') as f:
            data = f.read()
            
            # wire format means the binary data format that is written to the file
            # iqd = int, long, double
            # means 4 bytes, 8 bytes, 8 bytes
            for i in range(0, len(data), struct.calcsize('iqd')):
                chunk = data[i : i + struct.calcsize('iqd')]
                portfolio_id, timestamp, total_pnl = struct.unpack('iqd', chunk)
                if portfolio_id != self.portfolio_id:
                    continue
                
                plt_timestamp_ns = timestamp - self.t_zero_ns
                plt_timestamp_hrs = plt_timestamp_ns / 1e9 / 3600
                
                
                if plt_timestamp_hrs > 12:
                    am_pm = 'PM'
                else:
                    am_pm = 'AM'
                
                # # get the fractional part of plt_timestamp_hrs
                plt_timestamps_frac = plt_timestamp_hrs % 1
                
                plt_timestamps_mins = round(plt_timestamps_frac * 60)                
                
                # truncate the fractional part of plt_timestamp_hrs in 12-hour format
                plt_timestamp_hrs_int = int(plt_timestamp_hrs) % 12
                            
                #plt_timestamp_tick = 'HH:MM:SS'
                plt_timestamp_tick = str(plt_timestamp_hrs_int) + ':' + str(plt_timestamps_mins).zfill(2) + am_pm
             
                timestamps.append(plt_timestamp_hrs)
                timestamp_ticks.append(plt_timestamp_tick)
                total_pnls.append(total_pnl)
        
        
        plt_wrapper = self.query_one(PlotextPlot)
        plt = plt_wrapper.plt
        plt.clear_data()
        plt.clear_figure()
        #dates = plotext.datetimes_to_string(timestamps)
        plt.xticks(timestamps, timestamp_ticks)
        plt.plot(timestamps, total_pnls, marker='braille')
        plt.title("Intraday P&L")
        plt_wrapper.refresh()       
            
    def automatic_refresh(self):
        if self.query_one(ContentSwitcher).current == "dashboard":
            self.refresh_dashboard()
            #self.run_worker(self.refresh_dashboard(), exclusive=True)
                   
        elif self.query_one(ContentSwitcher).current == "intraday_pnl_chart":
            self.refresh_intraday_pnl_chart()
        
        super().automatic_refresh()



    def compose(self) -> ComposeResult:
        self.table = DataTable(id="dashboard", cursor_type="row")
        self.table.fixed_columns = 2
        
        tree: Tree[dict] = Tree("Dashboard", id="reports_selector", classes="box1", data={"type": "root"})
        tree.root.expand()
        
        tree.ICON_NODE = "📡  "
        tree.ICON_NODE_EXPANDED = "📡  "
        
        db_conn = Settings().get_db_connection()
        qry = """
        SELECT *
        FROM
            portfolios
        """
        df = db_conn.execute(qry).fetchdf()
        
        for row in df.itertuples():
            portfolio = tree.root.add(row.portfolio_name, expand=True, 
                                      data={"type": "portfolio", "portfolio_id": row.portfolio_id, "portfolio_name": row.portfolio_name})
            
            portfolio.add_leaf("Stocks", 
                               data={"type": "stocks_node", "portfolio_id": row.portfolio_id, "portfolio_name": row.portfolio_name})
            
            portfolio.add_leaf("Options", 
                               data={"type": "options_node", "portfolio_id": row.portfolio_id, "portfolio_name": row.portfolio_name})

            portfolio.add_leaf("P&L Chart", 
                               data={"type": "chart_node", "portfolio_id": row.portfolio_id, "portfolio_name": row.portfolio_name})

                
        db_conn.close()
        
        
        self.total_pnl_widget = TotalIntradayPnLWidget()
        
        yield tree
        with Vertical():
            yield self.total_pnl_widget
            with ContentSwitcher(initial="dashboard", id="content_switcher", classes="right_dock"):
                yield self.table
                yield PlotextPlot(id="intraday_pnl_chart")

    def on_mount(self) -> None:
        self.sub_socket = self.zmq_context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        self.sub_socket.setsockopt(zmq.CONFLATE, 1)
        self.sub_socket.connect(Settings().get_realtime_calculation_tcp_socket())
        
        self.poller = zmq.Poller()
        self.poller.register(self.sub_socket, zmq.POLLIN)
        
        self._init_table()
        
        next_cob_date = get_next_cob_date() # 0:00:00
        next_cob_date_str = next_cob_date.strftime('%Y-%m-%d %H:%M:%S')
        next_cob_datetime = datetime.strptime(next_cob_date_str, '%Y-%m-%d %H:%M:%S')
        self.t_zero_ns = next_cob_datetime.timestamp() * 1e9
        
        self.auto_refresh = 0.1

    def on_unmount(self) -> None:
        pass


    def on_tree_node_selected(self, message: Tree.NodeSelected) -> None:
        self.log(f"Node selected: {message.node}")
        
        if message.node.data["type"] == "chart_node":
            self.portfolio_id = message.node.data["portfolio_id"]            
            self.refresh_intraday_pnl_chart()            
            self.query_one(ContentSwitcher).current = "intraday_pnl_chart"
            return
        
        else:
            self.query_one(ContentSwitcher).current = "dashboard"
                
            if message.node.data["type"] == "root":
                self.filter = {'portfolio_name': None, 'security_type_2': None}    
                
                if not message.node.is_expanded:
                    message.node.expand()
                                
            elif message.node.data["type"] == "portfolio":
                self.filter = {'portfolio_name': message.node.data["portfolio_name"], 'security_type_2': None}
                
                if not message.node.is_expanded:
                    message.node.expand()
                
            elif message.node.data["type"] == "stocks_node":
                self.filter = {'portfolio_name': message.node.data["portfolio_name"], 'security_type_2': 'Common Stock'}
                
            elif message.node.data["type"] == "options_node":
                self.filter = {'portfolio_name': message.node.data["portfolio_name"], 'security_type_2': 'Option'}
            
            else:
                self.warn(f"Unknown node type: {message.node.data['type']}")
                return
            
            self.update_row_filter()            
            self._init_table()

class DashboardScreen(Screen):
    

    def compose(self) -> ComposeResult:
        cur_cob_date = get_current_cob_date()
        cur_trading_date = get_next_cob_date()
        next_cob_date = get_t_plus_one_cob_date()
        yield CustomHeaderWidget(cur_cob_date, cur_trading_date, next_cob_date, id="CustomHeaderWidget")
        real_time_viewer = RealTimeViewerWidget(cur_cob_date, cur_trading_date)
        yield real_time_viewer
        yield Footer(id="Footer")
        