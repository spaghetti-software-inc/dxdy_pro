# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)
# (R)eal (T)ime (D)ata (RTD) Calculation Server 


import sys
from multiprocessing import Process

import struct
import duckdb
import pandas as pd
from tabulate import tabulate

import ntplib
import time
from datetime import datetime, date, timedelta
from zoneinfo import ZoneInfo

import zmq


# disable default logger
from loguru import logger


import rich
from rich.traceback import install
install(show_locals=True)


from rich import box
from rich.console import Console
from rich.live import Live
from rich.table import Table
from rich.text import Text
from rich.layout import Layout
from rich.panel import Panel

if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

from dxdy.db.market_data import MarketDataApi, MarketDataApiFactory
from dxdy.settings import Settings
import dxdy.db.utils as db_utils
from dxdy.email.reports import send_intraday_pnl_report
from dxdy.eod.tasks import task_load_intraday_transactions_data

API_SELECTION = "bbg"
#API_SELECTION = "spgi"


API : MarketDataApi = MarketDataApiFactory().get_api(API_SELECTION)
CID = API.securities_identifier()

###################################################
#from dxdy.bbg.api import real_time_api
#from dxdy.quant.api import real_time_api
###################################################

# def compute_positions_asof_date(conn, asof_date):
#     """
#     Computes the final EOD snapshot of positions for each (portfolio_id, security_id)
#     as of the given 'asof_date'. 
#     Returns a DataFrame with one row per portfolio/security final position state:
#         portfolio_id, security_id, quantity, avg_cost, realized_pnl_to_date
#     """


#     # ----------------------------------------------------------------------
#     # Step 1: Pull trades UP TO asof_date
#     # ----------------------------------------------------------------------
#     query = f"""
#         SELECT 
#             trade_id,
#             portfolio_id,
#             security_id,
#             trade_date,
#             quantity,
#             price,
#             commission
#         FROM 
#             trades
#         WHERE 
#             trade_date <= '{asof_date}'
#         ORDER BY 
#             portfolio_id, security_id, trade_date, trade_id
#     """
#     df_trades = conn.execute(query).fetch_df()

#     if df_trades.empty:
#         # No trades up to asof_date => return empty DataFrame
#         return pd.DataFrame(columns=[
#             'portfolio_id', 'security_id', 'quantity',
#             'avg_cost', 'realized_pnl_to_date'
#         ])

#     # ----------------------------------------------------------------------
#     # Step 2: Define a helper to compute final position for a group
#     # ----------------------------------------------------------------------
#     def compute_final_position_for_group(df):
#         """
#         df: trades for one (portfolio_id, security_id) in ascending date/trade_id order
#         We'll do Weighted Avg Cost. We assume:
#           - 'quantity' is positive for buys, negative for sells
#           - crossing zero fully realizes PnL 
#           - commissions reduce realized PnL if closing, or are capitalized if opening
#         Returns a single dict with final quantity, avg_cost, realized_pnl, etc.
#         """
#         current_qty = 0.0
#         current_avg_cost = 0.0
#         realized_pnl = 0.0

#         for _, row in df.iterrows():
#             qty_change = row['quantity']
#             trade_price = row['price']
#             commission = row['commission']

#             old_qty = current_qty
#             old_cost = current_avg_cost
#             new_qty = old_qty + qty_change

#             # Check if crossing zero
#             if old_qty * new_qty < 0:
#                 # crossing from long to short or short to long in one trade
#                 closed_qty = -old_qty  # fully close old_qty
#                 # Realized portion
#                 realized_pnl += closed_qty * (trade_price - old_cost)
#                 # Subtract commission from realized PnL
#                 realized_pnl -= commission

#                 # Remainder = new_qty after fully closing old
#                 remainder = qty_change + old_qty  # e.g. -5 if we had 10 and sold 15
#                 current_qty = remainder
#                 # new position cost basis
#                 current_avg_cost = trade_price if remainder != 0 else 0.0

#             else:
#                 # same side (increasing or decreasing but not crossing zero)
#                 if old_qty == 0:
#                     # opening from zero
#                     current_qty = new_qty
#                     current_avg_cost = trade_price
#                     # Optionally capitalize commission into avg cost
#                     # (common if it's an "opening trade")
#                     if current_qty != 0:
#                         current_avg_cost = ((current_avg_cost * abs(current_qty)) + commission) / abs(current_qty)

#                 elif (old_qty * new_qty) > 0:
#                     # partial close or add
#                     if abs(new_qty) > abs(old_qty):
#                         # net add to position => recalc weighted avg cost
#                         total_old_cost = old_qty * old_cost
#                         total_new_cost = qty_change * trade_price
#                         # If you prefer to add commission to new cost:
#                         total_new_cost += commission
#                         current_avg_cost = (total_old_cost + total_new_cost) / new_qty
#                         current_qty = new_qty
#                     else:
#                         # partial close (realize PnL on the closed portion)
#                         closed_qty = old_qty - new_qty  # e.g. close 4 if old=10,new=6
#                         realized_pnl += closed_qty * (trade_price - old_cost)
#                         realized_pnl -= commission  # subtract commission from realized
#                         current_qty = new_qty
#                         # cost basis stays the same if partial close
#                 else:
#                     # new_qty == 0 => fully closed
#                     closed_qty = old_qty
#                     realized_pnl += closed_qty * (trade_price - old_cost)
#                     realized_pnl -= commission
#                     current_qty = 0.0
#                     current_avg_cost = 0.0

#         return {
#             'quantity': current_qty,
#             'avg_cost': current_avg_cost,
#             'realized_pnl_to_date': realized_pnl
#         }

#     # ----------------------------------------------------------------------
#     # Step 3: Group by (portfolio_id, security_id), keep final position only
#     # ----------------------------------------------------------------------
#     results = []
#     grouped = df_trades.groupby(['portfolio_id', 'security_id'], group_keys=True)
#     for (pid, sid), group_df in grouped:
#         pos_dict = compute_final_position_for_group(group_df)
#         pos_dict['portfolio_id'] = pid
#         pos_dict['security_id'] = sid
#         results.append(pos_dict)

#     df_positions_asof = pd.DataFrame(results, columns=[
#         'portfolio_id', 'security_id', 'quantity',
#         'avg_cost', 'realized_pnl_to_date'
#     ])

#     return df_positions_asof



def get_rtd_positions(cur_cob_date, mkt_cob_date) -> pd.DataFrame:
    db_conn = Settings().get_db_connection()
    
  

    sql_query = f"""
            SELECT 
                ROW_NUMBER() OVER() as row_num,
                psn.portfolio_id,
                portfolio_name,
                cash_balances.latest_cash_balance,
                s.security_id,
                s.figi,
                --------------------------------------------------------
                --s.ticker AS ticker,

                CASE
                    WHEN s.security_type_2 = 'Option' THEN s.security_description
                    ELSE s.ticker
                END AS  ticker,

                --------------------------------------------------------
                s.exch_code,
                s.name,
                s.ccy,
                fx2.fx_rate / fx1.fx_rate AS fx_rate,
                s.security_type_2,
                --security_description AS display_ticker,
                net_quantity AS quantity,
                COALESCE(o.shares_per_contract, 1) AS multiplier,
                psn.close_price AS close_price,
                psn.avg_cost,
                o.contract_type,
                o.expiration_date,
                CAST(NULL AS DOUBLE) AS price,
                CAST(NULL AS DOUBLE) AS bid,
                CAST(NULL AS DOUBLE) AS ask,
                CAST(NULL AS DOUBLE) AS mkt_value,
                CAST(NULL AS DOUBLE) AS pct_aum,
                CAST(NULL AS DOUBLE) AS gain_loss,
                CAST(NULL AS DOUBLE) AS chg,
                CAST(NULL AS DOUBLE) AS pct_chg,
                CAST(NULL AS DOUBLE) AS pnl
            FROM 
                (SELECT * FROM daily_positions WHERE cob_date = '{cur_cob_date}') AS psn
            LEFT JOIN
                latest_cash_balance_view cash_balances
            ON
                cash_balances.portfolio_id = psn.portfolio_id
            LEFT JOIN
                portfolios 
            ON 
                portfolios.portfolio_id = psn.portfolio_id
            LEFT JOIN
                securities s
            ON 
                s.security_id = psn.security_id
            LEFT JOIN
                options o
            ON
                o.security_id = psn.security_id
            --LEFT JOIN
            --    market_data m
            --ON 
            --    m.security_id = psn.security_id
            --AND 
            --    m.trade_date = '{mkt_cob_date}'
            LEFT JOIN
                fx_rates_data fx1
            ON
                fx1.ccy = portfolios.portfolio_ccy
            AND
                fx1.fx_date = '{mkt_cob_date}'
            LEFT JOIN
                fx_rates_data fx2
            ON
                fx2.ccy = s.ccy
            AND
                fx2.fx_date = '{mkt_cob_date}'
            WHERE
                psn.net_quantity != 0
            AND
                (o.expiration_date >= '{cur_cob_date}') OR (o.expiration_date IS NULL)
            ORDER BY
                portfolio_name,
                s.security_type_2 ASC,
                s.base_ticker
            """
    positions_df = db_conn.execute(sql_query).fetchdf()
    
    
    # cost_basis = compute_positions_asof_date(db_conn, cur_cob_date)
            
    db_conn.close()
    
    # positions_df = pd.merge(positions_df, 
    #                         cost_basis[['portfolio_id', 'security_id', 'avg_cost', 'realized_pnl_to_date']], 
    #                         on=['portfolio_id', 'security_id'], how='left')
        
    positions_df['timestamp'] = 0       
    positions_df['quote_timestamp'] = datetime.now().astimezone(ZoneInfo("America/New_York"))
    positions_df['delay'] = float('nan')
    positions_df['price'] = float('nan')
        
    positions_df['mkt_value'] = positions_df['close_price'] \
                              * positions_df['quantity'] \
                              * positions_df['multiplier'] \
                              * positions_df['fx_rate']
    positions_df['chg'] = 0.0
    positions_df['pct_chg'] = 0.0
    positions_df['pnl'] = 0.0
    
    
    
    return positions_df    

def get_ntp_time(local_tz, ntp_stats):
        if ntp_stats is None:
            current_time_ns = time.time_ns() 
            local_dt = datetime.now()
            return current_time_ns, local_dt
        
        #delay = self.ntp_stats.delay
        offset = ntp_stats.offset
        current_time_ns = time.time_ns()
        corrected_time_ns = current_time_ns + int(offset * 1e9)
        utc_ntp_corrected_time = datetime.fromtimestamp(corrected_time_ns / 1e9, tz=ZoneInfo("UTC"))
        # precision = self.ntp_stats.precision
        # root_delay = self.ntp_stats.root_delay
        # root_dispersion = self.ntp_stats.root_dispersion
        # epsilon = 2 ** precision
        # total_error = (delay / 2) + (root_delay / 2) + root_dispersion + epsilon
        local_dt = utc_ntp_corrected_time.astimezone(local_tz)
        return corrected_time_ns, local_dt


def send_intraday_email(positions_df):
    logger.debug("sending intraday email")
    send_intraday_pnl_report(positions_df)
        
class RtdCalcServer:
    # Constants and configurations
    ntp_client = ntplib.NTPClient()

    ZMQ_PUB = None
    zmq_context = None

    cur_cob_date = None
    next_cob_date = None
    
    # ticks_per_second = 0
    
    #INTRADAY_FILE = Settings().get_intrady_pnl_files_dir() / f'intraday_pnl_{cur_cob_date}.bin'
    
    intraday_files = {}
        
    def __init__(self):
        
        self.ntp_stats = None
        self.context = None
        self.pub_socket = None
        self.positions_df = None
        self.tickers = None
        
        self.ZMQ_PUB = Settings().get_realtime_calculation_tcp_socket()
        self.db_file = Settings()._get_db_file()
        self.NTP_SERVER = Settings().get_ntp_server()
        self.local_tz = Settings().get_timezone()
        self.cur_cob_date = db_utils.get_current_cob_date()
        self.next_cob_date = db_utils.get_next_cob_date()


    def check_intraday_fills(self):
        pass

    def main(self):
        self.context = zmq.Context()
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.setsockopt(zmq.SNDHWM, 1)    # only keep the last message in the buffer
        self.pub_socket.setsockopt(zmq.CONFLATE, 1)  # only keep the last message to ensure real-time data streaming

        self.pub_socket.bind(self.ZMQ_PUB)
        
        task_load_intraday_transactions_data(self.next_cob_date, self.cur_cob_date)
        self.positions_df = get_rtd_positions(self.next_cob_date, self.cur_cob_date)
        
        
        self.tickers = self.positions_df.groupby(['ticker'])['close_price'].mean()
        
        
        if len(self.tickers) == 0:
            raise ValueError("No tickers found in the test data")
        
    
        
        portfolio_ids = self.positions_df['portfolio_id'].unique()
        for portfolio_id in portfolio_ids:
            file_path = Settings().get_intraday_pnl_files_dir() / f'intraday_pnl_{self.cur_cob_date}_{portfolio_id}.bin'
            self.intraday_files[portfolio_id] = open(file_path, 'ab')
        
        
        self.ntp_stats = self.ntp_client.request(self.NTP_SERVER, version=4)
        local_time_ns_prev, local_time_prev = get_ntp_time(self.local_tz, self.ntp_stats)
        
        intraday_chart_timer_ns_prev = local_time_ns_prev
        intraday_email_timer_ns_prev = local_time_ns_prev
        intraday_fills_timer_ns_prev = local_time_ns_prev
        
        # num_quotes = 0
        start_time_ns = local_time_ns_prev
        
        is_start_iteration = True

        rich.print("[cyan]Real-time data calculation server starting")
        icnt = 0
        while icnt < 1:
            print(".")
            time.sleep(1)
            icnt += 1
        print("\n")

        ####################################  third-party API here ################################### 
        req = self.positions_df        
        rt_api = API.real_time_api(req)
        ##############################################################################################
        
        #logger.info(f"subscribing to real-time data stream: {req}")
        

        while(True):
            local_time_ns, local_time = get_ntp_time(self.local_tz, self.ntp_stats)
            self.positions_df['timestamp'] = local_time
            
            
            ################################################################################################

            # try:
            cid, last_price, bid_price, ask_price = next(rt_api)
            ticker_mask = self.positions_df[CID] == cid

            # except StopIteration:
            #     logger.debug("real-time data stream ended")
            #     #exit(1)
            #     time.sleep(1)
            #     continue

            if cid is None:
                logger.debug("real-time data stream ended")
                continue

            ################################################################################################
            
            

            if not ticker_mask.any():
                continue
            

            if last_price is None:
                continue

            self.positions_df.loc[ticker_mask, 'price'] = last_price
            self.positions_df.loc[ticker_mask, 'bid'] = bid_price
            self.positions_df.loc[ticker_mask, 'ask'] = ask_price
                
            local_time_ns, local_time = get_ntp_time(self.local_tz, self.ntp_stats)
            
            
            quote_timestamp_ns = local_time_ns
            quote_timestamp = local_time
                
                
            delay_ns = local_time_ns - quote_timestamp_ns
                    
            self.positions_df.loc[ticker_mask, 'quote_timestamp'] = quote_timestamp
            self.positions_df.loc[ticker_mask, 'delay'] = delay_ns * 1e-9
    
            price = self.positions_df.loc[ticker_mask, 'price'].values
            close_price = self.positions_df.loc[ticker_mask, 'close_price'].values
            quantity = self.positions_df.loc[ticker_mask, 'quantity'].values
            multiplier = self.positions_df.loc[ticker_mask, 'multiplier'].values
            aum = self.positions_df.loc[ticker_mask, 'latest_cash_balance'].values
            avg_cost = self.positions_df.loc[ticker_mask, 'avg_cost'].values
            fx_rate = self.positions_df.loc[ticker_mask, 'fx_rate'].values
            
            mkt_value = price * quantity * multiplier * fx_rate
            chg = price - close_price
            pct_chg = price / close_price - 1
            pnl = (chg * quantity) * multiplier * fx_rate
                
            self.positions_df.loc[ticker_mask, 'mkt_value'] = mkt_value
            self.positions_df.loc[ticker_mask, 'chg'] = chg
            self.positions_df.loc[ticker_mask, 'pct_chg'] = pct_chg
            self.positions_df.loc[ticker_mask, 'pnl'] = pnl

            self.positions_df.loc[ticker_mask, 'pct_aum'] = mkt_value / aum
            self.positions_df.loc[ticker_mask, 'gain_loss'] = mkt_value - (avg_cost * quantity)
            
            #rich.print(self.positions_df[['ticker','quantity','price','mkt_value','chg','pct_chg','pnl']])

            buffer = []
            sentinel_flag = struct.pack('i', 0)  # Set the sentinel flag to 1
            buffer.append(sentinel_flag)

            for index, row in self.positions_df[ticker_mask].iterrows():
                row_num = row['row_num']
                quantity = row['quantity']
                price = row['price']
                bid = row['bid']
                ask = row['ask']
                mkt_value = row['mkt_value']
                pct_aum = row['pct_aum']
                gain_loss = row['gain_loss']
                pct_chg = row['pct_chg']
                pnl = row['pnl']
                
                wire_format_str = 'qiddddddddd'
                data_bytes = struct.pack(wire_format_str, local_time_ns, row_num, quantity, price, bid, ask, mkt_value, pct_aum, gain_loss, pct_chg, pnl)
                buffer.append(data_bytes)
                
            binary_wire_format_data = b''.join(buffer)
            #self.pub_socket.send(binary_wire_format_data)
            self.pub_socket.send(binary_wire_format_data)
            
            logger.debug(f"\n{self.positions_df.loc[ticker_mask, ['ticker','quantity','price', 'bid', 'ask', 'mkt_value', 'pct_aum', 'gain_loss', 'chg','pct_chg','pnl']]}")
            
            # num_quotes += 1
            # self.ticks_per_second = round(num_quotes / (local_time_ns - start_time_ns) * 1e9)
            
                
            # save intraday pnl file every second
            delta_time_ns = local_time_ns - intraday_chart_timer_ns_prev
            if delta_time_ns > 1e9:
                for portfolio_id in portfolio_ids:
                    total_pnl = self.positions_df.loc[self.positions_df['portfolio_id'] == portfolio_id, 'pnl'].sum()
                        
                    data_bytes = struct.pack('iqd', portfolio_id, local_time_ns, total_pnl)
                    self.intraday_files[portfolio_id].write(data_bytes)
                    self.intraday_files[portfolio_id].flush()
                        
                    intraday_chart_timer_ns_prev = local_time_ns

            # check for intraday fills every X seconds
            delta_time_ns = local_time_ns - intraday_fills_timer_ns_prev
            if is_start_iteration or delta_time_ns * 1e-9 > 30:
                # check for intraday fills
                #self.check_intraday_fills()
                #self.cur_cob_date = db_utils.get_current_cob_date()
                
                task_load_intraday_transactions_data(self.next_cob_date, self.cur_cob_date)


                self.positions_df = get_rtd_positions(self.next_cob_date, self.cur_cob_date)

                logger.info("Loaded intraday transactions data")
                
                intraday_fills_timer_ns_prev = local_time_ns
                is_start_iteration = False
                
                # send a sentinel flag to indicate the end of the intraday fills
                sentinel_flag = struct.pack('i', 1)  # Set the sentinel flag to 1
                self.pub_socket.send(sentinel_flag)

            # send email every 30 minutes
            delta_time_ns = local_time_ns - intraday_email_timer_ns_prev
            if is_start_iteration or delta_time_ns * 1e-9 > 1800:
                proc = Process(target=send_intraday_email,  args = (self.positions_df.copy(),))
                proc.start()
                    
                
                intraday_email_timer_ns_prev = local_time_ns
                is_start_iteration = False
                
            

            #self.recent_updates.append([local_time, cid])
            # live.console.print(f"recent updates: {self.recent_updates}")
            
            #live.update(self.refresh_display())
    
    def run(self):
        try:
            self.main()
            
        except KeyboardInterrupt:
            logger.info("Real-time data calculation server exiting")
            
        finally:
            for file in self.intraday_files.values():
                file.close()
            self.intraday_files.clear()
            self.pub_socket.close()
            self.context.term()