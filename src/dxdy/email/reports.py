from enum import Enum
import random
from datetime import date

import pandas as pd
from tabulate import tabulate

from smtplib import SMTP_SSL as SMTP       # this invokes the secure SMTP protocol (port 465, uses SSL)

from email.message import EmailMessage
from email.utils import make_msgid

import dxdy.db.utils as db_utils
from dxdy.settings import Settings
from dxdy.saas_settings import SaaSConfig


saas_config = SaaSConfig()

from loguru import logger


##################################################################################

def compute_positions_asof_date(conn, asof_date):
    """
    Computes the final EOD snapshot of positions for each (portfolio_id, security_id)
    as of the given 'asof_date'. 
    Returns a DataFrame with one row per portfolio/security final position state:
        portfolio_id, security_id, quantity, avg_cost, realized_pnl_to_date
    """


    # ----------------------------------------------------------------------
    # Step 1: Pull trades UP TO asof_date
    # ----------------------------------------------------------------------
    query = f"""
        SELECT 
            trade_id,
            portfolio_id,
            security_id,
            trade_date,
            quantity,
            price,
            commission
        FROM 
            trades
        WHERE 
            trade_date <= '{asof_date}'
        ORDER BY 
            portfolio_id, security_id, trade_date, trade_id
    """
    df_trades = conn.execute(query).fetch_df()

    if df_trades.empty:
        # No trades up to asof_date => return empty DataFrame
        return pd.DataFrame(columns=[
            'portfolio_id', 'security_id', 'quantity',
            'avg_cost', 'realized_pnl_to_date'
        ])

    # ----------------------------------------------------------------------
    # Step 2: Define a helper to compute final position for a group
    # ----------------------------------------------------------------------
    def compute_final_position_for_group(df):
        """
        df: trades for one (portfolio_id, security_id) in ascending date/trade_id order
        We'll do Weighted Avg Cost. We assume:
          - 'quantity' is positive for buys, negative for sells
          - crossing zero fully realizes PnL 
          - commissions reduce realized PnL if closing, or are capitalized if opening
        Returns a single dict with final quantity, avg_cost, realized_pnl, etc.
        """
        current_qty = 0.0
        current_avg_cost = 0.0
        realized_pnl = 0.0

        for _, row in df.iterrows():
            qty_change = row['quantity']
            trade_price = row['price']
            commission = row['commission']

            old_qty = current_qty
            old_cost = current_avg_cost
            new_qty = old_qty + qty_change

            # Check if crossing zero
            if old_qty * new_qty < 0:
                # crossing from long to short or short to long in one trade
                closed_qty = -old_qty  # fully close old_qty
                # Realized portion
                realized_pnl += closed_qty * (trade_price - old_cost)
                # Subtract commission from realized PnL
                realized_pnl -= commission

                # Remainder = new_qty after fully closing old
                remainder = qty_change + old_qty  # e.g. -5 if we had 10 and sold 15
                current_qty = remainder
                # new position cost basis
                current_avg_cost = trade_price if remainder != 0 else 0.0

            else:
                # same side (increasing or decreasing but not crossing zero)
                if old_qty == 0:
                    # opening from zero
                    current_qty = new_qty
                    current_avg_cost = trade_price
                    # Optionally capitalize commission into avg cost
                    # (common if it's an "opening trade")
                    if current_qty != 0:
                        current_avg_cost = ((current_avg_cost * abs(current_qty)) + commission) / abs(current_qty)

                elif (old_qty * new_qty) > 0:
                    # partial close or add
                    if abs(new_qty) > abs(old_qty):
                        # net add to position => recalc weighted avg cost
                        total_old_cost = old_qty * old_cost
                        total_new_cost = qty_change * trade_price
                        # If you prefer to add commission to new cost:
                        total_new_cost += commission
                        current_avg_cost = (total_old_cost + total_new_cost) / new_qty
                        current_qty = new_qty
                    else:
                        # partial close (realize PnL on the closed portion)
                        closed_qty = old_qty - new_qty  # e.g. close 4 if old=10,new=6
                        realized_pnl += closed_qty * (trade_price - old_cost)
                        realized_pnl -= commission  # subtract commission from realized
                        current_qty = new_qty
                        # cost basis stays the same if partial close
                else:
                    # new_qty == 0 => fully closed
                    closed_qty = old_qty
                    realized_pnl += closed_qty * (trade_price - old_cost)
                    realized_pnl -= commission
                    current_qty = 0.0
                    current_avg_cost = 0.0

        return {
            'quantity': current_qty,
            'avg_cost': current_avg_cost,
            'realized_pnl_to_date': realized_pnl
        }

    # ----------------------------------------------------------------------
    # Step 3: Group by (portfolio_id, security_id), keep final position only
    # ----------------------------------------------------------------------
    results = []
    grouped = df_trades.groupby(['portfolio_id', 'security_id'], group_keys=True)
    for (pid, sid), group_df in grouped:
        pos_dict = compute_final_position_for_group(group_df)
        pos_dict['portfolio_id'] = pid
        pos_dict['security_id'] = sid
        results.append(pos_dict)

    df_positions_asof = pd.DataFrame(results, columns=[
        'portfolio_id', 'security_id', 'quantity',
        'avg_cost', 'realized_pnl_to_date'
    ])

    return df_positions_asof
##################################################################################


def send_email_report(subject:str, content:str, dataframes: list[pd.DataFrame] = None):
        SMTPserver = saas_config.get_smtp_server()
        sender =     saas_config.get_smtp_username()
        to = saas_config.get_smtp_recipients()

        USERNAME = saas_config.get_smtp_username()
        PASSWORD = saas_config.get_smtp_password()

        
        random_int = random.randint(100000, 999999)
        image_cid = make_msgid(domain=f'{random_int}@softwarespaghetti.com')
        image_cid=image_cid[1:-1]
        
        html_content = f"""
            <html>
                <body>
                    <div>
                        {content}
                    </div> 
                    <br>
                    <br>
                    <img src="cid:{image_cid}" alt="dxdy">
                </body>
            </html>
        """
        
        
        msg = EmailMessage()
        
        msg.set_content(content + '\n\n\n\n' + 'dxdy v1.0')
        msg.add_alternative(html_content, subtype='html')
        
        with open(Settings().get_project_root() / 'dxdy_logo.png', 'rb') as img:
            maintype, subtype = 'image', 'png'
            msg.get_payload()[1].add_related(img.read(), 
                                                 maintype=maintype, 
                                                 subtype=subtype, 
                                                 cid=image_cid)

        if dataframes:
            for i, df in enumerate(dataframes, start=1):
                # Convert the DataFrame to CSV format (without the index)
                csv_content = df.to_csv(index=False)
                # Convert the CSV string to bytes (UTF-8 encoding)
                csv_bytes = csv_content.encode('utf-8')
                
                # Attach the CSV file to the email with a default filename.
                # msg.add_attachment(
                #     csv_bytes,
                #     maintype='text',
                #     subtype='csv',
                #     filename=f'report_{i}.csv'
                # )
            
        #msg = MIMEText(html_content, text_subtype)
        msg['Subject']= subject
        msg['From']   = sender

        conn = SMTP(SMTPserver)
        conn.set_debuglevel(False)
        conn.login(USERNAME, PASSWORD)
        
        conn.sendmail(sender, to, msg.as_string())
        
        conn.quit()
        
        logger.info(f"Email sent to {to} with subject {subject}")


def send_intraday_pnl_report(positions_df):

    positions_df = positions_df[['portfolio_name', 'ticker', 'quantity', 'chg', 'pct_chg', 'pnl']]
    summary_df = positions_df.groupby('portfolio_name').agg({'pnl':'sum'}).reset_index()

    subject = 'Intraday P&L Report'
    
    positions_table = tabulate(positions_df, 
                       headers=['Portfolio', 'Ticker', 'Quantity', 'Change', '% Change', 'P&L'],
                       tablefmt='html', 
                       numalign='right', 
                       floatfmt=',.2f', intfmt=',',
                       showindex=False)

    summary_table = tabulate(summary_df, 
                       headers=['Portfolio', 'Total P&L'],
                       tablefmt='html',
                       numalign='right', 
                       floatfmt=',.2f', intfmt=',',
                       showindex=False)
    
    content = f"""{summary_table}\n\n{positions_table}"""
    
    
    send_email_report(subject, content)


class ReportFormat(Enum):
     HTML = 1,
     MARKDOWN = 2,


def gen_report_heading(text : str, level : int, fmt : ReportFormat) -> str:
    if fmt == ReportFormat.HTML:
        return f"<h{level}>{text}</h{level}>\n"
    
    elif fmt == ReportFormat.MARKDOWN:
        return f"{'#' * level} {text}\n"
    else:
        raise ValueError(f"Unsupported format {fmt}")
    

def get_pnl_pivot(db, measure : str, portfolio_id : int, cob_date : date) -> pd.DataFrame:
        cob_date_str = cob_date.strftime('%Y-%m-%d')
        
        qry = f"""
            PIVOT
                (SELECT
                    cob_date,
                    SUM(pl.{measure}) AS {measure},
                    s.security_type_2,
                    CASE WHEN quantity >= 0 THEN 'Long' ELSE 'Short' END AS position_type,
                FROM
                    security_level_pnl pl
                LEFT JOIN
                    securities s
                ON
                    s.security_id = pl.security_id
                WHERE
                    cob_date = '{cob_date_str}'
                AND
                    portfolio_id = {portfolio_id}
                GROUP BY
                    cob_date,
                    s.security_type_2,
                    CASE WHEN quantity >= 0 THEN 'Long' ELSE 'Short' END) agg
            ON
                position_type
            USING
                SUM({measure})
            ORDER BY
                cob_date DESC,
                security_type_2
            """
        
        df = db.execute(qry).fetchdf()
        
        if not 'Long' in df.columns:
            df['Long'] = 0
        if not 'Short' in df.columns:
            df['Short'] = 0
       
        df['Total'] = df['Long'] + df['Short']
        
        with db_utils.DuckDBTemporaryTable(db, 'tmp_pivot', df) as tmp_table_name:
            qry = f"""
                SELECT
                    cob_date,
                    'Total' AS security_type_2,
                    SUM(Long) AS Long,
                    SUM(Short) AS Short,
                    SUM(Total) AS Total
                FROM
                    tmp_pivot
                GROUP BY
                    cob_date
                """
            df_total = db.execute(qry).fetchdf()
        
        # concatenate the tables
        df = pd.concat([df, df_total])       
        
        
        df.fillna(0.00, inplace=True)
        
        return df
        
        
def gen_pnl_report(db, cob_date : date, fmt : ReportFormat) -> str:
    cob_date_str = cob_date.strftime('%Y-%m-%d')

    match fmt:
        case ReportFormat.HTML:
              tblfmt = 'html'
        case ReportFormat.MARKDOWN:
            #tblfmt = 'github'
            tblfmt = 'simple'
        case _:
            raise ValueError(f"Unsupported format {fmt}")

    qry = f"""
            SELECT
                *
            FROM
                portfolios
            """    
    portfolios_df = db.execute(qry).fetchdf()
    
    rpt_str = gen_report_heading(f"{cob_date_str} P&L Report", 2, fmt)
    
    for index, row in portfolios_df.iterrows():
        
        rpt_str += gen_report_heading(row.portfolio_name, 2, fmt)
        
        for measure in [{'name':'total_pnl_portfolio_ccy', 'label':'Total P&L'},
                        {'name': 'dividends_portfolio_ccy_exdiv','label':'Dividends'},
                        {'name': 'realized_pnl_portfolio_ccy', 'label':'Realized P&L'},
                        {'name': 'unrealized_pnl_portfolio_ccy_local', 'label':'Unrealized P&L'},
                        {'name': 'unrealized_fx_pnl', 'label':'FX P&L'}]:
            
            
            rpt_str += gen_report_heading(measure['label'], 3, fmt) 
            
            df = get_pnl_pivot(db, measure['name'], row.portfolio_id, cob_date)
             
            tbl = tabulate(df[['security_type_2', 'Long', 'Short', 'Total']],
                            headers   =  ['Asset Class', 'Long', 'Short', 'Total'],
                            tablefmt  =  tblfmt,
                            numalign  =  'right', 
                            floatfmt  =  ',.2f', intfmt=',',
                            showindex =  False)
            rpt_str += tbl + '\n'
            rpt_str += '\n'
                                
    return rpt_str

def gen_risk_report(db, cob_date : date, fmt : ReportFormat) -> str:
    cob_date_str = cob_date.strftime('%Y-%m-%d')

    match fmt:
        case ReportFormat.HTML:
              tblfmt = 'html'
        case ReportFormat.MARKDOWN:
            #tblfmt = 'github'
            tblfmt = 'simple'
        case _:
            raise ValueError(f"Unsupported format {fmt}")

    qry = f"""
            SELECT
                *
            FROM
                portfolios
            ORDER BY
                portfolio_name
            """    
    portfolios_df = db.execute(qry).fetchdf()
    
    rpt_str = gen_report_heading(f"{cob_date_str}", 1, fmt)
    rpt_str += gen_report_heading(f"Risk Report", 2, fmt)

    for index, row in portfolios_df.iterrows():
        
        rpt_str += gen_report_heading(row.portfolio_name, 2, fmt)
        
        rpt_str += gen_report_heading("Summary", 3, fmt)
        
        
        qry = f"""
            SELECT *
            FROM
                cash_balance_as_of('{cob_date_str}') cb
            WHERE
                portfolio_id = {row.portfolio_id}
            """
        df = db.execute(qry).fetchdf()
        aum = df['latest_cash_balance'].sum()
        
        rpt_str += f"""
            <b>AUM</b>: {aum:,.2f}
            \n\n
            """
        
        
        
        
        qry = f"""
            PIVOT
                (SELECT 
                    cob_date,
                    security_type_2,
                    position_type,
                    mkt_value_portfolio_ccy
                FROM 
                    strategy_allocations
                WHERE 
                    portfolio_id = {row.portfolio_id}
                AND
                    cob_date = '{cob_date_str}'
                )
            ON
                position_type
            USING
                SUM(mkt_value_portfolio_ccy)
            ORDER BY
                cob_date DESC,
                security_type_2
            """
        df = db.execute(qry).fetchdf()

        if not 'Long' in df.columns:
            df['Long'] = 0
        if not 'Short' in df.columns:
            df['Short'] = 0

        df['Net'] = df['Long'] + df['Short']
        df['Total'] = df['Long'] - df['Short']
        
        with db_utils.DuckDBTemporaryTable(db, 'tmp_pivot', df) as tmp_table_name:
            qry = f"""
                SELECT
                    cob_date,
                    'Total' AS security_type_2,
                    SUM(Long) AS Long,
                    SUM(Short) AS Short,
                    SUM(Net) AS Net,
                    SUM(Total) AS Total
                FROM
                    tmp_pivot
                GROUP BY
                    cob_date
                """
            df_total = db.execute(qry).fetchdf()
        
        # concatenate the tables
        df = pd.concat([df, df_total])
        
        # fill NaNs with 0
        df = df.fillna(0.00)
        
        tbl = tabulate(df[['security_type_2', 'Long', 'Short', 'Net', 'Total']], 
                        headers   =  ['Asset Class', 'Long', 'Short', 'Net', 'Total'],
                        tablefmt  =  tblfmt,
                        numalign  =  'right', 
                        floatfmt  =  ',.2f', intfmt=',',
                        showindex =  False)
        
        
        
        
        rpt_str += tbl + '\n'
        rpt_str += '\n'
        
        
        
        # divide each element of df by aum
        df[['Long', 'Short', 'Net', 'Total']] = df[['Long', 'Short', 'Net', 'Total']].div(aum) * 100
        
        rpt_str += gen_report_heading("% of AUM", 4, fmt)
        
        tbl = tabulate(df[['security_type_2', 'Long', 'Short', 'Net', 'Total']],
                        headers   =  ['Asset Class' 'Long %', 'Short %', 'Net %', 'Total %'],
                        tablefmt  =  tblfmt,
                        numalign  =  'right', 
                        floatfmt  =  ',.2f', intfmt=',',
                        showindex =  False)
        
        rpt_str += tbl + '\n'
        rpt_str += '\n'
        
        
        
        rpt_str += gen_report_heading("Sectors", 3, fmt)
        qry = f"""
        SELECT
            cob_date,
            sector_name,
            mkt_value_portfolio_ccy,
            mkt_value_portfolio_ccy / {aum} * 100 AS pct_aum
        FROM
            sector_allocations
        WHERE
            portfolio_id = {row.portfolio_id}
        AND
            cob_date = '{cob_date_str}'
        ORDER BY
            cob_date DESC,
            sector_name
        """
        df = db.execute(qry).fetchdf()
        
        tbl = tabulate(df[['sector_name', 'mkt_value_portfolio_ccy', 'pct_aum']],
                        headers   =  ['Sector', 'Market Value', '% AUM'],
                        tablefmt  =  tblfmt,
                        numalign  =  'right', 
                        floatfmt  =  ',.2f', intfmt=',',
                        showindex =  False)
        
        rpt_str += tbl + '\n'
        rpt_str += '\n'

        rpt_str += gen_report_heading("Positions", 3, fmt)
        
        qry = f"""
            SELECT 
                ROW_NUMBER() OVER() as row_num,
                psn.portfolio_id,
                portfolio_name,
                securities.security_id,
                securities.figi,
                --------------------------------------------------------
                security_description AS ticker,
                --securities.base_ticker AS ticker,
                --------------------------------------------------------
                securities.exch_code,
                securities.ccy,
                securities.security_type_2,
                --security_description AS display_ticker,
                quantity,
                COALESCE(o.shares_per_contract, 1) AS multiplier,
                --avg_cost,
                market_data.close_price AS close_price,
                market_data.close_price * quantity * multiplier AS mkt_value,
                f2.fx_rate / f1.fx_rate AS fx_rate,
                market_data.close_price * quantity * multiplier * f2.fx_rate / f1.fx_rate AS mkt_value_portfolio_ccy,
                market_data.close_price * quantity * multiplier * f2.fx_rate / f1.fx_rate / {aum} * 100 AS pct_aum,
            FROM 
                positions('{cob_date_str}') AS psn
            LEFT JOIN
                portfolios 
            ON 
                portfolios.portfolio_id = psn.portfolio_id
            LEFT JOIN
                securities 
            ON 
                securities.security_id = psn.security_id
            LEFT JOIN
                options o
            ON
                o.security_id = psn.security_id
            LEFT JOIN
                market_data 
            ON 
                market_data.security_id = psn.security_id
            AND 
                market_data.trade_date = '{cob_date_str}'
            LEFT JOIN
                fx_rates_data f1
            ON
                f1.ccy = portfolios.portfolio_ccy
            AND
                f1.fx_date = '{cob_date_str}'
            LEFT JOIN
                fx_rates_data f2
            ON
                f2.ccy = securities.ccy
            AND 
                f2.fx_date = '{cob_date_str}'
            WHERE
                psn.quantity != 0
            AND
                psn.portfolio_id = {row.portfolio_id}
            ORDER BY
                portfolio_name,
                securities.security_type_2,
                securities.base_ticker
            """
        df = db.execute(qry).fetchdf()
        
        cost_basis = compute_positions_asof_date(db, cob_date)
        
        
        df = pd.merge(df, 
                      cost_basis[['portfolio_id', 'security_id', 'avg_cost', 'realized_pnl_to_date']], 
                      on=['portfolio_id', 'security_id'], how='left')
        
        
        tbl = tabulate(df[[ 'ticker', 'ccy', 'quantity', 'avg_cost', 'close_price', 'mkt_value', 'mkt_value_portfolio_ccy', 'pct_aum']],
                        headers   =  [ 'Ticker', 'Crncy', 'Quantity', 'Cost Basis', 'Close Price', 'Market Value (local)', 'Market Value', '% AUM'],
                        tablefmt  =  tblfmt,
                        numalign  =  'right', 
                        floatfmt  =  ',.2f', intfmt=',',
                        showindex =  False)
        
        rpt_str += tbl + '\n'
        rpt_str += '\n'
        
        
    return rpt_str
            
 
def send_eod_risk_report(db, cob_date : date):
    
    risk_report = gen_risk_report(db, cob_date, ReportFormat.HTML)
    pnl_report = gen_pnl_report(db, cob_date, ReportFormat.HTML)        
    report = f'{risk_report}\n\n{pnl_report}'
    
    qry = f"""
        SELECT *
        FROM
            security_level_pnl p
        LEFT JOIN
            securities s
        ON
            p.security_id = s.security_id
        LEFT JOIN
            options o
        ON
            s.security_id = o.security_id
        WHERE
            cob_date = '{cob_date}'
        ORDER BY
            portfolio_id,
            s.security_type_2,
            p.base_ticker
    """
    df1 = db.execute(qry).fetchdf()
    
    send_email_report(f'Risk Report - {cob_date}', report, [df1])