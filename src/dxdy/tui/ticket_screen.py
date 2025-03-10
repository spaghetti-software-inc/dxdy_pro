# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

from enum import Enum
import datetime

import pandas as pd

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.reactive import reactive
from textual.widgets import Header, Footer, Input, Label, Button, Select, TabbedContent, TabPane, ProgressBar, Static
from textual.suggester import SuggestFromList
from textual.containers import Vertical, Horizontal
from textual.color import Color, Gradient
from textual.widget import Widget

from loguru import logger

from ..settings import Settings
from ..db.utils import DuckDBTemporaryTable

gradient = Gradient.from_colors(
    Color(105, 27, 158),
    Color(110,141,233),
    Color(133,22,87),
    Color(255,168,255),
    Color(205,73,220),
    Color(51,74,171),
    Color(191,214,250),
    Color(5,157,197),
    Color(91,239,143),
    Color(21,78,86),
    Color(82,233,230),
    Color(6,150,104),
)

class InputFormState(Enum):
    IDLE = "idle"
    PENDING = "pending"
    SUCCESS = "success"
    ERROR = "error"



#########################################################################
#
#
class StockTradeTicket(Widget):
    
    input_form_state: InputFormState = InputFormState.IDLE
    form_data : dict = None
    portfolios : list = None
    date_choices : SuggestFromList = None
    stock_ticker_choices : SuggestFromList = None
    exch_codes_choices : SuggestFromList = None
    
    def __init__(self, portfolios, date_choices, stock_ticler_choices, exch_codes_choices, **kwargs):
        super().__init__(**kwargs)
        
        self.portfolios = portfolios
        self.date_choices = date_choices
        self.stock_ticker_choices = stock_ticler_choices
        self.exch_codes_choices = exch_codes_choices
        
    
    def compose(self) -> ComposeResult:
        yield ProgressBar(total=100, gradient=gradient, show_percentage=False, show_eta=False, id="progress_bar")

        yield Select(self.portfolios, prompt="Portfolio", id="portfolio")
        yield Input(placeholder="Trade Date (YYYY-MM-DD)", type="text", suggester=self.date_choices, id="trade_date")
        yield Input(placeholder="Ticker Symbol", type="text", suggester=self.stock_ticker_choices, id="base_ticker")
        yield Input(placeholder="Exchange", type="text", suggester=self.exch_codes_choices, id="exch_code")
        yield Input(placeholder="Quantity", type="integer", id="quantity")
        yield Input(placeholder="Price", type="number", id="price")

        with Horizontal():
            yield Button(label="Buy 📈", id="buy_button")
            yield Button(label="Sell 📉", id="sell_button")

        self.info_label = Label("", id = "trading_ticket_info_label")
        yield self.info_label

        
    def on_mount(self) -> None:
        self.query_one(f"#progress_bar").update(progress=10)
             

    def update_form_data(self) -> None:
        # Collect form data
        self.form_data = {
            "portfolio": self.query_one("#portfolio").value,
            "trade_date": self.query_one("#trade_date").value,
            "base_ticker": self.query_one("#base_ticker").value,
            "exch_code": self.query_one("#exch_code").value,
            "quantity": self.query_one("#quantity").value,
            "price": self.query_one("#price").value
        }


    def on_input_changed(self, event: Input.Changed) -> None:
        
        self.update_form_data()
        self.log(self.form_data['portfolio'])
        
        with Settings().get_db_connection(readonly=True) as db:
            # try to parse the date
            dt = None
            try:
                dt = datetime.datetime.strptime(self.form_data['trade_date'], "%Y-%m-%d")
            except ValueError:
                pass
                        
            if dt is not None:
                qry = f"""
                        SELECT *
                        FROM
                            calendar
                        WHERE
                            cob_date = '{self.form_data['trade_date']}'
                        """
                date_search = db.execute(qry).fetch_df()                   
                                            
                date_str = dt.strftime("%a, %b %d, %Y")
            
                if date_search.empty:                            
                    date_info = f"[red]Invalid date: [cyan] {date_str}"
                else:
                    date_info = f"[cyan]  Trade date: [medium_spring_green] {date_str}"
            else:
                date_info = "[purple] . . ."
                                
            qry = f"""
                SELECT *
                FROM
                    securities
                WHERE
                    securities.base_ticker = '{self.form_data['base_ticker']}'
                AND
                    securities.exch_code = '{self.form_data['exch_code']}'
                AND
                    security_type_2 = 'Common Stock'
                """
            ticker_search = db.execute(qry).fetch_df()
            
            if self.form_data['base_ticker'] == "":
                ticker_info = f"[purple] . . ."
            elif ticker_search.empty:
                ticker_info = f"[red] Unknown ticker: [cyan] {self.form_data['base_ticker']} {self.form_data['exch_code']}"
            else:
                name = ticker_search["name"].values[0]
                ticker_info = f"[cyan] Stock ticker: [medium_spring_green] {self.form_data['base_ticker']} {self.form_data['exch_code']} ({name})"
        
            self.info_str =  date_info + '[white] | ' + ticker_info
            self.info_label.update(self.info_str)
            
            
            
    async def update_db(self, data : dict) -> None:
        form_data_df = pd.DataFrame([data])
        with Settings().get_db_connection(readonly=False) as db:
            with DuckDBTemporaryTable(db, "tmp_form_data", form_data_df):
                
                qry = f"""
                INSERT INTO
                    trades (portfolio_id, security_id, trade_date, quantity, price, created_by)
                SELECT
                    portfolio AS portfolio_id,
                    security_id,
                    trade_date,
                    quantity,
                    price,
                    'manual entry'
                FROM
                    tmp_form_data
                LEFT JOIN
                    securities
                ON
                    tmp_form_data.base_ticker = securities.base_ticker
                AND
                    tmp_form_data.exch_code = securities.exch_code
                AND
                    security_type_2 = 'Common Stock'
                """
                db.execute(qry)
                db.commit()
            
                self.info_str = "[cyan2]Stock transaction saved"
                self.info_label.update(self.info_str)
                logger.info(f"Stock transaction saved: {data}")
        
        # Update the progress bar
        progress_bar = self.query_one(f"#progress_bar")
        progress_bar.update(progress=100)
        
        # Clear the form inputs
        for input in self.query("Input"):
            input.clear()
            
        for selec in self.query("Select"):
            selec.clear()
               
        
        # Enable buttons and form inputs
        for button in self.query("Button"):
            button.disabled = False
            button.variant = 'default'
        for input_field in self.query("Input"):
            input_field.disabled = False
            
        # Reset the form state
        self.input_form_state = InputFormState.IDLE            
            
            
            
    async def on_button_pressed(self, event: Button.Pressed) -> None:
        
        if self.input_form_state == InputFormState.IDLE:
            # Disable buttons and form inputs
            for button in self.query("Button"):
                button.disabled = True
            for input_field in self.query("Input"):
                input_field.disabled = True
            
    
            # Collect form data
            self.update_form_data()


            # enable the button that was pressed
            button = self.query_one(f"#{event.button.id}")
            self.log(button)
            button.disabled = False
            button.variant = 'primary'
            
            # set the screen focus to the button that was pressed
            # self.set_focus(button)
            
            # Set the form state to pending
            self.input_form_state = InputFormState.PENDING
            
            # update progress bar
            progress_bar = self.query_one(f"#progress_bar")
            progress_bar.update(progress=50)
            
            
            # now we wait for the user to press the button again to submit the form
            if event.button.id == "buy_button":
                button_str = "[bright_green]BUY"
                instruct_str = "Press BUY again to confirm"
                
            elif event.button.id == "sell_button":                
                self.form_data['quantity'] = -int(self.form_data['quantity'])
                button_str = "[bright_red]SELL"
                instruct_str = "Press SELL again to confirm"
            
            self.info_str += '[white] | ' + button_str + f' [steel_blue]({instruct_str})'
            self.info_label.update(self.info_str)
            
            
        elif self.input_form_state == InputFormState.PENDING:
            # disable the button that was pressed
            button = self.query_one(f"#{event.button.id}")
            button.disabled = True
            button.variant = 'default'
            
            # Run the update_db function with the collected data
            #self.run_worker(self.update_db(self.form_data), exclusive=True)
            await self.update_db(self.form_data)





#########################################################################
#
#
class OptionTradeTicket(Widget):
    
    input_form_state: InputFormState = InputFormState.IDLE
    form_data : dict = None
    portfolios : list = None
    date_choices : SuggestFromList = None
    stock_ticker_choices : SuggestFromList = None
    exch_codes_choices : SuggestFromList = None
    contract_types :list = [('Call',1), ('Put',2)]
    
    def __init__(self, portfolios, date_choices, stock_ticler_choices, exch_codes_choices, **kwargs):
        super().__init__(**kwargs)
        self.info_label = Label("", id = "trading_ticket_info_label")
        
        self.portfolios = portfolios
        self.date_choices = date_choices
        self.stock_ticker_choices = stock_ticler_choices
        self.exch_codes_choices = exch_codes_choices
        
    
    def compose(self) -> ComposeResult:
        yield ProgressBar(total=100, gradient=gradient, show_percentage=False, show_eta=False, id="progress_bar")
        
        yield Select(self.portfolios, prompt="Portfolio", id="portfolio")
        yield Input(placeholder="Trade Date (YYYY-MM-DD)", type="text", suggester=self.date_choices, id="trade_date")
        yield Select(self.contract_types, prompt="Contract Type", id="contract_type")
        yield Input(placeholder="Underlying Ticker Symbol", suggester=self.stock_ticker_choices, type="text", id="underlying_base_ticker")
        yield Input(placeholder="Exchange", type="text", suggester=self.exch_codes_choices, id="exch_code")
        
        yield Input(placeholder="Expiration Date (YYYY-MM-DD)", type="text", suggester=self.date_choices, id="expiration_date")
        yield Input(placeholder="Strike Price", type="number", id="strike_price")
                
        yield Input(placeholder="Quantity", type="integer", id="quantity")
        yield Input(placeholder="Price", type="number", id="price")

        with Horizontal():
            yield Button(label="Buy 📈", id="buy_button")
            yield Button(label="Sell 📉", id="sell_button")
            
        self.info_label = Label("", id = "trading_ticket_info_label")
        yield self.info_label


        
    def on_mount(self) -> None:
        self.query_one(f"#progress_bar").update(progress=10)
             

    def update_form_data(self) -> None:
        # Collect form data
        self.form_data = {
            "portfolio": self.query_one("#portfolio").value,
            "trade_date": self.query_one("#trade_date").value,
            "contract_type": self.query_one("#contract_type").value,
            "underlying_base_ticker": self.query_one("#underlying_base_ticker").value,
            "exch_code": self.query_one("#exch_code").value,
            "strike_price": self.query_one("#strike_price").value,
            "expiration_date": self.query_one("#expiration_date").value,
            "quantity": self.query_one("#quantity").value,
            "price": self.query_one("#price").value,
        }



    def on_input_changed(self, event: Input.Changed) -> None:
        
        self.update_form_data()
        
        with Settings().get_db_connection(readonly=True) as db:
            # try to parse the date
            dt = None
            try:
                dt = datetime.datetime.strptime(self.form_data['trade_date'], "%Y-%m-%d")
            except ValueError:
                pass
                        
            if dt is not None:
                qry = f"""
                        SELECT *
                        FROM
                            calendar
                        WHERE
                            cob_date = '{self.form_data['trade_date']}'
                        """
                date_search = db.execute(qry).fetch_df()                   
                                            
                date_str = dt.strftime("%a, %b %d, %Y")
            
                if date_search.empty:                            
                    date_info = f"[red]Invalid date: [cyan] {date_str}"
                else:
                    date_info = f"[cyan]  Trade date: [medium_spring_green] {date_str}"
            else:
                date_info = "[purple] . . ."
                
            # TODO: add option info query
            qry = f"""
                SELECT *
                FROM
                    securities
                WHERE
                    securities.base_ticker = '{self.form_data['underlying_base_ticker']}'
                AND
                    securities.exch_code = '{self.form_data['exch_code']}'
                AND
                    security_type_2 = 'Common Stock'
                """
            ticker_search = db.execute(qry).fetch_df()
            
            
            option_info = f"[purple] . . ."
            self.contract_type = ""
            if self.form_data['contract_type'] == 1:
                self.contract_type = "Call"
            elif self.form_data['contract_type'] == 2:
                self.contract_type = "Put"
            
            if self.form_data['underlying_base_ticker'] == "":
                ticker_info = f"[purple] . . ."
                
            elif ticker_search.empty:
                ticker_info = f"[red] Unknown ticker: [cyan] {self.form_data['underlying_base_ticker']} {self.form_data['exch_code']}"
            else:
                underlying_security_id = ticker_search["security_id"].values[0]
                name = ticker_search["name"].values[0]
            

                ticker_info = f"[cyan] Stock ticker: [medium_spring_green] {self.form_data['underlying_base_ticker']} {self.form_data['exch_code']} ({name})"
        
                # try to parse the expiration date
                expiry_dt = None
                try:
                    expiry_dt = datetime.datetime.strptime(self.form_data['expiration_date'], "%Y-%m-%d")
                except ValueError:
                    option_info = "[red] invalid expiry date {expiry_dt}"
                                                     
        
                if underlying_security_id is not None and self.form_data['strike_price'] != "" and expiry_dt is not None:
                    qry = f"""
                    SELECT *
                    FROM
                        options o
                    LEFT JOIN
                        securities s
                    ON
                        o.security_id = s.security_id
                    WHERE
                        o.underlying_security_id = {underlying_security_id}
                    AND
                        o.contract_type = '{self.contract_type}'
                    AND
                        o.strike_price = {self.form_data['strike_price']}
                    AND
                        o.expiration_date = '{self.form_data['expiration_date']}'
                    """
                    self.log(qry)
                    option_search = db.execute(qry).fetch_df()

   
                    if option_search.empty:
                        option_info = f"[red] Option not found: [green] {self.contract_type} [cyan] {self.form_data['expiration_date']} {self.form_data['strike_price']}"
                    else:
                        option_info = f"[purple] {option_search['ticker'].values[0]} | [cyan] {self.form_data['expiration_date']} {self.form_data['strike_price']}"
                        self.option_found = True
                        self.option_security_id = option_search['security_id'].values[0]
                
            self.info_str =  date_info + f'| [green] {self.contract_type} | ' + ticker_info + ' | ' + option_info
            self.info_label.update(self.info_str)
            
            
    async def update_db(self, data : dict) -> None:
        if not self.option_found:
            return
        
        form_data_df = pd.DataFrame([data])
        with Settings().get_db_connection(readonly=False) as db:
            with DuckDBTemporaryTable(db, "tmp_form_data", form_data_df):
                
                qry = f"""
                INSERT INTO
                    trades (portfolio_id, security_id, trade_date, quantity, price, created_by)
                SELECT
                    d.portfolio AS portfolio_id,
                    o.security_id,
                    d.trade_date,
                    d.quantity,
                    d.price,
                    'manual entry'
                FROM
                    tmp_form_data d
                LEFT JOIN
                    options o
                ON
                    o.security_id = {self.option_security_id}
                """
                db.execute(qry)
                db.commit()
            
                self.info_str = "[cyan2]Option transaction saved"
                self.info_label.update(self.info_str)
                logger.info(f"Option transaction saved: {data}")

        
        # Update the progress bar
        progress_bar = self.query_one(f"#progress_bar")
        progress_bar.update(progress=100)
        
        # Clear the form inputs
        for input in self.query("Input"):
            input.clear()
            
        for selec in self.query("Select"):
            selec.clear()
               
        
        # Enable buttons and form inputs
        for button in self.query("Button"):
            button.disabled = False
            button.variant = 'default'
        for input_field in self.query("Input"):
            input_field.disabled = False
            
        # Reset the form state
        self.input_form_state = InputFormState.IDLE            
            
            
            
    async def on_button_pressed(self, event: Button.Pressed) -> None:
        
        if self.input_form_state == InputFormState.IDLE:
            # Disable buttons and form inputs
            for button in self.query("Button"):
                button.disabled = True
            for input_field in self.query("Input"):
                input_field.disabled = True
            
    
            # Collect form data
            self.update_form_data()


            # enable the button that was pressed
            button = self.query_one(f"#{event.button.id}")
            button.disabled = False
            button.variant = 'primary'
            
            # set the screen focus to the button that was pressed
            # self.set_focus(button)
            
            # Set the form state to pending
            self.input_form_state = InputFormState.PENDING
            
            # update progress bar
            progress_bar = self.query_one(f"#progress_bar")
            progress_bar.update(progress=50)
            
            
            # now we wait for the user to press the button again to submit the form
            if event.button.id == "buy_button":
                button_str = "[bright_green]BUY"
                instruct_str = "Press BUY again to confirm"
                
            elif event.button.id == "sell_button":
                self.form_data['quantity'] = -int(self.form_data['quantity'])
                
                button_str = "[bright_red]SELL"
                instruct_str = "Press SELL again to confirm"
            else:
                button_str = ""
                instruct_str = "Press SAVE again to confirm"
            
            self.info_str += '[white] | ' + button_str + f' [steel_blue]({instruct_str})'
            self.info_label.update(self.info_str)
            
            
        elif self.input_form_state == InputFormState.PENDING:
            # disable the button that was pressed
            button = self.query_one(f"#{event.button.id}")
            button.disabled = True
            button.variant = 'default'
            
            # Run the update_db function with the collected data
            #self.run_worker(self.update_db(self.form_data), exclusive=True)
            await self.update_db(self.form_data)

            
            
            
            
            
#########################################################################
#
#
class CashTransactionTicket(Widget):
    
    input_form_state: InputFormState = InputFormState.IDLE
    form_data : dict = None
    portfolios : list = None
    date_choices : SuggestFromList = None
    currencies : list = None
    
    def __init__(self, portfolios, date_choices, currencies, **kwargs):
        super().__init__(**kwargs)
        self.info_label = Label("", id = "trading_ticket_info_label")
        
        self.portfolios = portfolios
        self.date_choices = date_choices
        self.currencies = currencies
        
    
    def compose(self) -> ComposeResult:
        yield ProgressBar(total=100, gradient=gradient, show_percentage=False, show_eta=False, id="progress_bar")
        
        yield Select(self.portfolios, prompt="Portfolio", id="portfolio")
        yield Input(placeholder="Cashflow Date (YYYY-MM-DD)", type="text", suggester=self.date_choices, id="trade_date")
        yield Input(placeholder="Cash Amount", type="number", id="amount")                        
        yield Select(self.currencies, prompt="Currency", id="currency")
        yield Select([('AUM',1), ('Dividend',2), ('Expense',3), ('Other',4)], prompt="Cashflow Type", id="cashflow_type")


        with Horizontal():
            yield Button(label="Save 💾", id="save_button")
        
        self.info_label = Label("", id = "trading_ticket_info_label")
        yield self.info_label

        
    def on_mount(self) -> None:
        self.query_one(f"#progress_bar").update(progress=10)
             

    def update_form_data(self) -> None:
        # Collect form data
        self.form_data = {
            "portfolio": self.query_one("#portfolio").value,
            "trade_date": self.query_one("#trade_date").value,
            "amount": self.query_one("#amount").value,
            "currency_id": self.query_one("#currency").value,
            "cashflow_type": self.query_one("#cashflow_type").value,
        }

  

    def on_input_changed(self, event: Input.Changed) -> None:
        
        self.update_form_data()
        
        with Settings().get_db_connection(readonly=True) as db:
            # try to parse the date
            dt = None
            try:
                dt = datetime.datetime.strptime(self.form_data['trade_date'], "%Y-%m-%d")
            except ValueError:
                pass
                        
            if dt is not None:
                qry = f"""
                        SELECT *
                        FROM
                            calendar
                        WHERE
                            cob_date = '{self.form_data['trade_date']}'
                        """
                date_search = db.execute(qry).fetch_df()                   
                                            
                date_str = dt.strftime("%a, %b %d, %Y")
            
                if date_search.empty:                            
                    date_info = f"[red]Invalid date: [cyan] {date_str}"
                else:
                    date_info = f"[cyan]  Trade date: [medium_spring_green] {date_str}"
            else:
                date_info = "[purple] . . ."
                
        self.info_str = date_info + '[white] | ' + self.form_data['amount'] 
        self.info_label.update(self.info_str)

            
            
    async def update_db(self, data : dict) -> None:
        form_data_df = pd.DataFrame([data])
        
        
        with Settings().get_db_connection(readonly=False) as db:
            with DuckDBTemporaryTable(db, "tmp_form_data", form_data_df):
                qry = f"""
                INSERT INTO
                    cash_transactions (portfolio_id, cash_date, cash_amount, ccy, cash_type, created_by)
                SELECT
                    portfolio AS portfolio_id,
                    trade_date AS cash_date,
                    amount AS cash_amount,
                    ccy,
                    cashflow_type AS cash_type,
                    'manual entry'
                FROM
                    tmp_form_data
                LEFT JOIN
                    currencies
                ON
                    tmp_form_data.currency_id = currencies.currency_id
                """
                db.execute(qry)
                db.commit()
            
                self.info_str = "[cyan2]Cash transaction saved"
                self.info_label.update(self.info_str)
                logger.info(f"Cash transaction saved: {data}")

        
        # Update the progress bar
        progress_bar = self.query_one(f"#progress_bar")
        progress_bar.update(progress=100)
        
        # Clear the form inputs
        for input in self.query("Input"):
            input.clear()
            
        for selec in self.query("Select"):
            selec.clear()
               
        
        # Enable buttons and form inputs
        for button in self.query("Button"):
            button.disabled = False
            button.variant = 'default'
        for input_field in self.query("Input"):
            input_field.disabled = False
            
        # Reset the form state
        self.input_form_state = InputFormState.IDLE            
            
            
            
    async def on_button_pressed(self, event: Button.Pressed) -> None:
        
        if self.input_form_state == InputFormState.IDLE:
            # Disable buttons and form inputs
            for button in self.query("Button"):
                button.disabled = True
            for input_field in self.query("Input"):
                input_field.disabled = True
            
    
            # Collect form data
            self.update_form_data()


            # enable the button that was pressed
            button = self.query_one(f"#{event.button.id}")
            button.disabled = False
            button.variant = 'primary'
            
            # set the screen focus to the button that was pressed
            # self.set_focus(button)
            
            # Set the form state to pending
            self.input_form_state = InputFormState.PENDING
            
            # update progress bar
            progress_bar = self.query_one(f"#progress_bar")
            progress_bar.update(progress=50)
            
            
            # now we wait for the user to press the button again to submit the form
            button_str = ""
            instruct_str = "Press SAVE again to confirm"
            
            self.info_str += '[white] | ' + button_str + f' [steel_blue]({instruct_str})'
            self.info_label.update(self.info_str)
            
            
        elif self.input_form_state == InputFormState.PENDING:
            # disable the button that was pressed
            button = self.query_one(f"#{event.button.id}")
            button.disabled = True
            button.variant = 'default'
            
            # Run the update_db function with the collected data
            #self.run_worker(self.update_db(self.form_data), exclusive=True)
            await self.update_db(self.form_data)

            
            
            
class TradingTicketScreen(Screen):

    def compose(self) -> ComposeResult:
 
        ###########################################################################
        with Settings().get_db_connection() as db:
            
            # portfolios selector
            sql_query = """
            SELECT
                portfolio_name,
                portfolio_id
            FROM 
                portfolios
            """
            portfolios_df = db.execute(sql_query).fetchdf()
            self.portfolios = []
            for index, row in portfolios_df.iterrows():
                self.portfolios.append((row["portfolio_name"], row["portfolio_id"]))
            
                
            # currency selector
            sql_query = """
            SELECT
                currency_id,
                ccy
            FROM
                currencies
            """
            currencies_df = db.execute(sql_query).fetchdf()
            self.currencies = []
            for index, row in currencies_df.iterrows():
                self.currencies.append((row["ccy"], row["currency_id"]))
                
            
            sql_query = """
            SELECT
                cob_date
            FROM
                calendar
            ORDER BY
                cob_date DESC
            """
            calendar_df = db.execute(sql_query).fetchdf()
            self.date_choices = SuggestFromList(calendar_df["cob_date"].dt.strftime("%Y-%m-%d").tolist())
 
            sql_query = """
            SELECT
                base_ticker
            FROM
                securities
            WHERE
                security_type_2 = 'Common Stock'
            """
            stock_tickers_df = db.execute(sql_query).fetchdf()
            self.stock_ticker_choices = SuggestFromList(stock_tickers_df["base_ticker"].tolist())
 
            sql_query = """
            SELECT
                base_ticker
            FROM
                securities
            WHERE
                security_type_2 = 'Option'
            """
            option_tickers_df = db.execute(sql_query).fetchdf()
            self.option_ticker_choices = SuggestFromList(option_tickers_df["base_ticker"].tolist())
 
  
            sql_query = """
            SELECT
                DISTINCT exch_code
            FROM
                securities
            """
            exch_code_df = db.execute(sql_query).fetchdf()
            self.exch_codes_choices = SuggestFromList(exch_code_df["exch_code"].tolist())
        ###########################################################################
            

        
        yield Header(id="Header", icon="⋈", show_clock=False)

        with TabbedContent(id="ticket_tabbed_content"):
            with TabPane("Stock", id="stock_tab"):
                yield StockTradeTicket(self.portfolios, self.date_choices, self.stock_ticker_choices, self.exch_codes_choices, id="stock_trade_ticket")
        
            with TabPane("Option", id="option_tab"):
                yield OptionTradeTicket(self.portfolios, self.date_choices, self.stock_ticker_choices, self.exch_codes_choices, id="option_trade_ticket")

            with TabPane("Cash", id="cash_transaction_tab"):
                yield CashTransactionTicket(self.portfolios, self.date_choices, self.currencies, id="cash_transaction_ticket")
                
        
        yield Footer(id="Footer")
    
    
    
    