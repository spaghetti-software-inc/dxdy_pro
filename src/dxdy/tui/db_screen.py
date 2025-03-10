import re
from datetime import datetime

from textual.app import ComposeResult, App, Binding
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widget import Widget
from textual.widgets import (
    DataTable,
    Tree,
    Header,
    Footer,
    Button,
    Input,
)
from textual.events import Key
from textual.message import Message

from .tui_utils import format_data_table_cell, DxDyLogMsg

from ..settings import Settings
from ..db import schema


from loguru import logger
import re

class DuckDbTable(Widget):
    
    class QueryInput(Message):
        raw_input = None
        date_query = None
        
        def __init__(self, raw_input, date_query = None) -> None:
            super().__init__()
            self.raw_input = raw_input
            self.date_query = date_query
    
    
    sql_query = None
    
    """
    A widget that displays data from a DuckDB query in a Textual DataTable.
    Includes basic pagination and a keyboard shortcut to copy the entire DataFrame.
    """

    def __init__(self, table_format=None, rows_per_page: int = 3300, **kwargs):
        """
        :param table_format: An optional list of columns to display and format.
        :param rows_per_page: How many rows to display per page by default.
        """
        super().__init__(**kwargs)
        self.table_format = table_format
        self.rows_per_page = rows_per_page

        self.df = None
        self.table = None
        self.current_page = 0
        self.total_pages = 0
        self.cur_sort_col = None
        
        self.cur_row = None
        
        self.sort_order = -1

    def compose(self) -> ComposeResult:
        self.table = DataTable( id="duckdb_data_table", cursor_type="row")
        self.table.fixed_columns = 2

        self.prev_button = Button("Prev", id="btn-prev", variant="primary")
        self.next_button = Button("Next", id="btn-next", variant="primary")
        
            
        with Vertical():
            with Horizontal(id="table-container"):
                self.table = DataTable(id="duckdb_data_table", cursor_type="row")
                self.table.fixed_columns = 2
                yield self.table
            # with Horizontal(id="db_widget_controls"):
            #     yield Button("Prev", id="btn-prev", variant="primary")
            #     yield Button("Next", id="btn-next", variant="primary")
            #     yield Input(id="db_widget_input")

        
    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.log(f"Input submitted: {event.value}")
        
        date_query = None
        # attempt to parse the input as an ISO format date
        try:
            date_query = datetime.fromisoformat(event.value)
            self.log(f"Input parsed as date: {date_query}")
        except:
            pass
        
        self.post_message(DuckDbTable.QueryInput(event.value, date_query))
        
        input_widget = self.query_one(Input)
        input_widget.blur()

    def clear_input_widget(self):
        input_widget = self.query_one(Input)
        input_widget.clear()


    def on_button_pressed(self, event: Button.Pressed) -> None:
        """
        Handles button presses for pagination.
        """
        if not self.df is None:
            if event.button.id == "btn-prev":
                self.current_page = max(self.current_page - 1, 0)
                self.refresh_table()
                
                #self.next_button.disabled = False
                
            elif event.button.id == "btn-next":
                if self.current_page < self.total_pages - 1:
                    self.current_page = min(self.current_page + 1, self.total_pages - 1)
                    self.refresh_table()
                    
                    # if self.current_page > 0: 
                    #     self.prev_button.disabled = False
                else:
                    #self.next_button.disabled = True
                    pass

    def on_data_table_cell_highlighted(self, event : DataTable.CellHighlighted) -> None:
        self.log(f"Cell selected: {event.coordinate}")

    # RowSelected(data_table, cursor_row, row_key)
    def on_data_table_row_selected(self, event : DataTable.RowSelected) -> None:
        self.log(f"Row selected: {event.cursor_row}")
        self.cur_row = event.cursor_row
        
        
    # HeaderSelected(data_table, column_key, column_index, label)
    def on_data_table_header_selected(self, event: DataTable.HeaderSelected) -> None:
        """
        Sorts the DataFrame by the selected column.
        """
        if self.df is not None:
            if self.table_format is None:
                col = self.df.columns[event.column_index]
            else:
                self.log(f"{event.column_index} of {self.table_format.keys()}")
                self.log(f"{self.df.columns}")
                
                view_idx = event.column_index
                
                columns = []
                for col_name in self.table_format.keys():
                    if col_name in self.df.columns:
                        columns.append(col_name)

                col = columns[view_idx]
                self.log(f"{columns}")
                # # ToMl
                # col = list(self.table_format.keys())[event.column_index]
            
            self.sort_order *= -1
            
            
            if self.sort_order == 1:
                self.set_sql_query_sort_order(col, "ASC")
            else:        
                self.set_sql_query_sort_order(col, "DESC")
                
            
    def set_sql_query_sort_order(self, col, order):
        sql_without_order_by = re.sub(r'\bORDER BY\b.*', '', self.original_sql_query, 
                                      flags=re.IGNORECASE | re.DOTALL)
        
        self.sql_query = sql_without_order_by  + f"ORDER BY {col} {order}"

        self.cur_sort_col = col

        # self.log(f"{sql_without_order_by}")
        # self.log(f"{self.sql_query}")
        
        db_conn = Settings().get_db_connection()
        self.df = db_conn.execute(self.sql_query).fetchdf()
        
        # self.log(f"{self.df}")
        
        
        self.df.fillna(0.00, inplace=True)
        db_conn.close()
        
        # Filter columns if display_cols is provided
        # if self.display_cols is not None:
        #     cols_to_drop = [col for col in self.df.columns if col not in self.display_cols]
        #     self.df.drop(columns=cols_to_drop, inplace=True)

        # Calculate total pages
        num_rows = len(self.df)
        self.total_pages = max((num_rows + self.rows_per_page - 1) // self.rows_per_page, 1)
        self.current_page = 0

        self.table.clear(columns=True)
        
        if self.table_format is None:
            for col in self.df.columns:
                self.table.add_column(col)
        else:
            for col_name in self.table_format:
                if col_name in self.df.columns:
                    col_display_name = self.table_format[col_name]['name']
                    self.table.add_column(col_display_name, key=col_name)
            
                
        # Populate first page
        self.refresh_table()
        
        
    def set_sql_query(self, sql_query: str) -> None:
        """
        Runs the provided SQL query in DuckDB, stores the result DataFrame, calculates pages,
        and populates the table (first page).
        """
        self.sql_query = sql_query
        self.original_sql_query = sql_query


        db_conn = Settings().get_db_connection()
        self.df = db_conn.execute(self.sql_query).fetchdf()
        
        self.log(f"{self.df}")
        
        self.df.fillna(0.00, inplace=True)
        db_conn.close()
        
        # Filter columns if display_cols is provided
        # if self.display_cols is not None:
        #     cols_to_drop = [col for col in self.df.columns if col not in self.display_cols]
        #     self.df.drop(columns=cols_to_drop, inplace=True)

        # Calculate total pages
        num_rows = len(self.df)
        self.total_pages = max((num_rows + self.rows_per_page - 1) // self.rows_per_page, 1)
        self.current_page = 0

        self.table.clear(columns=True)
        
        if self.table_format is None:
            for col in self.df.columns:
                self.table.add_column(col)
        else:
            for col_name in self.table_format:
                if col_name in self.df.columns:
                    col_display_name = self.table_format[col_name]['name']
                    self.table.add_column(col_display_name, key=col_name)
            
                
        # Populate first page
        self.refresh_table()
        
        # if self.current_page == 0: 
        #     self.prev_button.disabled = True
        
        # if self.total_pages == 1:
        #     self.next_button.disabled = True
            
        

    def refresh_table(self) -> None:
        """
        Populates the DataTable with the current page’s subset of the data.
        """
        if self.df is None:
            return

        # Calculate page boundaries
        start_idx = self.current_page * self.rows_per_page
        end_idx = start_idx + self.rows_per_page

        # Use a slice of the DataFrame for the current page
        df_subset = self.df.iloc[start_idx:end_idx]

        # Convert the subset to rows (list of tuples)
        df_lst = list(df_subset.itertuples(index=False, name=None))

        # Clear the table and re-populate
        self.table.clear(columns=False)  
        
        if self.table_format is None:
            self.table.add_rows(df_lst)
        else:
            for idx, row in df_subset.iterrows():
                styled_row = []
                for col_name in self.table_format:
                    if col_name in df_subset.columns:
                        col_type = self.table_format[col_name]['type']
                        if 'style' in self.table_format[col_name]:
                            col_style = self.table_format[col_name]['style']
                        else:
                            col_style = None                
                        
                        value = row[col_name]
                        cell = format_data_table_cell(col_type, value, col_style)
                        styled_row.append(cell)
                self.table.add_row(*styled_row)
                

        # self.log(
        #     f"Showing page {self.current_page+1}/{self.total_pages}, "
        #     f"rows {start_idx+1} to {min(end_idx, len(self.df))} of {len(self.df)}."
        # )

    def on_key(self, event: Key) -> None:
        #self.log(f"Key pressed: {event.key}")
        """
        Copies the entire DataFrame to the clipboard when any key is pressed.
        (You may want to restrict this to a specific key instead.)
        """
        if self.df is not None:
            # Copy the DataFrame to clipboard if CTRL+e is pressed
            if event.key == "x":            
                self.log("Copying the DataFrame to clipboard!")
                
                if self.table_format is None:
                    cols = self.df.columns
                else:
                    cols = []
                    for col_name in self.table_format:
                        if col_name in self.df.columns:
                            cols.append(col_name)
                    
                self.df[cols].to_clipboard(index=False, header=True)
            
                query = self.sql_query.strip()
                query = query.replace('\t', ' ').replace('\n', ' ')
                query = re.sub(r'\s+', ' ', query)
                logger.info(f"Data copied to clipboard: {query}")


class DuckDbTree(Widget):
    """
    A widget that displays a tree of tables and their columns, grouped by table name.
    Clicking a table name (node) will prompt the parent widget to load that table.
    """


    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.db_tree = None

    def compose(self) -> ComposeResult:
        self.db_tree = Tree("db")
        self.db_tree.root.expand()
        schema_df = schema.get_database_schema()

        for table_name, table_df in schema_df.groupby("table_name"):
            table_node = self.db_tree.root.add(table_name, data=table_name, expand=False)
            for row in table_df.itertuples():
                column_label = f"{row.column_name} ({row.data_type})"
                table_node.add_leaf(column_label, data={"column": row.column_name})

        yield self.db_tree

    def on_tree_node_selected(self, message: Tree.NodeSelected) -> None:
        """
        When a table node is selected, we expand it in case it isn’t already,
        and dispatch a message for the parent.
        """
        self.log(f"Node selected: {message.node}")

        # Expand the node if it has children
        if not message.node.is_expanded and len(message.node.children) > 0:
            self.post_message(Tree.NodeExpanded(node=message.node))

    def on_tree_node_expanded(self, message: Tree.NodeExpanded) -> None:
        self.log(f"Node expanded: {message.node}")


class DbViewerWidget(Widget):
    """
    Wrapper widget that contains both the DuckDbTree and the DuckDbTable side by side.
    When a table is clicked in the tree, the corresponding data is loaded in the table.
    """

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.duckdb_tree = None
        self.duckdb_table = None

    def compose(self) -> ComposeResult:
        self.duckdb_tree = DuckDbTree(classes="box1", id="duckdb_tree")
        yield self.duckdb_tree

        self.duckdb_table = DuckDbTable(id="duckdb_table", classes="box2")
        yield self.duckdb_table

    def on_tree_node_expanded(self, message: Tree.NodeExpanded) -> None:
        """
        When a node representing a table is expanded (user clicked on table name),
        load data for that table.
        """
        table_name = message.node.data
        if isinstance(table_name, str):  # Only proceed if this node is a table
            self.log(f"Loading data for table: {table_name}")
            self.duckdb_table.set_sql_query(f"SELECT * FROM {table_name}")


class DbScreen(Screen):
    """
    A screen containing a Header, Footer, and the database viewer widget.
    """

    DEFAULT_CSS = """
    DbScreen {
        width: 1fr;
        height: 1fr;
    }
    """

    def compose(self) -> ComposeResult:
        yield Header(id="Header", show_clock=True)
        yield Footer(id="Footer")
        yield DbViewerWidget(id="DbViewerWidget")


class DbApp(App):
    """
    An example Textual App that shows the DbScreen with the enhanced DuckDB table viewer.
    Press 'ctrl+c' to exit.
    """

    BINDINGS = [
        Binding("escape", "app.quit", "Quit"),
    ]

    CSS_PATH = "app.css"

    def on_mount(self) -> None:
        """
        Called after the app has mounted.
        We push the DbScreen onto the screen stack so it’s displayed.
        """
        self.push_screen(DbScreen())


if __name__ == "__main__":
    app = DbApp()
    app.run()
