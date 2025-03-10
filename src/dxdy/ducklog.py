
# ducklog module

def version():
    return "This is DuckLog v0.1.0"

from pathlib import Path
import logging
import rich
import duckdb
from datetime import datetime

import pandas as pd

class DuckDBHandler(logging.Handler):
    # db_file is in-memory by default
    def __init__(self, db_file=":memory:", table_name='logs'):
        super().__init__()
    
        new_db = False
        if db_file == ":memory:":
            self.db_file = db_file
        else:
            self.db_file = Path(db_file)
            if not self.db_file.exists():
                new_db = True
        

        self.table_name = table_name
        self.conn = duckdb.connect(self.db_file)
    
        if new_db:
            self._create_table()

    def _create_table(self):
        # Create a table to store log messages if it doesn't exist
        create_table_query = f"""
        CREATE SEQUENCE seq_logid START 1;
        
        CREATE TABLE IF NOT EXISTS {self.table_name} (
            id INTEGER PRIMARY KEY,
            timestamp TIMESTAMP,
            level TEXT,
            message TEXT,
            module TEXT,
            funcName TEXT,
            lineno INTEGER
        );
        """
        self.conn.execute(create_table_query)

    def emit(self, record):
        # Format the log record and insert it into DuckDB
        timestamp = datetime.now()
        level = record.levelname
        message = record.msg
        module = record.module
        funcName = record.funcName
        lineno = record.lineno

        insert_query = f"""
        INSERT INTO {self.table_name} 
        (id, timestamp, level, message, module, funcName, lineno)
        VALUES (nextval('seq_logid'), ?, ?, ?, ?, ?, ?);
        """
        # self.conn.execute(insert_query, (timestamp, level, message, module, funcName, lineno))

        rich.print(f"[bold]{timestamp}[/bold] [blue]{level}[/blue] [green]{module}[/green] [yellow]{funcName}[/yellow] [red]{lineno}[/red] {message}")

    def fetch_df(self):
        # Fetch the log messages from DuckDB and return them as a DataFrame
        query = f"SELECT * FROM {self.table_name} ORDER BY timestamp DESC"
        df = self.conn.execute(query).fetch_df()
        return df

    def close(self):
        # self.conn.close()
        super().close()


from .settings import Settings
settings = Settings()
log_file = settings.get_log_file()
#duckdb_handler = DuckDBHandler(db_file=str(log_file))
duckdb_handler = DuckDBHandler(db_file=":memory:")

class DuckDBLogger(logging.Logger):
    def __init__(self, name="dxdy", level=logging.DEBUG):
        super().__init__(name, level)
        #self.duckdb_handler = DuckDBHandler(db_file)
        self.addHandler(duckdb_handler)

    def fetch_df(self):
        return duckdb_handler.fetch_df()

    def close(self):
       pass

    def print_logs(self):
        df = self.fetch_df()
        print('\n')
        print(df)

    def __str__(self):
        return f"DuckDBLogger(name={self.name}, level={self.level}, db_file={duckdb_handler.db_file})"

    def __del__(self):
        self.close()

