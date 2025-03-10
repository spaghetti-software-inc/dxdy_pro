# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

import sys
from pathlib import Path
from tomlkit import dumps
from tomlkit import parse  # you can also use loads
import duckdb
import time
from datetime import date
from zoneinfo import ZoneInfo
import pytz
import pandas_market_calendars as mkt_cal



from loguru import logger

# logger.remove()
# # enable logger
# logger.add(sys.stderr, format="<green>{time}</green> <level>{level}</level> <cyan>{message}</cyan>")
# logger.level("DEBUG")

class Settings:
    def __init__(self):
        
        home_dir = Path.home()
        self.dxdy_dir = home_dir / ".dxdy"
        self.dxdy_dir.mkdir(parents=True, exist_ok=True)
        
        self.ui_settings_file = self.dxdy_dir / "ui_settings.toml"
        
        self.log_file = self.dxdy_dir  / "dxdy_log.duckdb"
        self.settings_file = self.dxdy_dir / "settings.toml"
        

        if not self.settings_file.exists():
            raise Exception(f"Settings file {self.settings_file} does not exist")
        
        if not self.ui_settings_file.exists():
            raise Exception(f"UI Settings file {self.ui_settings_file} does not exist")
        
        self.ui_settings = self.load_ui_settings()        
        self.settings = self.load_settings()
        
    def load_settings(self):
        with open(self.settings_file, "r") as f:
            return parse(f.read())
        
    def load_ui_settings(self):
        with open(self.ui_settings_file, "r") as f:
            return parse(f.read())
        
        
    def save_settings(self):
        with open(self.settings_file, "w") as f:
            f.write(dumps(self.settings))

    def get_log_file(self) -> Path:
        log_db_file = self.log_file
        
        if not log_db_file.exists():
            # Create the log file
            conn = duckdb.connect(log_db_file)

            # Create 'log' table
            log_table_sql = """
            CREATE TABLE IF NOT EXISTS dxdy_log (
                log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                task TEXT,
                message TEXT,
                is_error BOOLEAN DEFAULT FALSE,
            );
            """
            conn.execute(log_table_sql)
            conn.close()

        return Path(self.log_file)
    
    
    def _get_db_file(self) -> Path:
        return Path(self.settings['database']['file'])

    def get_dxdy_user_dir(self) -> Path:
        return self.dxdy_dir
        


    def get_db_backup_file(self, cob_date: date) -> Path:
        now = time.time()
        return Path(self.settings['database']['backup_directory']) / f"dxdy_backup_{cob_date}_{now}.duckdb"

    def get_test_data_dir(self) -> Path:
        return Path('/Users/av/repos/dxdy/data/test_db/')

    def get_test_ai_data_dir(self) -> Path:
        return Path('/Users/av/repos/dxdy/data/ai/')

    def get_project_root(self) -> Path:
        return Path(__file__).parent.parent

    def get_db_connection(self, readonly=True):
        MAX_TRIES = 9
        
        for i in range(MAX_TRIES):
            try:
                return duckdb.connect(self._get_db_file(), read_only=readonly)
            except Exception as e:
                if i == MAX_TRIES - 1:
                    raise e
                else:
                    #raise e
                    #logger.debug(f"duckdb.connect({self._get_db_file()}, read_only={readonly}) retrying in 1.0 seconds")
                    time.sleep(1.0)
                    continue
        
    
    def get_intraday_pnl_files_dir(self) -> Path:
        dir = Path(self.settings['intraday_pnl']['directory'])
        # make sure the directory exists
        dir.mkdir(parents=True, exist_ok=True)
        return dir
    

    def get_ntp_server(self) -> str:
        return str(self.settings['ntp']['server'])
    
    def get_timezone(self) -> ZoneInfo:        
        # with self.get_db_connection() as conn:
        #     qry = f"""
        #     SELECT *
        #     FROM 
        #         duckdb_settings()
        #     WHERE 
        #         name = 'TimeZone';
        #     """
        #     df = conn.execute(qry).fetchdf()
        #     return ZoneInfo(df.loc[0, 'value'])
        return ZoneInfo('America/New_York')


    def get_timezone_pytz(self) -> pytz.timezone:
        return pytz.timezone(str(self.get_timezone()))
    
    
    def get_calendars(self) -> list:
        user_calendars = self.settings['calendar']['trading_exchanges']
        for calendar in user_calendars:
            if calendar not in mkt_cal.get_calendar_names():
                raise Exception(f"Calendar {calendar} not found in pandas_market_calendars")
            
        return self.settings['calendar']['trading_exchanges']
    
    
    def get_realtime_calculation_tcp_socket(self) -> str:
        return str(self.settings['microservices']['realtime_calculation_tcp_socket'])
    
    def get_config_file(self):
        return self.settings
    
    def get_ui_config_file(self):
        return self.ui_settings   
    
    
    