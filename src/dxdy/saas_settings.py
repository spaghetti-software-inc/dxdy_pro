# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

import sys
from pathlib import Path

from datetime import date

from tomlkit import dumps
from tomlkit import parse  # you can also use loads

# import keyring
# from keyring.errors import PasswordDeleteError

class SaaSConfig:
    
    dxdy_dir: Path = None
    settings_file: Path = None
    settings: dict = None
    
    
    def __init__(self):
        
        home_dir = Path.home()
        self.dxdy_dir = home_dir / ".dxdy"
        self.dxdy_dir.mkdir(parents=True, exist_ok=True)
        
        
        self.settings_file = self.dxdy_dir / "saas_config.toml"

        if not self.settings_file.exists():
            raise Exception(f"SaaS configurations file {self.settings_file} does not exist")
        
        self.settings = self.load_settings()
        
    def load_settings(self):
        with open(self.settings_file, "r") as f:
            return parse(f.read())
        
    def save_settings(self):
        with open(self.settings_file, "w") as f:
            f.write(dumps(self.settings))


    def get_smtp_server(self) -> str:
        return self.settings['smtp']['smtp_server']
    
    def set_smtp_server(self, server: str):
        self.settings['smtp']['smtp_server'] = server
        self.save_settings()
        
    
    def get_smtp_username(self) -> str:
        return self.settings['smtp']['smtp_username']
    
    def set_smtp_username(self, username: str):
        self.settings['smtp']['smtp_username'] = username
        self.save_settings()
    
    
    def get_smtp_password(self) -> str:
        # password = keyring.get_password("dxdy_service", "smtp_password")
        # if password is None:
        #     raise Exception("SMTP password not found in keyring")
        return self.settings['smtp']['smtp_password']
    
    
    def set_smtp_password(self, password: str):
        # try:
        #     keyring.delete_password("dxdy_service", "smtp_password")
        # except PasswordDeleteError:
        #     # In case the password doesn't exist yet
        #     pass
        # keyring.set_password("dxdy_service", "smtp_password", password)
        # test = self.get_smtp_password()
        # if test != password:
        #     raise Exception(f"Error saving password {password}")
        
        self.settings['smtp']['smtp_password'] = password
        self.save_settings()        

    def get_smtp_recipients(self) -> str:
        # password = keyring.get_password("dxdy_service", "smtp_password")
        # if password is None:
        #     raise Exception("SMTP password not found in keyring")
        return self.settings['smtp']['smtp_to']



    def get_reporting_start_date(self) -> date:
        date_str = str(self.settings['reports']['start_date'])
        return date.fromisoformat(date_str)
    
    
    def get_emsx_directory(self) -> Path:
        return Path(self.settings['trade_blotters']['emsx']['download_directory'])
    
    def get_emsx_csv_files(self, trade_date: date = None) -> list:
        if trade_date is None:
            file_pattern = f"*.csv"
        else:
            file_pattern = f"*{trade_date.strftime('%Y%m%d')}*.csv"
            
        return list(self.get_emsx_directory().glob(file_pattern))
    
    def get_emsx_stock_broker(self) -> str:
        return self.settings['trade_blotters']['emsx']['stock_broker']
    
    def get_emsx_options_broker(self) -> str:
        return self.settings['trade_blotters']['emsx']['options_broker']

    def get_openai_key(self) -> str:
        return self.settings['openai']['api_key']
    
    def get_edgar_user_agent(self) -> str:
        return self.settings['edgar']['user_agent']
    
    # https://fred.stlouisfed.org/docs/api/api_key.html
    def get_fred_api_key(self) -> str:
        return self.settings['fred']['api_key']