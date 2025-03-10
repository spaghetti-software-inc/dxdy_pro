# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)


from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Static, Header, Footer, OptionList, Placeholder, Input, Label, Button, Select, TabbedContent, TabPane, ProgressBar
from textual.events import ScreenResume
from textual.containers import Vertical, Horizontal

# import keyring

from rich.traceback import install
install(show_locals=True)

from dxdy.settings import Settings
from dxdy.saas_settings import SaaSConfig

saas_config = SaaSConfig()



class DbConfigScreen(Screen):
    
    def compose(self) -> ComposeResult:
        yield Header(id="Header", icon="⋈", show_clock=False)
        yield Footer(id="Footer")
        yield Static("Database Configuration")

    def on_mount(self) -> None:
        self.title = "dxdy Control Panel"
        self.sub_title = "Database Configuration"
            
            
            
class SmtpConfigScreen(Screen):
    form_data: dict = None
    
    def compose(self) -> ComposeResult:
        smtp_server = saas_config.get_smtp_server()
        smtp_username = saas_config.get_smtp_username()
        
        
        smtp_password = ""
        try:
            smtp_password = keyring.get_password("dxdy_service", "smtp_password")
        except Exception as e:
            pass
        
        
        with Vertical():
            yield Header(id="Header", icon="⋈", show_clock=False)
            with TabbedContent(id="tabbed_content"):
                with TabPane("SMTP Configuration", id="stock_tab"):
                    yield Input(value=smtp_server,   placeholder="SMTP Server", id="smtp_server")
                    yield Input(value=smtp_username, placeholder="SMTP Username", id="smtp_username")
                    yield Input(value=smtp_password, placeholder="SMTP Password", password=True, id="smtp_password")
                    yield Button("Save", id="save_button")
                    
            yield Footer(id="Footer")

    def update_form_data(self) -> None:
        self.form_data = {
            "smtp_server": self.query_one("#smtp_server").value,
            "smtp_username": self.query_one("#smtp_username").value,
            "smtp_password": self.query_one("#smtp_password").value,
        }

    def on_mount(self) -> None:
        self.title = "dxdy Control Panel"
        self.sub_title = "Email Configuration"
        
    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.log(f"Button pressed: {event}")
        
        if event.button.id == "save_button":
            self.update_form_data()
            self.log(f"Form data: {self.form_data}")
            
            saas_config.set_smtp_server(self.form_data['smtp_server'])
            saas_config.set_smtp_username(self.form_data['smtp_username'])
            
            keyring.delete_password("dxdy_service", "smtp_password")    
            keyring.set_password("dxdy_service", "smtp_password", self.form_data['smtp_password'])
            
            #saas_config.set_smtp_password(self.form_data['smtp_password'])
            
            #self.emit(ScreenResume())
    
        
class DxDyControlPanel(App):

    DEFAULT_CSS = """
        Header {
            dock: top;
        }
        Footer {
            dock: bottom;
        }
        Input {
            width: 50%;   
        }
    """

    BINDINGS = [
        ("q", "quit", "Close"),
        ("e", "switch_mode('smtp_config_screen')", "Email"),
        ("d", "switch_mode('db_config_screen')", "Database"),
    ]
    MODES = {
        "smtp_config_screen": SmtpConfigScreen,
        "db_config_screen": DbConfigScreen,  
    }    
    
    def compose(self) -> ComposeResult:
        yield Placeholder()
        
    def on_mount(self) -> None:
        self.log("DxDyControlPanel application module loaded")
        self.switch_mode("smtp_config_screen")
    


if __name__ == "__main__":

    app = DxDyControlPanel()
    app.run()
    
