# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)
#
#                    +------------------+
#                    |       App        |
#                    +------------------+
#                            │
#                            ▼
#                    +------------------+
#                    |     Screen       |
#                    +------------------+
#                            │
#                            ▼
#                    +------------------+
#                    |     Widget       |
#                    +------------------+
#                   ╱       │         ╲
#                  ╱        │          ╲
#                 ╱         │           ╲
#  +----------------+  +---------------+  +------------------+
#  |    Static      |  |   Control   │  |   Container      |
#  | (e.g., Header, │  | (Interactive│  | (Layouts & Group)|
#  |  Footer, Label)|  |  elements)  |  |                  |
#  +----------------+  +---------------+  +------------------+
#                       ╱         ╲              │
#                      ╱           ╲             ▼
#             +--------+         +--------+   +---------------+
#             | Button |         |  Input |   |   ScrollView  |
#             +--------+         +--------+   +---------------+
#                                              ╱         ╲
#                                             ╱           ╲
#                                    +---------------+  +--------------+
#                                    |  GridLayout   |  |  DockLayout  |
#                                    +---------------+  +--------------+




import sys


import textual
from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, OptionList, Placeholder
from textual.events import ScreenResume

from .splash_screen import SplashScreen
from .dashboard_screen import DashboardScreen
from .reports_screen import ReportsScreen, ReportsWidget
from .risk_screen import RiskScreen
from .db_screen import DbScreen
from .ticket_screen import TradingTicketScreen
from .log_screen import LogScreen
from .help_screen import HelpScreen
from .ai_screen import AiScreen
from .query_screen import QueryScreen

from ..settings import Settings
from ..db import schema
from ..rtd.rtd_calcs import get_rtd_positions, get_ntp_time
from ..db.utils import get_current_cob_date
from ..tui.tui_utils import DxDyLogMsg

from loguru import logger

# disable default logger
logger.remove()
# add file logger
logger.add("log_dxdy.log", format="{time} {level} {message}", rotation="1 week")
logger.level("DEBUG")

#logger.debug(f"DxDyApp application module loaded")


class DxDyApp(App):
    CSS_PATH = "dxdy_app.tcss"
    
    BINDINGS = [
        ("q", "quit", "Close"),
        ("r", "switch_mode('dashboard')", "Dashboard"),
        ("k", "switch_mode('reports')", "Reports"),
        ("s", "switch_mode('risk')", "Risk"),
        #("i", "switch_mode('ai_screen')", "AI Analyst"),
        ("e", "switch_mode('trade_ticket')", "Data Entry"),
        #("d", "switch_mode('db')", "Database"),
        ("x", "ctrl_key('ctrl+e')", "Copy"),
        # ("y", "switch_mode('query_screen')", "Query"),
        ("o", "switch_mode('log')", "Log"),
        ("h", "switch_mode('help_screen')", "Help"),
    ]
    MODES = {
        "splash": SplashScreen,
        #"db": DbScreen,  
        "dashboard": DashboardScreen,
        "trade_ticket": TradingTicketScreen,
        "reports": ReportsScreen,
        "risk": RiskScreen,
        "log": LogScreen,
        "help_screen": HelpScreen,
        #"ai_screen": AiScreen,
        # "query_screen": QueryScreen,
    }
    

    def compose(self) -> ComposeResult:
        yield Placeholder()

    def on_load(self):
        self.log(f"Python : {sys.version}")
        self.log(f"textual : {textual.__version__}")
        self.log(f"log handlers: {logger._core.handlers}")


    def on_mount(self) -> None:
        #self.title = 'dxdy'
        #self.sub_title = 'v1.0.0'
        self.log_level = "DEBUG"
        #self.theme = "nord"
        #self.theme = "textual-light"
        
        #self.switch_mode("risk")
        #self.switch_mode("reports")
        #self.switch_mode("splash")
        self.switch_mode("dashboard")
        #self.switch_mode("trade_ticket")
        #self.switch_mode("help_screen")
        #self.switch_mode("ai_screen")
        
        #self.switch_mode("ai_screen")
        # self.switch_mode("query_screen")

    def action_ctrl_key(self, key : str) -> None:
        pass

    def on_reports_widget_report_title(self, message: ReportsWidget.ReportTitle) -> None:
        self.sub_title = message.report_title
        self.title = message.report_subtitle

    # def on_screen_resume(self, event : ScreenResume) -> None:
    #     self.log.debug("Log screen resumed")


    def on_splash_screen_init_completed(self, message: SplashScreen.InitCompleted) -> None:
        self.gradient_rotation_angle_deg = 0
        self.switch_mode("dashboard")
        self.log("InitCompleted")
