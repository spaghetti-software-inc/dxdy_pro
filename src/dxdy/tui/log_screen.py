# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

from loguru import logger


from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, RichLog



class LogScreen(Screen):
    log_widget = None
    
    def compose(self) -> ComposeResult:
        self.log_widget = RichLog()
        
        yield Header(id="Header", icon="⋈", show_clock=False)
        yield self.log_widget
        yield Footer(id="Footer")

    def on_screen_resume(self) -> None:
        # clear the log widget
        self.log_widget.clear()
        
        # read the log file and display it
        with open("log_dxdy.log", "r") as log_file:
            for line in log_file:
                self.log_widget.write(line)
        


    # def log_msg(self, content):
    #     self.log_widget.write(content)
        