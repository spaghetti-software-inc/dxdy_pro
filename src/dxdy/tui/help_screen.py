# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

from loguru import logger


from textual.app import ComposeResult
from textual.screen import Screen
from textual.widgets import Header, Footer, MarkdownViewer, TabbedContent, TabPane
from textual.containers import Vertical, Horizontal

from ..settings import Settings

class HelpScreen(Screen):
    
    def compose(self) -> ComposeResult:
        
        database_help_file = Settings().get_project_root() / "help_database.md"        
        with open(database_help_file, "r", encoding="utf8") as f:
            markdown_str = f.read()
        self.database_help = MarkdownViewer(markdown_str, show_table_of_contents=True)
        
        user_guide_help_file = Settings().get_project_root() / "help_user_guide.md"        
        with open(user_guide_help_file, "r", encoding="utf8") as f:
            markdown_str = f.read()
        self.user_guide_help = MarkdownViewer(markdown_str, show_table_of_contents=True)       
        
        yield Header(id="Header", icon="⋈", show_clock=False)
        with Vertical():
            with TabbedContent(id="tabbed_content"):
                with TabPane("User Guide", id="user_guide_help_tab"):
                    yield self.user_guide_help
                    
                with TabPane("Database", id="database_help_tab"):
                    yield self.database_help

        yield Footer(id="Footer")


        


    # def log_msg(self, content):
    #     self.log_widget.write(content)
        