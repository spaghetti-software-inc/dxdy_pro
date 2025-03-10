# Copyright (C) 2024-2025 Spaghetti Software Inc. (SPGI)

from enum import Enum
import datetime

import pandas as pd

from textual.app import App, ComposeResult
from textual.screen import Screen
from textual.reactive import reactive
from textual.widgets import Header, Footer, Input, Label, Button, Select, TabbedContent, TabPane, ProgressBar, TextArea, Placeholder
from textual.suggester import SuggestFromList
from textual.containers import Vertical, Horizontal
from textual.color import Color, Gradient
from textual.widget import Widget

from loguru import logger

from ..ai.sql_programmer import SqlProgramer
from .db_screen import DuckDbTable
from ..settings import Settings
from ..db.utils import DuckDBTemporaryTable

class QueryScreen(Screen):
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.table = None
        self.user_input = None
        self.text_area = None
        
        self.sql_programmer = SqlProgramer()

    def compose(self) -> ComposeResult:
        self.table = DuckDbTable(id="query_table")
        self.user_input = Input(id="query_input")
        self.text_area = TextArea.code_editor(language="sql", id="sql_textarea")

        yield Header("Query Screen")
        yield Footer(id="Footer")
 
            
        with TabbedContent(id="tabbed_content"):
                            
            with TabPane("SQL", id="sql_tab"):
                
                with Vertical():
                    with Horizontal(id="query_prompt"):
                        yield Label("Enter your query:", id = "query_prompt_label")
                        
                        yield self.user_input
                        yield Button("Run Query", id="run_button")
                    yield self.text_area 
                    
            with TabPane("Results", id="results_tab"):
                yield self.table
    
    async def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "run_button":
            user_query = self.user_input.value
            
            sql_query = self.sql_programmer.generate_sql(user_query)
            
            self.text_area.text = sql_query
            self.table.set_sql_query(sql_query)