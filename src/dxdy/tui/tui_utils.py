# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

   
import math
from datetime import datetime

import pandas as pd

from rich.text import Text
from textual.message import Message
from textual.widgets import Input

# Utility classes
class CtrlKey(Message):
    def __init__(self, key: str) -> None:
        self.key = key
        super().__init__()



class DxDyLogMsg(Message):
    content = None
    def __init__(self, content) -> None:
        super().__init__()
        self.content = content
   
def format_currency(value: float) -> str:
    if value < 0:
        return '({0:,.2f})'.format(abs(value))
    else:
        return '{0:,.2f}'.format(value)
    
    
def format_datetime_microseconds(dt: datetime) -> str:
    microsecond_frac = dt.microsecond / 1e6
    microsecond_frac_str = str(microsecond_frac)[2:4]
    return microsecond_frac_str


def format_data_table_cell(data_type : str, value, style = None) -> Text:    
    # numeric data types are right justified to align on the decimal point
    # string data types are left justified in left-to-right language scripts
    if pd.isnull(value) or value is None:
        return Text('')

    if data_type in ['int+', 'quote $+', 'portfolio $+']:
        if math.isnan(value):
            return Text('')
        
        justify = 'right'
        
        if data_type == 'int+':
            cell_str = f"{value:,.0f}"
            
        elif data_type in ['quote $+', 'portfolio $+']:
            cell_str = f"{value:,.2f}"
            
        if style is not None and len(style) == 1:
            return Text(cell_str, style=style, justify=justify)
        else:
            return Text(cell_str, justify=justify)
        
    elif data_type in ['int', '%', 'quote $', 'portfolio $', 'price $']:
        if math.isnan(value):
            return Text('')

        justify = 'right'
        
        if data_type == 'int':
            if value >= 0:        
                cell_str = f"{value:,.0f}"
            else:
                cell_str = f"({abs(value):,.0f})"
            
        elif data_type == '%':
            cell_str = f"{100 * value:,.2f}%"

        elif data_type == 'price $':
            cell_str = f"{value:,.4f}"
                        
        elif data_type in ['quote $', 'portfolio $']:
            if value >= 0:
                cell_str = f"{value:,.2f}"
            else:
                cell_str = f"({abs(value):,.2f})"
            
        if style is not None and len(style) == 2:
            if value >= 0:
                return Text(cell_str, style=style[1], justify=justify)
            else:
                return Text(cell_str, style=style[0], justify=justify)
        else:
            return Text(cell_str, justify=justify)
        
    elif data_type == 'string':
        justify = 'left'
        if style is not None and len(style) == 1:
            return Text(value, style=style, justify=justify)
        else:
            return Text(value, justify=justify)
        
    elif data_type == 'date':
        
        justify = 'center'
        if style is not None and len(style) == 1:
            return Text(value.strftime('%Y-%m-%d'), style=style, justify=justify)
        else:
            return Text(value.strftime('%Y-%m-%d'), justify=justify)
    else:
        raise ValueError(f"Unknown data type {data_type}")
    
