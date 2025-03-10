# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

from datetime import datetime
from zoneinfo import ZoneInfo
import random

import ntplib

from textual.widget import Widget
from textual.widgets import Label
from textual.reactive import reactive
from textual.message import Message

from ..settings import Settings
from ..rtd.rtd_calcs import get_ntp_time


def get_time_icon(local_dt: datetime) -> str:
    if local_dt.weekday() in [5, 6]:
        return '🌃 '
    elif local_dt.hour < 12:
        return '☕ '
    elif local_dt.hour < 13:
        return random.choice(['🌮 ', '🍱 ', '🍪 ', '🍚 ', '🍝 ', '🍲 '])
    elif local_dt.hour <= 15 and local_dt.minute < 59:
        return '☕ '
    elif local_dt.hour <= 16 and local_dt.minute < 30:
        return '🔔 '
    elif local_dt.hour < 20:
        return '🍷 '
    else:
        #return '⁉️ ' if local_dt.weekday() == 4 else '⥁ '
        return '🍷 '

class TimeSync(Message):
    local_tz : ZoneInfo = None
    
    def __init__(self, ntp_stats: ntplib.NTPStats) -> None:
        super().__init__()
        
        self.local_tz = Settings().get_timezone()
        self.local_time_ns, self.local_dt = get_ntp_time(self.local_tz, ntp_stats)
        self.local_dt_str = self.local_dt.isoformat()        
        self.time_icon = get_time_icon(self.local_dt)


class ClockWidget(Widget):
    now_date_time = reactive("00:00:00")
    
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self.ntp_stats = None
        self.ntp = None
        self.leap_warn = ""
        self.ntp = None
        
        self.ntp_client = ntplib.NTPClient()
        self.local_tz = Settings().get_timezone()
        

    def _time_sync(self) -> None:
        self.post_message(TimeSync(self.ntp_stats))

    def on_time_sync(self, message: TimeSync) -> None:
        self.ntp = message

    def on_mount(self) -> None:
        self.ntp_stats = self.ntp_client.request(Settings().get_ntp_server(), version=4)
        self.leap_warn = ""
        if self.ntp_stats.leap == 1:
            self.leap_warn = "▕ θ leap second +1s▕ "
        elif self.ntp_stats.leap == 2:
            self.leap_warn = "▕ θ leap second -1s▕ "
        self._time_sync()
        self.auto_refresh = 1.0

    def render(self) -> str:
        self._time_sync()
        return self.ntp.time_icon + self.ntp.local_dt_str + self.leap_warn

class CustomHeaderWidget(Widget):
    cur_cob_date = None

    def __init__(self, cur_cob_date, cur_trading_date, next_cob_date, **kwargs):
        super().__init__(**kwargs)

        self.cur_cob_date = cur_cob_date
        self.cur_trading_date = cur_trading_date
        self.next_cob_date = next_cob_date

    def compose(self):
        header = f"⋈ - COB (T-1) {self.cur_cob_date} | T0 {self.cur_trading_date} | T+1 {self.next_cob_date}"
        yield Label(header , classes='logo_header_label')
        yield ClockWidget(id="clock_widget", classes='time_header_label')
