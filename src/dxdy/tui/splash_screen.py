# Copyright (C) 2024 Spaghetti Software Inc. (SPGI)

from textual.app import ComposeResult, RenderableType
from textual.screen import Screen
from textual.message import Message
from textual_plotext import PlotextPlot
from textual.renderables.gradient import LinearGradient


from ..settings import Settings

COLORS = [
    "#881177", "#aa3355", "#cc6666", "#ee9944", "#eedd00",
    "#99dd55", "#44dd88", "#22ccbb", "#00bbcc", "#0099cc",
    "#3366bb", "#663399",
]
STOPS = [(i / (len(COLORS) - 1), color) for i, color in enumerate(COLORS)]

class SplashScreen(Screen):
    class InitCompleted(Message):
        def __init__(self) -> None:
            super().__init__()

    fps_rate = 30
    def compose(self) -> ComposeResult:
        self.gradient_rotation_angle_deg = 0
        yield PlotextPlot()

    def render(self) -> RenderableType:
        self.gradient_rotation_angle_deg += 100
        if self.gradient_rotation_angle_deg >= 180 + 4*180:
            self.post_message(self.InitCompleted())
        return LinearGradient(self.gradient_rotation_angle_deg, STOPS)

    def on_mount(self) -> None:
        self.auto_refresh = 1 / self.fps_rate
        plt = self.query_one(PlotextPlot).plt
        plt.image_plot(Settings().get_project_root() / "splash.png")
        plt.clear_color()

    def on_splash_screen_init_completed(self, message: InitCompleted) -> None:
        self.gradient_rotation_angle_deg = 0
        self.auto_refresh = None

    def on_screen_resume(self) -> None:
        self.auto_refresh = 1 / self.fps_rate

