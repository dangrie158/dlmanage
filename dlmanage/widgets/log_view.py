from pathlib import Path
from typing import List

from rich.syntax import Syntax
from rich.text import Text
from rich.style import Style

from textual.widget import Widget
from textual.messages import CursorMove


class LogView(Widget):
    def __init__(
        self, file: Path, name: str | None = None, lexer: str = "python"
    ) -> None:
        super().__init__(name)
        self.file_path = file
        self.file = file.open("r")
        self.lexer = Syntax("", line_numbers=True, lexer=lexer)
        self.lines: List[Text] = []
        for line in self.file.readlines():
            self.add_line(line)

        self.set_interval(1, self.check_for_new_lines)

        self.set_timer(0, self.scroll_to_bottom)

    def add_line(self, line) -> Text:
        line_number = len(self.lines) + 1
        line_number_color = self.lexer._get_line_numbers_color()
        line_number_text = Text(
            f"{line_number:>6} ", style=Style(color=line_number_color)
        )
        highlighted_line = self.lexer.highlight(line)
        combined_line = line_number_text.append(highlighted_line)
        self.lines.append(combined_line)
        return combined_line

    @property
    def auto_scroll(self):
        # if we're at the bottom, autoscroll if new lines are added
        parent = self.parent
        return (parent.scroll.y + parent.size.height) >= self.parent.virtual_size.height

    async def scroll_to_bottom(self):
        # a cursor move normally moves the selected line to the center, but the
        # scroll view also clamps the value to a maximum value based on the current
        # height, so we can just "overscroll" and pass the full virtual height
        new_scroll_y = self.parent.virtual_size.height
        await self.emit(CursorMove(self, new_scroll_y))

    async def check_for_new_lines(self):
        needs_refresh = False
        while line := self.file.readline():
            self.add_line(line)
            needs_refresh = True

        if needs_refresh:
            self.refresh(layout=True)
            if self.auto_scroll:
                await self.scroll_to_bottom()

    def render(self):
        text = Text().assemble(*self.lines)
        return text
