from __future__ import annotations
from operator import invert

from rich.console import RenderableType
from rich.style import Style
from rich.text import Text
import rich.repr

from textual import events
from textual.message import Message, MessageTarget
from textual.reactive import Reactive
from textual.widget import Widget


@rich.repr.auto
class PromptResponse(Message):
    def __init__(
        self, sender: MessageTarget, response: str, confirmed: bool = True
    ) -> None:
        self.response = response
        self.confirmed = confirmed
        super().__init__(sender)

    def __rich_repr__(self) -> rich.repr.Result:
        yield "response", self.response
        yield "confirmed", self.confirmed


@rich.repr.auto
class ErrorDismissed(Message):
    pass


@rich.repr.auto
class Footer(Widget):
    highlight_key: Reactive[str | None] = Reactive(None)
    prompt_message: Reactive[str | None] = Reactive(None, layout=True)
    error_message: Reactive[str | None] = Reactive(None, layout=True)
    prompt_response: Reactive[str] = Reactive("", layout=True)
    is_awaiting_confirm: bool = False

    def __init__(self, style="white on dark_green") -> None:
        self.keys: list[tuple[str, str]] = []
        super().__init__()
        self.layout_size = 1
        self._key_text: Text | None = None
        self.style = style

    async def watch_highlight_key(self, value) -> None:
        """If highlight key changes we need to regenerate the text."""
        self._key_text = None

    async def on_mouse_move(self, event: events.MouseMove) -> None:
        """Store any key we are moving over."""
        self.highlight_key = event.style.meta.get("key")

    async def on_leave(self, event: events.Leave) -> None:
        """Clear any highlight when the mouse leave the widget"""
        self.highlight_key = None

    def __rich_repr__(self) -> rich.repr.Result:
        yield "keys", self.keys

    def make_key_text(self) -> Text:
        """Create text containing all the keys."""
        text = Text(
            style=self.style,
            no_wrap=True,
            overflow="ellipsis",
            justify="left",
            end="",
        )
        for binding in self.app.bindings.shown_keys:
            key_display = (
                binding.key.upper()
                if binding.key_display is None
                else binding.key_display
            )
            hovered = self.highlight_key == binding.key
            key_text = Text.assemble(
                (f" {key_display} ", "reverse" if hovered else "default on default"),
                f" {binding.description} ",
                meta={"@click": f"app.press('{binding.key}')", "key": binding.key},
            )
            text.append_text(key_text)
        return text

    def refresh(self, repaint: bool = True, layout: bool = False) -> None:
        self._key_text = None
        return super().refresh(repaint, layout)

    def render(self) -> RenderableType:
        if self.error_message is not None:
            return Text(
                f"{self.error_message}",
                style=self.style + Style(reverse=True),
                justify="center",
            )
        if self.prompt_message is not None:
            return Text(
                f"{self.prompt_message}\t{self.prompt_response}",
                style=self.style + Style(reverse=True),
                justify="left",
            )
        if self._key_text is None:
            self._key_text = self.make_key_text()
        return self._key_text

    async def on_key(self, event: events.Key):
        # clear the error on every keystroke
        if self.error_message is not None:
            await self.emit(ErrorDismissed(self))
            self.error_message = None

        if self.prompt_message is None:
            return

        match event.key:
            case "enter":
                event.stop()
                await self._finish_prompt(confirm=True)
            case "escape":
                event.stop()
                await self._finish_prompt(confirm=False)
            case "ctrl+h":
                event.stop()
                self.prompt_response = self.prompt_response[:-1]
            case other:
                if self.is_awaiting_confirm:
                    if other in ("Y", "y"):
                        await self._finish_prompt(confirm=True)
                    elif other in ("N", "n"):
                        await self._finish_prompt(confirm=False)
                    else:
                        self.console.bell()
                elif other.isprintable() and len(other) == 1:
                    event.stop()
                    self.prompt_response += event.key

    def prompt(self, message: str):
        self.prompt_response = ""
        self.prompt_message = f"{message} [ESC to cancel]"

    def confirm(self, message: str):
        self.is_awaiting_confirm = True
        self.prompt_response = ""
        self.prompt_message = f"{message} [Y/n/esc]:"

    def show_error(self, message: str):
        self.error_message = message
        self.console.bell()

    async def _finish_prompt(self, confirm: bool):
        await self.emit(PromptResponse(self, self.prompt_response, confirm))
        self.is_awaiting_confirm = False
        self.prompt_message = None
        self.prompt_response = ""
        self.refresh()
