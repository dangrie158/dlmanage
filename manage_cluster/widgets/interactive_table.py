from __future__ import annotations

from contextlib import suppress
from typing import (
    Any,
    Dict,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from abc import ABC, abstractmethod

import rich
from rich.style import Style, NULL_STYLE
from rich.table import Table
from rich.text import Text
from rich.padding import PaddingDimensions
from rich.console import RenderableType

from textual import events
from textual.reactive import Reactive
from textual.widget import Widget
from textual.message import Message, MessageTarget
from textual.messages import CursorMove

TablePosition = NamedTuple("TablePosition", (("column", str), ("row", int)))


class TableTheme:
    cell = Style(color="gray62")
    text_cell = Style(color="white")
    int_cell = Style(color="green")
    choice_cell = Style(color="green3")
    hovered_cell = Style(bold=True)
    hovered_editable_cell = Style(bold=True, underline=True)
    selected_row = Style(bgcolor="gray19")
    focused_cell = Style(color="#E08D6D", bgcolor="gray30", bold=True)
    disabled_row = Style(italic=True, color="grey50")
    editing_cell = Style(color="#E08D6D", bgcolor="gray30", bold=True, underline=False)
    cursor = Style(reverse=True, blink=True, underline=True)
    undefined_value = Style(italic=True, color="grey50")


class TableCell(Widget):
    is_focused: Reactive[bool] = Reactive(False, layout=True)
    is_hovered: Reactive[bool] = Reactive(False, layout=True)

    def __init__(
        self,
        position: TablePosition,
        text: Optional[str] = None,
        *,
        style: Optional[Style] = None,
        theme: Optional[TableTheme] = None,
        placeholder: str = "<undefined>",
    ):
        super().__init__(f"TableCell<{position.column}:{position.row}>")
        self.position = position
        self.placeholder = placeholder
        self.text = text
        self.theme = theme or TableTheme()
        style = style or self.theme.cell
        self.base_style = style + Style(
            meta={"column": self.position.column, "row": self.position.row}
        )

    def render(self) -> RenderableType:
        style = NULL_STYLE
        text = self.text

        if text is None:
            style += self.theme.undefined_value
            text = self.placeholder

        if self.is_focused:
            style += self.theme.focused_cell
        elif self.is_hovered and isinstance(self, EditableTableCell):
            style += self.theme.hovered_editable_cell
        elif self.is_hovered:
            style += self.theme.hovered_cell

        return Text(text, style=self.base_style + style)

    def focus(self):
        self.is_focused = True

    def unfocus(self):
        self.is_focused = False

    def hover_enter(self):
        self.is_hovered = True

    def hover_leave(self):
        self.is_hovered = False

    async def on_key(self, event: events.Key) -> None:
        pass


class EditableTableCell(TableCell):
    is_editing: Reactive[bool] = Reactive(False, layout=True)
    cursor_position: Reactive[int] = Reactive(0, layout=True)
    value: Reactive[str] = Reactive("", layout=True)

    def __init__(
        self,
        position: TablePosition,
        text: Optional[str] = None,
        *,
        style: Optional[Style] = None,
        theme: Optional[TableTheme] = None,
        placeholder: str = "<undefined>",
    ):
        theme = theme or TableTheme()
        style = style or theme.text_cell
        super().__init__(
            position, text, style=style, theme=theme, placeholder=placeholder
        )

    async def begin_editing(self):
        self.value = self.text or " "
        self.is_editing = True

    async def abort_edit(self):
        self.is_editing = False
        await self.emit(CellFinishedEditing(self, self.position))

    async def commit_edit(self):
        self.is_editing = False

        # to_value can throw to indicate an invalid value. only send an event if the conversion succeeds
        try:
            new_value = self.to_value()
        except Exception:
            pass
        else:
            await self.emit(CellEdited(self, self.position, new_value))

        await self.emit(CellFinishedEditing(self, self.position))

    def to_value(self) -> Any:
        return self.value or None

    def render(self) -> RenderableType:
        if self.is_editing:
            style = self.theme.editing_cell
            text = self.value
            cell_content = Text(text, style=self.base_style + style)
            cell_content.stylize(
                self.theme.cursor, self.cursor_position, self.cursor_position + 1
            )
            return cell_content
        return super().render()

    def input_key(self, key_name: str) -> None:
        self.cursor_position += 1
        self.value = (
            self.value[0 : self.cursor_position - 1]
            + key_name
            + self.value[self.cursor_position - 1 :]
        )

    async def on_key(self, event: events.Key) -> None:
        if not self.is_editing:
            return

        event.stop()
        match (event.key):
            case "ctrl+h":  # backspace
                self.value = (
                    self.value[0 : self.cursor_position - 1]
                    + self.value[self.cursor_position :]
                )
                self.cursor_position -= 1
            case "delete":
                self.value = (
                    self.value[0 : self.cursor_position]
                    + self.value[self.cursor_position + 1 :]
                )
            case "enter":
                await self.commit_edit()
            case "escape":
                await self.abort_edit()
            case "up":
                self.cursor_position = 0
            case "down":
                self.cursor_position = len(self.value)
            case "left":
                if self.cursor_position > 0:
                    self.cursor_position -= 1
            case "right":
                if self.cursor_position < len(self.value) - 1:
                    self.cursor_position += 1
            case other:
                if other.isprintable() and len(other) == 1:
                    self.input_key(other)


class EditableIntTableCell(EditableTableCell):
    def __init__(
        self,
        position: TablePosition,
        text: Optional[str] = None,
        *,
        style: Optional[Style] = None,
        theme: Optional[TableTheme] = None,
        placeholder: str = "<undefined>",
        max_val: Optional[int] = None,
        min_val: Optional[int] = None,
    ):
        self.min_val = min_val
        self.max_val = max_val

        theme = theme or TableTheme()
        style = style or theme.int_cell
        super().__init__(
            position, text, style=style, theme=theme, placeholder=placeholder
        )

    async def begin_editing(self):
        self.value = self.text or ""
        self.cursor_position = len(self.value)
        self.is_editing = True

    def to_value(self) -> Any:
        value = int(self.value)
        if self.max_val is not None:
            value = min(self.max_val, value)
        if self.min_val is not None:
            value = max(self.min_val, value)
        return value

    def input_key(self, key_name: str) -> None:
        match (key_name):
            case "+":
                with suppress(ValueError):
                    if self.max_val is None or self.to_value() < self.max_val:
                        self.value = str(int(self.value) + 1)
            case "-":
                with suppress(ValueError):
                    if self.min_val is None or self.to_value() > self.min_val:
                        self.value = str(int(self.value) - 1)
            case other:
                if other.isnumeric():
                    super().input_key(other)


class EditableChoiceTableCell(EditableTableCell):
    def __init__(
        self,
        position: TablePosition,
        text: Optional[str] = None,
        *,
        style: Optional[Style] = None,
        theme: Optional[TableTheme] = None,
        placeholder: str = "<undefined>",
        choices: Optional[Sequence[str]] = None,
    ):
        self.choices = choices or [text]

        theme = theme or TableTheme()
        style = style or theme.choice_cell
        super().__init__(
            position, text, style=style, theme=theme, placeholder=placeholder
        )

    def to_value(self):
        return self.choices[self.cursor_position]

    def render(self) -> RenderableType:
        if self.is_editing:
            max_choice_len = max(len(choice) for choice in self.choices)
            choice_list = Text()
            for index, choice in enumerate(self.choices):
                style = NULL_STYLE
                if index == self.cursor_position:
                    style += self.theme.cursor
                if choice == self.value:
                    style += self.theme.choice_cell

                selector = "◉" if choice == self.value else "○"
                choice_list += Text(
                    f"{selector} {choice.rjust(max_choice_len)}\n", style
                )

            return choice_list
        return super().render()

    async def begin_editing(self):
        await super().begin_editing()
        try:
            self.cursor_position = self.choices.index(self.text)
        except ValueError:
            self.cursor_position = 0

    def input_key(self, key_name: str) -> None:
        # jump to the first choice matching the entered key
        if key_name.isprintable() and len(key_name) == 1:
            matching_choices = [
                index
                for index, choice in enumerate(self.choices)
                if choice.startswith(key_name)
            ]
            if len(matching_choices) > 0:
                self.cursor_position = matching_choices[0]
        return

    async def on_key(self, event: events.Key) -> None:
        if not self.is_editing:
            return

        event.stop()
        match (event.key):
            case "up":
                if self.cursor_position > 0:
                    self.cursor_position -= 1
            case "down":
                if self.cursor_position < len(self.choices) - 1:
                    self.cursor_position += 1
            case "left" | "right":
                return
            case _:
                await super().on_key(event)


@rich.repr.auto
class CellEdited(Message):
    def __init__(
        self, sender: MessageTarget, position: TablePosition, new_content: str
    ) -> None:
        self.position = position
        self.new_content = new_content
        super().__init__(sender)

    def __rich_repr__(self) -> rich.repr.Result:
        yield "position", self.position
        yield "new_content", self.new_content


@rich.repr.auto
class CellFinishedEditing(Message):
    def __init__(self, sender: MessageTarget, position: TablePosition) -> None:
        self.position = position
        super().__init__(sender)

    def __rich_repr__(self) -> rich.repr.Result:
        yield "position", self.position
        yield "new_content", self.new_content


class InteractiveTableModel(ABC):
    title: str

    @abstractmethod
    def get_columns(self) -> Sequence[str]:
        ...

    @abstractmethod
    def get_num_rows(self) -> int:
        ...

    @abstractmethod
    def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        ...

    @abstractmethod
    def on_row_delete(self, position: TablePosition) -> None:
        ...

    @abstractmethod
    def on_row_add(self, position: TablePosition) -> None:
        ...

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return {}

    def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        return TableCell, {}

    @abstractmethod
    def get_cell(self, position: TablePosition) -> str:
        ...

    def get_primary_column(self) -> str:
        return list(self.get_columns())[0]

    def is_cell_editable(self, position: TablePosition) -> bool:
        try:
            cell_class, _ = self.get_cell_class(position)
        except:
            return False
        return issubclass(cell_class, EditableTableCell)

    def get_editable_columns(self, row: int) -> Sequence[str]:
        return [
            column_name
            for column_name in self.get_columns()
            if self.is_cell_editable(TablePosition(column_name, row))
        ]

    def get_next_colum(self, position: Optional[TablePosition]) -> str | None:
        row = position.row if position is not None else 0
        columns = self.get_editable_columns(row)

        if position is None or not self.is_cell_editable(position):
            return columns[0] if len(columns) > 0 else None

        try:
            current_index = columns.index(position.column)
            if current_index < len(columns) - 1:
                return columns[current_index + 1]
        except ValueError:
            pass
        return None

    def get_previous_colum(self, position: Optional[TablePosition]) -> str | None:
        row = position.row if position is not None else 0
        columns = self.get_editable_columns(row)

        if position is None or not self.is_cell_editable(position):
            return columns[-1] if len(columns) > 0 else None
        try:
            current_index = columns.index(position.column)
            if current_index > 0:
                return columns[current_index - 1]
        except ValueError:
            pass
        return None

    def get_next_row_matching(self, current_row: int, needle: str) -> int | None:
        search_column = self.get_primary_column()

        total_rows = self.get_num_rows()
        candidates = list(range(current_row + 1, total_rows))
        candidates += list(range(0, current_row))
        for row in candidates:
            value = self.get_cell(TablePosition(search_column, row))
            if value.startswith(needle):
                return row
        return None


class InteractiveTable(Widget):
    selection_position: Reactive[Optional[TablePosition]] = Reactive(None, layout=True)
    hover_position: Reactive[Optional[TablePosition]] = Reactive(None, layout=True)
    is_in_edit_mode: Reactive[bool] = Reactive(False)

    def __init__(
        self,
        model: InteractiveTableModel,
        *,
        name: str | None = None,
        padding: PaddingDimensions = (1, 1),
        theme: TableTheme = TableTheme(),
    ) -> None:
        self.theme = theme

        self.cells: Dict[TablePosition, TableCell] = {}
        self.columns: Sequence[str] = []
        self.num_rows: int = 0
        self.model = model
        self.refresh_data_from_model()

        super().__init__(name=name)
        self.padding = padding

    async def set_model(self, new_model: InteractiveTableModel):
        self.model = new_model
        self.refresh_data_from_model()
        self.refresh()

    def refresh_data_from_model(self):
        # TODO: handle removing of old data
        #       and updating only cell content instead of the complete object
        self.num_rows = self.model.get_num_rows()
        self.columns = self.model.get_columns()
        for column in self.model.get_columns():
            for row in range(self.num_rows):
                current_position = TablePosition(column, row)
                cell_text = self.model.get_cell(current_position)
                cell_class, cell_kwargs = self.model.get_cell_class(current_position)
                cell = cell_class(
                    current_position, cell_text, theme=self.theme, **cell_kwargs
                )
                for attr_name, attr_value in cell_kwargs.items():
                    setattr(cell, attr_name, attr_value)
                cell.set_parent(self)
                self.cells[current_position] = cell

    def watch_selection_position(
        self, old_value: Optional[TablePosition], new_value: Optional[TablePosition]
    ) -> None:
        if old_value is not None and old_value in self.cells:
            self.cells[old_value].unfocus()

        if new_value is not None and new_value in self.cells:
            self.emit_no_wait(CursorMove(self, new_value.row + self.gutter.top))
            self.cells[new_value].focus()

    def watch_hover_position(
        self, old_value: Optional[TablePosition], new_value: Optional[TablePosition]
    ) -> None:
        if old_value is not None and old_value in self.cells:
            self.cells[old_value].hover_leave()
        if new_value is not None and new_value in self.cells:
            self.cells[new_value].hover_enter()

    def render(self) -> Table:
        table = Table(expand=True, highlight=True)
        for column in self.columns:
            column_kwargs = self.model.get_column_kwargs(column)
            table.add_column(column, **column_kwargs)

        for row in range(self.num_rows):
            row_style: Style = None
            if self.selection_position and self.selection_position.row == row:
                row_style = self.theme.selected_row
            elif self.is_in_edit_mode:
                row_style = self.theme.disabled_row
            else:
                row_style = NULL_STYLE

            cells = []
            for column in self.columns:
                current_position = TablePosition(column, row)
                cell = self.cells[current_position]
                cells.append(cell.render())
            table.add_row(*cells, style=row_style)
        return table

    async def on_click(self, event: events.Click) -> None:
        if self.is_in_edit_mode:
            return
        column, row = event.style.meta.get("column"), event.style.meta.get("row")
        if None in (column, row):
            self.selection_position = None
            return

        self.selection_position = TablePosition(column, row)

    async def on_mouse_move(self, event: events.MouseMove) -> None:
        column, row = event.style.meta.get("column"), event.style.meta.get("row")
        if None in (column, row):
            self.hover_position = None
            return

        self.hover_position = TablePosition(column, row)

    async def on_key(self, event: events.Key) -> None:
        if (
            self.selection_position is not None
            and self.selection_position in self.cells
        ):
            # give the cell a chance to handle the event
            active_cell = self.cells[self.selection_position]
            await active_cell.on_key(event)
            if event._stop_propagation:
                self.refresh()
                return

        match (event.key):
            case "up":
                if self.selection_position is None:
                    self.selection_position = TablePosition("", self.num_rows - 1)
                    return

                if self.selection_position.row > 0:
                    self.selection_position = TablePosition(
                        self.selection_position.column, self.selection_position.row - 1
                    )
            case "down":
                if self.selection_position is None:
                    self.selection_position = TablePosition("", 0)
                    return

                if self.selection_position.row < self.model.get_num_rows() - 1:
                    self.selection_position = TablePosition(
                        self.selection_position.column, self.selection_position.row + 1
                    )
            case "left":
                if self.selection_position is None:
                    return

                previous_column = self.model.get_previous_colum(self.selection_position)
                if previous_column is not None:
                    self.selection_position = TablePosition(
                        previous_column, self.selection_position.row
                    )
            case "right":
                if self.selection_position is None:
                    return

                next_column = self.model.get_next_colum(self.selection_position)
                if next_column is not None:
                    self.selection_position = TablePosition(
                        next_column, self.selection_position.row
                    )
            case "enter":
                if (
                    self.selection_position is not None
                    and self.selection_position in self.cells
                ):
                    cell = self.cells[self.selection_position]
                    if isinstance(cell, EditableTableCell):
                        await cell.begin_editing()
                        self.refresh()
                        self.is_in_edit_mode = True
            case other:
                if other.isalnum() and len(other) == 1:
                    current_position = (
                        self.selection_position
                        if self.selection_position is not None
                        else TablePosition("", 0)
                    )
                    next_row = self.model.get_next_row_matching(
                        current_position.row, other
                    )
                    if next_row is not None:
                        self.selection_position = TablePosition(
                            current_position.column, next_row
                        )

    async def handle_cell_finished_editing(self, event: CellFinishedEditing):
        self.is_in_edit_mode = False

    async def handle_cell_edited(self, event: CellEdited):
        self.model.on_cell_update(event.position, event.new_content)
        self.refresh_data_from_model()
        self.refresh()
