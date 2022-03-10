from datetime import datetime
from typing import Any, Dict, Mapping, Sequence, Tuple, Type
from rich.style import Style, NULL_STYLE
from textual import events
from textual.app import App
from textual.widgets import Header, ScrollView


from widgets import (
    Footer,
    InteractiveTableModel,
    InteractiveTable,
    TablePosition,
    TableTheme,
    EditableTableCell,
    TableCell,
    EditableChoiceTableCell,
    EditableIntTableCell,
)
from slurmbridge import User, Account


def relative_time(date: datetime) -> str:
    return str(date)


class UserListModel(InteractiveTableModel):
    title = "All Users"

    def __init__(self):
        self._data = User.all()
        self._available_accounts = Account.all()
        self._columns = {
            "user": {},
            "account": {},
            "CPUs": {"justify": "right"},
            "GPUs": {"justify": "right"},
            "Timelimit": {"justify": "right"},
            "Home Directory": {"justify": "right"},
        }

    def get_columns(self) -> Sequence[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    def get_cell(self, position: TablePosition) -> Tuple[str, Style]:
        column, row = position
        row_data = self._data[row]
        cell_text: str | None = None
        cell_style = NULL_STYLE
        match column:
            case "user":
                cell_text = row_data.user
                editable = False
            case "account":
                cell_text = row_data.default_account
            case "CPUs":
                cell_text = row_data.max_cpus
            case "GPUs":
                cell_text = row_data.max_gpus
            case "Timelimit":
                cell_text = row_data.grp_wall
            case "Home Directory":
                if row_data.home_directory is None:
                    cell_text = "<home dir does not exist>"
                    cell_style = AppTheme.undefined_value
                else:
                    cell_text = row_data.home_directory
                editable = False
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

        return cell_text, cell_style

    def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        match position.column:
            case "user" | "Home Directory":
                return TableCell, {}
            case "account":
                return EditableChoiceTableCell, {
                    "choices": [account.account for account in self._available_accounts]
                }
            case "CPUs":
                return EditableIntTableCell, {"min_val": 0}
            case "GPUs":
                return EditableIntTableCell, {"min_val": 0}
            case "Timelimit":
                return EditableTableCell, {}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")


class AppTheme(TableTheme):
    # main colors: #DF4356, #9B55A9, #E08D6D
    header = Style(color="#E08D6D")
    hovered_cell = Style(bold=True, underline=True)
    selected_row = Style(bgcolor="gray19")
    focused_cell = Style(
        color="#E08D6D", bgcolor="gray30", bold=True, underline=True, overline=True
    )


class SlurmControl(App):
    theme = AppTheme()

    async def on_load(self, event: events.Load) -> None:
        await self.bind("q", "quit", "Quit")

    async def on_mount(self, event: events.Mount) -> None:
        user_list = InteractiveTable(
            model=UserListModel(), name="UserList", theme=self.theme
        )
        scroll_view = ScrollView(user_list)
        header = Header(clock=True, tall=False, style=self.theme.header)
        footer = Footer(style=self.theme.header)
        await self.view.dock(header, edge="top")
        await self.view.dock(footer, edge="bottom")
        await self.view.dock(scroll_view)


# Run our app class
SlurmControl.run(title="Slurm Control", log="slurm_control.log")
