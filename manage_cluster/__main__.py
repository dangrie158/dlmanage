from rich.style import Style, NULL_STYLE

from textual import events
from textual.app import App
from textual.widgets import Header, ScrollView, TreeControl, TreeClick
from textual.widget import Widget

from widgets import (
    Footer,
    InteractiveTableModel,
    InteractiveTable,
    TableTheme,
)

from models import UserListModel, AccountListModel, JobListModel


class AppTheme(TableTheme):
    # main colors: #DF4356, #9B55A9, #E08D6D
    header = Style(color="#E08D6D")
    hovered_cell = Style(bold=True, underline=True)
    selected_row = Style(bgcolor="gray19")
    choice_cell = Style(color="#9B55A9")
    focused_cell = Style(
        color="#E08D6D", bgcolor="gray30", bold=True, underline=True, overline=True
    )
    focused_border_style = NULL_STYLE
    blurred_border_style = Style(color="gray62")


class SlurmControl(App):
    theme = AppTheme()

    async def on_load(self, event: events.Load) -> None:
        await self.bind("q", "quit", "Quit")
        await self.bind("ctrl+i", "switch_focus", "Switch Focus", key_display="TAB")

    async def on_mount(self, event: events.Mount) -> None:
        header = Header(clock=True, tall=True, style=self.theme.header)
        header.disable_messages(events.Click)
        footer = Footer(style=self.theme.header)

        self.main_content = user_list = InteractiveTable(
            model=UserListModel(), name="UserList", theme=self.theme
        )
        self.main_content.can_focus = True
        self.main_content.border = "round"
        self.main_content_container = ScrollView(user_list)

        self.sidebar_content = model_tree = TreeControl("Models", None)
        self.sidebar_content.border = "round"
        self.sidebar_content.can_focus = True
        self.sidebar_content.focus()

        await model_tree.root.add("Users", UserListModel)
        await model_tree.root.add("Accounts", AccountListModel)
        await model_tree.root.add("Jobs", JobListModel)
        await model_tree.root.expand()

        await self.view.dock(header, edge="top")
        await self.view.dock(footer, edge="bottom")
        await self.view.dock(self.sidebar_content, edge="left", size=20)
        await self.view.dock(self.main_content_container)

    async def set_focus(self, widget: Widget | None) -> None:
        if self.focused == widget:
            return

        if self.focused:
            self.focused.border_style = AppTheme.blurred_border_style
        await super().set_focus(widget)
        if self.focused:
            self.focused.border_style = AppTheme.header

    async def action_switch_focus(self):
        if self.sidebar_content == self.focused:
            await self.main_content.focus()
        else:
            await self.sidebar_content.focus()

    async def handle_tree_click(self, message: TreeClick) -> None:
        model = message.node.data
        if model is not None and issubclass(model, InteractiveTableModel):
            await self.main_content.set_model(model())


if __name__ == "__main__":

    # from manage_cluster.slurmbridge import Account, User
    # x = User.get(user="test")
    # x.parent
    SlurmControl.run(title="Slurm Control", log="slurm_control.log")
