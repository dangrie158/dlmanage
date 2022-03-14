from rich.style import Style, NULL_STYLE

from textual import events
from textual.app import App
from textual.widgets import Header, ScrollView, TreeControl, TreeClick
from textual.widget import Widget
from textual.binding import NoBinding

from dlmanage.widgets import (
    Footer,
    InteractiveTableModel,
    InteractiveTable,
    TableTheme,
)

from dlmanage.models import AssociationListModel, JobListModel
from dlmanage.widgets.footer import PromptResponse


class AppTheme(TableTheme):
    # main colors: #007AD0, #17825D, #CD652A

    cell = Style(color="bright_white")
    header = Style(color="#CD652A")
    text_cell = Style(color="#007AD0")
    int_cell = Style(color="#17825D")
    hovered_cell = Style(bold=True, underline=True)
    selected_row = Style(bgcolor="gray19")
    choice_cell = Style(color="#007AD0")
    focused_cell = Style(
        color="#17825D", bgcolor="gray30", bold=True, underline=True, overline=True
    )
    editing_cell = Style(color="green", bgcolor="gray30", bold=True, underline=False)
    focused_border_style = NULL_STYLE
    blurred_border_style = Style(color="gray62")


class SlurmControl(App):
    theme = AppTheme()
    model: InteractiveTableModel
    footer: Footer

    async def on_load(self, event: events.Load) -> None:
        await self.bind("q", "quit", "Quit")
        await self.bind("ctrl+i", "switch_focus", "Switch Focus", key_display="TAB")
        await self.bind("enter", "edit", "Edit")

    async def on_mount(self, event: events.Mount) -> None:
        self.header = Header(clock=True, tall=True, style=self.theme.header)
        self.header.disable_messages(events.Click)
        self.footer = Footer(style=self.theme.header)

        self.main_content = object_list = InteractiveTable(
            name="ObjectList", theme=self.theme
        )
        self.main_content.can_focus = True
        self.main_content_container = ScrollView(object_list)

        self.sidebar_content = model_tree = TreeControl("Models", None)
        self.sidebar_content.border = "round"
        self.sidebar_content.can_focus = True

        await model_tree.root.add(AssociationListModel.title, AssociationListModel)
        await model_tree.root.add("Jobs", JobListModel)
        await model_tree.root.expand()

        await self.view.dock(self.header, edge="top")
        await self.view.dock(self.footer, edge="bottom")
        await self.view.dock(self.sidebar_content, edge="left", size=27)
        await self.view.dock(self.main_content_container)

        # initially set the focus to the sidebar so we can steal it after loading the model
        await self.set_focus(self.sidebar_content)
        await self.load_model(AssociationListModel(self))

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

    async def load_model(self, model: InteractiveTableModel) -> None:
        self.main_content.is_loading = True
        self.model = model
        if model is not None:
            self._action_targets.add("model")
            for binding in model.keys.values():
                await self.bind(
                    keys=binding.key,
                    action=f"model.{binding.action}",
                    description=binding.description,
                    key_display=binding.key_display,
                )
                self.footer.refresh()
        else:
            self._action_targets.remove("model")

        await model.load_data()
        self.main_content.model = model
        await self.set_focus(self.main_content_container)
        self.main_content.is_loading = False

    async def handle_tree_click(self, message: TreeClick) -> None:
        if not self.sidebar_content.can_focus:
            return
        model_class = message.node.data
        if model_class is not None and issubclass(model_class, InteractiveTableModel):
            model = model_class(self)
            await self.load_model(model)

    async def prompt(self, message: str):
        await self.set_focus(self.footer)
        self.main_content.can_focus = False
        self.sidebar_content.can_focus = False
        self.footer.prompt(message)

    async def confirm(self, message: str):
        await self.set_focus(self.footer)
        self.main_content.can_focus = False
        self.sidebar_content.can_focus = False
        self.footer.confirm(message)

    async def display_error(self, error: Exception):
        self.footer.show_error(str(error))

    async def handle_prompt_response(self, message: PromptResponse):
        await self.set_focus(self.main_content)
        self.main_content.can_focus = True
        self.sidebar_content.can_focus = True


if __name__ == "__main__":
    SlurmControl.run(title="Slurm Control")
