from rich.style import Style, NULL_STYLE

from textual import events
from textual._timer import Timer
from textual.app import App
from textual.widgets import Header, ScrollView, TreeControl, TreeClick
from textual.widget import Widget

from textual import actions

from dlmanage.widgets import (
    Footer,
    InteractiveTableModel,
    InteractiveTable,
    TableTheme,
)

from dlmanage.models import AssociationListModel, JobListModel, NodeListModel
from dlmanage.widgets.footer import ErrorDismissed, PromptResponse


class AppTheme(TableTheme):
    # main colors: #007AD0, #729900, #CD652A

    cell = Style(color="bright_white")
    header = Style(color="#CD652A")
    text_cell = Style(color="#007AD0")
    int_cell = Style(color="#729900")
    hovered_cell = Style(bold=True, underline=True)
    selected_row = Style(bgcolor="gray19")
    choice_cell = Style(color="#007AD0")
    focused_cell = Style(
        color="#729900", bgcolor="gray30", bold=True, underline=True, overline=True
    )
    editing_cell = Style(color="green", bgcolor="gray30", bold=True, underline=False)
    focused_border_style = NULL_STYLE
    blurred_border_style = Style(color="gray62")


class SlurmControl(App):
    theme = AppTheme()
    model: InteractiveTableModel | None = None
    footer: Footer
    current_response_action: str | None = None

    async def on_load(self, event: events.Load) -> None:
        await self.bind("q", "quit", "Quit")
        await self.bind("ctrl+i", "switch_focus", "Switch Focus", show=False)
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
        self.sidebar_content._tree.hide_root = True
        self.sidebar_content.border = "round"
        self.sidebar_content.can_focus = True

        await model_tree.root.add(AssociationListModel.title, AssociationListModel)
        await model_tree.root.add("Jobs", JobListModel)
        await model_tree.root.add("Nodes", NodeListModel)
        await model_tree.root.expand()

        await self.view.dock(self.header, edge="top")
        await self.view.dock(self.footer, edge="bottom")
        await self.view.dock(self.sidebar_content, edge="left", size=27)
        await self.view.dock(self.main_content_container)

        # initially set the focus to the sidebar so we can steal it after loading the model
        await self.set_focus(self.sidebar_content)
        await self.load_model(AssociationListModel(self, self.main_content))

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
        # unregister the bindings from the old model
        if self.model is not None:
            for key in self.model.keys.keys():
                del self.bindings.keys[key]

        self.model = model
        # register the new model bindings
        if model is not None:
            self.header.sub_title = model.title

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
            model = model_class(self, self.main_content)
            await self.load_model(model)

    async def prompt(self, message: str, response_action: str):
        await self.set_focus(self.footer)
        self.main_content.can_focus = False
        self.sidebar_content.can_focus = False
        self.current_response_action = response_action
        self.footer.prompt(message)

    async def confirm(self, message: str, response_action: str):
        await self.set_focus(self.footer)
        self.main_content.can_focus = False
        self.sidebar_content.can_focus = False
        self.current_response_action = response_action
        self.footer.confirm(message)

    async def handle_prompt_response(self, message: PromptResponse):
        if self.current_response_action is None:
            raise AssertionError("unexpected prompt response")
        self.main_content.can_focus = True
        self.sidebar_content.can_focus = True
        await self.set_focus(self.main_content)
        action_name, fixed_args = actions.parse(self.current_response_action)
        response_args = (message.response, message.confirmed)
        action_args = ", ".join([repr(arg) for arg in fixed_args + response_args])
        action_to_fire = f"{action_name}({action_args})"
        await self.action(action_to_fire)
        self.current_response_action = None

    async def display_error(self, error: Exception):
        await self.set_focus(self.footer)
        self.main_content.can_focus = False
        self.sidebar_content.can_focus = False
        self.footer.show_error(str(error))

    async def handle_error_dismissed(self, message: ErrorDismissed):
        self.main_content.can_focus = True
        self.sidebar_content.can_focus = True
        await self.set_focus(self.main_content)


def main():
    SlurmControl.run(title="Slurm Control")


if __name__ == "__main__":
    main()
