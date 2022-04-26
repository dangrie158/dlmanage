from __future__ import annotations
from contextlib import suppress
from copy import copy
from pathlib import Path
from typing import Type, Union
from rich.style import Style, NULL_STYLE

from textual import events
from textual.app import App
from textual.widgets import Header, TreeControl, TreeClick
from textual.widget import Widget
from textual import actions

from dlmanage.slurmbridge.cliobject import SlurmObjectException
from dlmanage.slurmbridge.objects import Account, Association, Job, User, Node
from dlmanage.slurmbridge.scontrol import SlurmControlError
from dlmanage.slurmbridge.sacctmgr import SlurmAccountManagerError


from dlmanage.widgets import Footer, TableTheme, LogView, JumpableScrollView

from dlmanage.models import AssociationListModel, JobListModel, NodeListModel
from dlmanage.widgets.footer import ErrorDismissed, PromptResponse
from dlmanage.widgets.interactive_table import InteractiveTableController, TablePosition


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
    controller: InteractiveTableController | None = None
    footer: Footer
    current_response_action: str | None = None

    async def on_load(self, event: events.Load) -> None:
        await self.bind("q", "quit", "Quit")
        await self.bind("ctrl+i", "switch_focus", "Switch Focus", show=False)

    async def on_mount(self, event: events.Mount) -> None:
        self.header = Header(clock=True, tall=True, style=self.theme.header)
        self.header.disable_messages(events.Click)
        self.footer = Footer(style=self.theme.header)

        self.main_content_container = JumpableScrollView(fluid=False)

        self.sidebar_content: TreeControl[
            Type[InteractiveTableController] | None
        ] = TreeControl("Models", None)
        self.sidebar_content._tree.hide_root = True
        self.sidebar_content.border = "round"
        self.sidebar_content.can_focus = True

        await self.sidebar_content.root.add(
            JobTableController.model_class.title, JobTableController
        )
        await self.sidebar_content.root.add(
            AssociationTableController.model_class.title, AssociationTableController
        )
        await self.sidebar_content.root.add(
            NodeTableController.model_class.title, NodeTableController
        )
        await self.sidebar_content.root.expand()

        await self.view.dock(self.header, edge="top")
        await self.view.dock(self.footer, edge="bottom")
        await self.view.dock(self.sidebar_content, edge="left", size=27)
        await self.view.dock(self.main_content_container)

        # initially set the focus to the sidebar so we can steal it after loading the model
        await self.set_focus(self.sidebar_content)
        await self.switch_controller(JobTableController(self))

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
            await self.main_content_container.window.widget.focus()
        else:
            await self.sidebar_content.focus()

    async def bind_controller(self, controller: InteractiveTableController):
        self._action_targets.add("controller")
        for binding in controller.keys.values():
            await self.bind(
                keys=binding.key,
                action=f"controller.{binding.action}",
                description=binding.description,
                key_display=binding.key_display,
            )
        self.footer.refresh()

    async def unbind_controller(self, controller: InteractiveTableController):
        for key in controller.keys.keys():
            del self.bindings.keys[key]
        self._action_targets.remove("controller")
        self.footer.refresh()

    async def switch_controller(self, controller: InteractiveTableController) -> None:
        # unregister the bindings from the old model
        if self.controller is not None:
            self.controller.view.is_loading = True
            await self.unbind_controller(self.controller)
            await self.controller.uninitialize()

        self.controller = controller
        # register the new model bindings
        if controller is not None:
            self.header.sub_title = self.controller.model_class.title
            await self.bind_controller(controller)
            await self.controller.initialize()
            await self.main_content_container.update(self.controller.view)
            await self.set_focus(self.main_content_container.window.widget)
            self.controller.view.is_loading = False
            self.footer.refresh()

    async def handle_tree_click(self, message: TreeClick) -> None:
        if not self.sidebar_content.can_focus:
            return
        controller_class = message.node.data
        if controller_class is not None and issubclass(
            controller_class, InteractiveTableController
        ):
            controller = controller_class(self)
            await self.switch_controller(controller)

    async def focus_footer(self):
        await self.set_focus(self.footer)
        if self.controller is not None:
            self.controller.view.can_focus = False
        self.sidebar_content.can_focus = False

    async def blur_footer(self):
        await self.set_focus(self.controller.view)
        self.controller.view.can_focus = True
        self.sidebar_content.can_focus = True

    async def prompt(self, message: str, response_action: str):
        self.current_response_action = response_action
        await self.focus_footer()
        self.footer.prompt(message)

    async def confirm(self, message: str, response_action: str):
        self.current_response_action = response_action
        await self.focus_footer()
        self.footer.confirm(message)

    async def handle_prompt_response(self, message: PromptResponse):
        if self.current_response_action is None:
            raise AssertionError("unexpected prompt response")

        await self.blur_footer()

        action_name, fixed_args = actions.parse(self.current_response_action)
        response_args = (message.response, message.confirmed)
        action_args = ", ".join([repr(arg) for arg in fixed_args + response_args])
        action_to_fire = f"{action_name}({action_args})"
        await self.action(action_to_fire)
        self.current_response_action = None

    async def display_error(self, error: Exception | str):
        await self.focus_footer()
        self.footer.show_error(str(error))

    async def handle_error_dismissed(self, message: ErrorDismissed):
        await self.blur_footer()


def main():
    SlurmControl.run(title="Slurm Control")


class AssociationTableController(
    InteractiveTableController[Union[User, Account], SlurmControl]
):
    model_class = AssociationListModel
    theme_class = AppTheme

    def __init__(self, app: SlurmControl):
        super().__init__(app)
        self.bind("ctrl+a", "add_account", "Add a new Account")
        self.bind("ctrl+u", "add_user", "Add a new User")
        self.bind("ctrl+d", "delete_entry", "Delete Entry")

    async def action_add_account(self):
        current_selection = self.view.selection_position or TablePosition("", 0)
        await self.app.prompt(
            "Enter the new accountname",
            f"controller.accountname_entered('{current_selection.column}', {current_selection.row})",
        )

    async def action_accountname_entered(
        self, column: str, row: int, accountname: str, confirmed: bool
    ):
        if confirmed:
            # try to find the next account up the hierarchy from the selected
            # row "upwards"
            parent: Association | None = self.model.get_data_object_for_row(row)
            while not isinstance(parent, Account):
                if parent is None:
                    break
                parent = parent.parent_object
            parent_name = (
                parent.account
                if parent is not None and parent.account is not None
                else "root"
            )
            try:
                new_account = await Account.create(account=accountname)
                with suppress(SlurmObjectException):
                    await new_account.set_parent(parent_name)
                await self.refresh()
                next_row = await self.model.get_next_row_matching(
                    0, accountname, ("account",)
                )
                if next_row is not None:
                    self.view.selection_position = TablePosition("", next_row)
            except (SlurmAccountManagerError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def action_add_user(self):
        current_selection = self.view.selection_position or TablePosition("", 0)
        await self.app.prompt(
            "Enter the new username",
            f"controller.username_entered('{current_selection.column}', {current_selection.row})",
        )

    async def action_username_entered(
        self, column: str, row: int, username: str, confirmed: bool
    ):
        if confirmed:
            try:
                # try to find the next account up the hierarchy from the selected
                # row "upwards"
                initial_account: Association | None = (
                    self.model.get_data_object_for_row(row)
                )
                while not isinstance(initial_account, Account):
                    if initial_account is None:
                        break
                    initial_account = initial_account.parent_object
                initial_account_name = (
                    initial_account.account
                    if initial_account is not None
                    and initial_account.account is not None
                    else "root"
                )
                await User.create(user=username, account=initial_account_name)
                await self.refresh()
                next_row = await self.model.get_next_row_matching(
                    0, username, ("user",)
                )
                if next_row:
                    self.view.selection_position = TablePosition(column, next_row)
            except (SlurmAccountManagerError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def action_delete_entry(self):
        current_selection = self.view.selection_position
        if current_selection is None:
            await self.app.display_error("Nothing selected")
            return
        object_to_delete = self.model.get_data_object_for_row(current_selection.row)
        await self.app.confirm(
            f"Do you really want to delete the {object_to_delete}",
            f"controller.delete_confirmed('{current_selection.column}', {current_selection.row})",
        )

    async def action_delete_confirmed(
        self, column: str, row: int, reponse: str, confirmed: bool
    ):
        if confirmed:
            object_to_delete = self.model.get_data_object_for_row(row)
            try:
                await object_to_delete.delete()
                await self.refresh()
                self.view.selection_position = TablePosition(
                    column, min(row, self.model.get_num_rows() - 1)
                )
            except (SlurmAccountManagerError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self.model.get_data_object_for_row(position.row)

        # if we update an account we can potentially update many associations
        allow_multiple_affected = (
            True if isinstance(affected_object, Account) else False
        )
        try:
            match position.column:
                case "account":
                    new_account = await Account.get(account=new_value)
                    next_row = None
                    if isinstance(affected_object, User):
                        await affected_object.set_account(new_account)
                        await self.model.load_data()
                        next_row = await self.model.get_next_row_matching(
                            0, affected_object.user, ("user",)
                        )
                    else:
                        await affected_object.set_parent(new_account.account)
                        await self.model.load_data()
                        next_row = await self.model.get_next_row_matching(
                            0, affected_object.account, ("account",)
                        )
                    if next_row is not None:
                        self.view.selection_position = TablePosition(
                            "account", next_row
                        )
                case "user":
                    assert isinstance(affected_object, User)
                    await affected_object.set_new_username(new_value)
                case "CPUs":
                    affected_object.max_cpus = new_value
                    await affected_object.save(allow_multiple_affected)
                case "GPUs":
                    affected_object.max_gpus = new_value
                    await affected_object.save(allow_multiple_affected)
                case "Timelimit":
                    if new_value is None:
                        new_value = "-1"
                    affected_object.grp_wall = new_value
                    await affected_object.save(allow_multiple_affected)
                case unknown_name:
                    raise AttributeError(f"Can't update column {unknown_name}")
            await self.refresh()
        except (SlurmAccountManagerError, SlurmObjectException) as error:
            await self.app.display_error(error)


class JobTableController(InteractiveTableController[Job, SlurmControl]):
    model_class = JobListModel
    theme_class = AppTheme

    def __init__(self, app: SlurmControl):
        super().__init__(app)

        self.bind("ctrl+d", "run_on_current_selection('cancel', True)", "Cancel Job")
        self.bind("ctrl+x", "run_on_current_selection('kill', True)", "Kill Job")
        self.bind("ctrl+p", "run_on_current_selection('hold', False)", "Put on Hold")
        self.bind(
            "ctrl+r", "run_on_current_selection('release', False)", "Release Hold"
        )

    async def action_run_on_current_selection(
        self, action_name: str, needs_confirmation: bool
    ):
        try:
            current_selection = self.view.selection_position
            if current_selection is None:
                raise IndexError
            selected_job = self.model.get_data_object_for_row(current_selection.row)
        except (AttributeError, IndexError):
            return await self.app.display_error("No Job selected")

        if needs_confirmation:
            return await self.app.confirm(
                f'Do you really want to {action_name} the Job "{selected_job.job_name}" ({selected_job.job_id_with_array})',
                f"controller.{action_name}_confirmed('{current_selection.column}', {current_selection.row})",
            )

        try:
            action = getattr(selected_job, action_name)
            await action()
        except (SlurmControlError, SlurmObjectException) as error:
            await self.app.display_error(error)
        await self.refresh()

    async def action_kill_confirmed(
        self, _: str, row: int, response: str, is_confirmed: bool
    ):
        if is_confirmed:
            await self.model.get_data_object_for_row(row).kill()

    async def action_cancel_confirmed(
        self, _: str, row: int, response: str, is_confirmed: bool
    ):
        if is_confirmed:
            await self.model.get_data_object_for_row(row).cancel()

    async def action_close_log_view(self):
        self.view.can_focus = True
        await self.app.main_content_container.update(self.view)
        await self.app.unbind_controller(self)
        self.keys = self.old_binding
        self.app.header.sub_title = self.model.title
        await self.app.bind_controller(self)

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self.model.get_data_object_for_row(position.row)
        try:
            match (position.column):
                case "CPUs":
                    await affected_object.set_cpus(new_value)
                case "GPUs":
                    await affected_object.set_gpus(new_value)
                case "Timelimit":
                    affected_object.time_limit = new_value
                    await affected_object.save()
                case unknown_name:
                    raise AttributeError(f"Don't know how to change {unknown_name}")
            await self.refresh()
        except (SlurmControlError, SlurmObjectException) as error:
            await self.app.display_error(error)

    async def on_cell_clicked(self, position: TablePosition) -> None:
        row_object = self.model.get_data_object_for_row(position.row)
        if row_object.std_out is None:
            return await self.app.display_error(FileNotFoundError("File not found"))
        try:
            log_view = LogView(Path(row_object.std_out))
        except Exception as error:
            return await self.app.display_error(str(error))

        self.view.can_focus = False
        await self.app.unbind_controller(self)
        self.old_binding = copy(self.keys)
        self.keys.clear()
        self.bind("escape", "close_log_view()", "Return to Joblist", key_display="esc")
        self.app.header.sub_title = (
            f"Logs for {row_object.job_name} (JobID: {row_object.job_id_with_array})"
        )
        await self.app.bind_controller(self)
        await self.app.main_content_container.update(log_view)


class NodeTableController(InteractiveTableController[Node, SlurmControl]):
    model_class = NodeListModel
    theme_class = AppTheme

    def __init__(self, app):
        super().__init__(app)
        self.bind("ctrl+r", "prompt_reboot_reason_selected_node(False)", "Reboot Node")
        self.bind("ctrl+x", "prompt_reboot_reason_selected_node(True)", "Force Reboot")

    async def prompt_for_reason(self, target_node: str, target_state: str):
        await self.app.prompt(
            f"Specify a reason for the new {target_state} state",
            f"controller.set_node_state('{target_node}', '{target_state}')",
        )

    async def action_prompt_reboot_reason_selected_node(self, force: bool):
        try:
            current_selection = self.view.selection_position
            if current_selection is None:
                raise IndexError
            selected_node = self.model.get_data_object_for_row(current_selection.row)
        except (AttributeError, IndexError):
            return await self.app.display_error("No Node selected")

        return await self.app.prompt(
            f'Specify the Reboot reason for "{selected_node.node_name}"',
            f"controller.reboot_node({current_selection.row}, {force})",
        )

    async def action_reboot_node(
        self, row: int, force: bool, reason: str, confirmed: bool
    ):
        if not confirmed:
            return
        try:
            node_to_reboot = self.model.get_data_object_for_row(row)
            await node_to_reboot.reboot(reason, force)
        except (SlurmControlError, SlurmObjectException) as error:
            await self.app.display_error(error)

    async def action_set_node_state(
        self, target_node: str, target_state: str, reason: str, confirmed: bool
    ):
        if confirmed:
            try:
                node_to_update = Node(node_name=target_node)
                await node_to_update.set_state(target_state, reason)
            except (SlurmControlError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self.model.get_data_object_for_row(position.row)
        try:
            match (position.column):
                case "State":
                    if new_value in ("DOWN", "DRAIN"):
                        return await self.prompt_for_reason(
                            affected_object.node_name, new_value
                        )
                    else:
                        await affected_object.set_state(new_value)
                case unknown_name:
                    raise AttributeError(f"Don't know how to change {unknown_name}")
            await self.refresh()

        except (SlurmControlError, SlurmObjectException) as error:
            await self.app.display_error(error)


if __name__ == "__main__":
    main()
