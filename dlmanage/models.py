from collections import defaultdict
from contextlib import suppress
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple, Type
from io import StringIO

from rich.tree import Tree
from rich.console import Console
from textual.app import App
from dlmanage.slurmbridge.scontrol import SlurmControlError

from dlmanage.widgets.interactive_table import (
    EditableChoiceTableCell,
    EditableIntTableCell,
    EditableTableCell,
    InteractiveTableModel,
    InteractiveTable,
    ProgressTableCell,
    TableCell,
    TablePosition,
)
from dlmanage.slurmbridge import (
    Job,
    SlurmAccountManagerError,
    User,
    Account,
    Association,
    Node,
    SlurmObjectException,
)


def get_association_with_lowest_grp_tres(
    object: Association, resource: str
) -> Association:
    lowest_value = getattr(object, resource)
    parent_with_lowest_value = object
    while object.parent_object is not None:
        parent_value = getattr(object.parent_object, resource)
        if lowest_value is None:
            lowest_value = parent_value
            if lowest_value is not None:
                parent_with_lowest_value = object.parent_object
        elif parent_value is not None:
            if int(parent_value) < int(lowest_value):
                lowest_value = parent_value
                parent_with_lowest_value = object.parent_object

        object = object.parent_object

    return parent_with_lowest_value


def get_bottleneck_hint(
    object: Association, resource: str
) -> Tuple[str | None, str | None]:
    hint, placeholder = None, "∞"
    own_value = getattr(object, resource)
    bottleneck = get_association_with_lowest_grp_tres(object, resource)
    bottleneck_value = getattr(bottleneck, resource) if bottleneck != object else None
    if bottleneck_value is not None and own_value is not None:
        if int(bottleneck_value) < int(own_value):
            hint = f"shadowed by {bottleneck.account}({bottleneck_value})"
        else:
            hint = (
                f"{own_value} (max {bottleneck_value} shared in <{bottleneck.account}>)"
            )
    elif bottleneck_value is not None and own_value is None:
        placeholder = f"{bottleneck_value} shared in <{bottleneck.account}>"

    return hint, placeholder


def build_account_tree(root_node: Association) -> Sequence[str]:
    def build_tree(tree: Tree, root: Association):
        node: Tree
        if isinstance(root, User):
            node = tree.add(root.user)
        else:
            node = tree.add(root.account)

        for child in root.children:
            build_tree(node, child)

    tree = Tree("All Associations", hide_root=True)
    build_tree(tree, root_node)
    tree_buffer = StringIO()
    console = Console(file=tree_buffer, width=500)
    console.print(tree)
    return tree_buffer.getvalue().splitlines()


def build_job_tree(jobs: Sequence[Job], attribute_name: str):
    jobs_by_attribute: defaultdict[str, List[Job]] = defaultdict(list)
    joblist_for_tree: List[Job | None] = []
    tree = Tree("All Jobs", hide_root=True)
    for job in jobs:
        jobs_by_attribute[getattr(job, attribute_name)].append(job)

    for node_name, joblist in jobs_by_attribute.items():
        node = tree.add(node_name)
        joblist_for_tree.append(None)
        for job in joblist:
            joblist_for_tree.append(job)
            node.add(job.job_id_with_array)

    tree_buffer = StringIO()
    console = Console(file=tree_buffer, width=500)
    console.print(tree)
    return tree_buffer.getvalue().splitlines(), joblist_for_tree


class AssociationListModel(InteractiveTableModel):
    title = "Users and Accounts"

    def __init__(self, app: App, table_widget: InteractiveTable):
        super().__init__(app, table_widget)
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "account": {"ratio": 3, "no_wrap": True},
            "user": {"ratio": 2, "no_wrap": True},
            "CPUs": {"justify": "center", "ratio": 2, "no_wrap": True},
            "GPUs": {"justify": "center", "ratio": 2, "no_wrap": True},
            "Timelimit": {"justify": "right", "ratio": 2, "no_wrap": True},
            "Home Directory": {"justify": "right", "ratio": 2, "no_wrap": True},
        }

        self.bind("ctrl+a", "add_account", "Add a new Account")
        self.bind("ctrl+u", "add_user", "Add a new User")
        self.bind("ctrl+d", "delete_entry", "Delete Entry")

    async def action_add_account(self):
        current_selection = self.table_widget.selection_position or TablePosition("", 0)
        await self.app.prompt(
            "Enter the new accountname",
            f"model.accountname_entered('{current_selection.column}', {current_selection.row})",
        )

    async def action_accountname_entered(
        self, column: str, row: int, accountname: str, confirmed: bool
    ):
        if confirmed:
            # try to find the next account up the hierarchy from the selected
            # row "upwards"
            parent = self._data[row]
            while not isinstance(parent, Account):
                parent = parent.parent_object
            parent_name = parent.account or "root"
            try:
                new_account = await Account.create(account=accountname)
                with suppress(SlurmObjectException):
                    await new_account.set_parent(parent_name)
                await self.refresh()
                next_row = await self.get_next_row_matching(
                    0, accountname, ("account",)
                )
                if next_row is not None:
                    self.table_widget.selection_position = TablePosition("", next_row)
            except (SlurmAccountManagerError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def action_add_user(self):
        current_selection = self.table_widget.selection_position or TablePosition("", 0)
        await self.app.prompt(
            "Enter the new username",
            f"model.username_entered('{current_selection.column}', {current_selection.row})",
        )

    async def action_username_entered(
        self, column: str, row: int, username: str, confirmed: bool
    ):
        if confirmed:
            try:
                # try to find the next account up the hierarchy from the selected
                # row "upwards"
                initial_account = self._data[row]
                while not isinstance(initial_account, Account):
                    initial_account = initial_account.parent_object
                initial_account_name = initial_account.account or "root"
                await User.create(user=username, account=initial_account_name)
                await self.refresh()
                next_row = await self.get_next_row_matching(0, username, ("user",))
                if next_row:
                    self.table_widget.selection_position = TablePosition(
                        column, next_row
                    )
            except (SlurmAccountManagerError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def action_delete_entry(self):
        current_selection = self.table_widget.selection_position
        if current_selection is None:
            await self.app.display_error("Nothing selected")
            return
        object_to_delete = self._data[current_selection.row]
        await self.app.confirm(
            f"Do you really want to delete the {object_to_delete}",
            f"model.delete_confirmed('{current_selection.column}', {current_selection.row})",
        )

    async def action_delete_confirmed(
        self, column: str, row: int, reponse: str, confirmed: bool
    ):
        if confirmed:
            object_to_delete = self._data[row]
            try:
                await object_to_delete.delete()
                await self.refresh()
                self.table_widget.selection_position = TablePosition(
                    column, min(row, len(self._data) - 1)
                )
            except (SlurmAccountManagerError, SlurmObjectException) as error:
                await self.app.display_error(error)

    async def load_data(self):
        self._data = await Association.all()
        self._available_accounts = await Account.all()
        self.account_tree = build_account_tree(self._data[0])

    def get_columns(self) -> Iterable[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    async def get_cell(self, position: TablePosition) -> str | None:
        column, row = position
        row_object = self._data[row]
        cell_text: str | None = None
        match column:
            case "account":
                cell_text = self.account_tree[row]
            case "user":
                cell_text = row_object.user if isinstance(row_object, User) else ""
            case "CPUs":
                cell_text = row_object.max_cpus
            case "GPUs":
                cell_text = row_object.max_gpus
            case "Timelimit":
                cell_text = row_object.grp_wall
            case "Home Directory":
                cell_text = (
                    row_object.home_directory if isinstance(row_object, User) else ""
                )
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

        return cell_text

    async def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        row_object = self._data[position.row]
        match position.column:
            case "account":
                selected_index: int
                choices = [account.account for account in self._available_accounts]
                current_value = row_object.account
                if isinstance(row_object, Account):
                    # don't display the account itself as a choice
                    choices = [
                        account for account in choices if account != row_object.account
                    ]
                    current_value = (
                        row_object.parent_object.account
                        if row_object.parent_object is not None
                        else ""
                    )
                try:
                    selected_index = choices.index(current_value)
                except ValueError:
                    selected_index = 0

                return EditableChoiceTableCell, {
                    "choices": choices,
                    "selected_index": selected_index,
                }
            case "user":
                # only user objects can set a new name
                is_user = isinstance(row_object, User)
                cell_class = EditableTableCell if is_user else TableCell
                return cell_class, {"can_focus": is_user}
            case "CPUs":
                hint, placeholder = get_bottleneck_hint(row_object, "max_cpus")
                return EditableIntTableCell, {
                    "min_value": 0,
                    "placeholder": placeholder,
                    "hint": hint,
                }
            case "GPUs":
                hint, placeholder = get_bottleneck_hint(row_object, "max_gpus")
                return EditableIntTableCell, {
                    "min_value": 0,
                    "placeholder": placeholder,
                    "hint": hint,
                }
            case "Timelimit":
                return EditableTableCell, {}
            case "Home Directory":
                return TableCell, {
                    "can_focus": False,
                    "placeholder": "<home dir does not exist>",
                }
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self._data[position.row]
        try:
            match position.column:
                case "account":
                    new_account = await Account.get(account=new_value)
                    next_row = None
                    if isinstance(affected_object, User):
                        await affected_object.set_account(new_account)
                        await self.load_data()
                        next_row = await self.get_next_row_matching(
                            0, affected_object.user, ("user",)
                        )
                    else:
                        await affected_object.set_parent(new_account.account)
                        await self.load_data()
                        next_row = await self.get_next_row_matching(
                            0, affected_object.account, ("account",)
                        )
                    if next_row is not None:
                        self.table_widget.selection_position = TablePosition(
                            "account", next_row
                        )
                case "user":
                    await affected_object.set_new_username(new_value)
                case "CPUs":
                    affected_object.max_cpus = new_value
                    await affected_object.save()
                case "GPUs":
                    affected_object.max_gpus = new_value
                    await affected_object.save()
                case "Timelimit":
                    if new_value is None:
                        new_value = "-1"
                    affected_object.grp_wall = new_value
                    await affected_object.save()
                case unknown_name:
                    raise AttributeError(f"Can't update column {unknown_name}")
            await self.refresh()
        except (SlurmAccountManagerError, SlurmObjectException) as error:
            await self.app.display_error(error)


class JobListModel(InteractiveTableModel):
    title = "Jobs"

    def __init__(self, app: App, table_widget: InteractiveTable):
        super().__init__(app, table_widget)
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "Job ID": {"ratio": 2, "no_wrap": True},
            "Job Name": {"justify": "center", "ratio": 3, "no_wrap": True},
            "CPUs": {"justify": "right", "ratio": 1, "no_wrap": True},
            "GPUs": {"justify": "right", "ratio": 1, "no_wrap": True},
            "Memory": {"justify": "right", "ratio": 1, "no_wrap": True},
            "Timelimit": {"justify": "right", "ratio": 2, "no_wrap": True},
            "Node": {"justify": "right", "ratio": 1, "no_wrap": True},
            "Runtime": {"justify": "right", "ratio": 2, "no_wrap": True},
        }

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
            current_selection = self.table_widget.selection_position
            selected_job = self._tree_list[current_selection.row]
        except (AttributeError, IndexError):
            return await self.app.display_error("No Job selected")

        if needs_confirmation:
            return await self.app.confirm(
                f'Do you really want to {action_name} the Job "{selected_job.job_name}" ({selected_job.job_id_with_array})',
                f"model.{action_name}_confirmed('{current_selection.column}', {current_selection.row})",
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
            await self._tree_list[row].kill()

    async def action_cancel_confirmed(
        self, _: str, row: int, response: str, is_confirmed: bool
    ):
        if is_confirmed:
            await self._tree_list[row].cancel()

    async def load_data(self):
        self._data = await Job.all()
        self._job_tree, self._tree_list = build_job_tree(self._data, "username")

    def get_columns(self) -> Iterable[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._job_tree)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    async def get_cell(self, position: TablePosition) -> str | None:
        tree_node = self._job_tree[position.row]
        row_object = self._tree_list[position.row]
        if row_object is None and position.column != "Job ID":
            return ""

        match (position.column):
            case "Job ID":
                return tree_node
            case "Job Name":
                return row_object.job_name
            case "CPUs":
                return row_object.cpus
            case "GPUs":
                return row_object.gpus
            case "Memory":
                return row_object.memory
            case "Timelimit":
                return row_object.time_limit
            case "Node":
                return row_object.node_list or ""
            case "Runtime":
                cell_text = row_object.run_time
                if row_object.job_state != "RUNNING":
                    cell_text = row_object.job_state
                    if row_object.reason is not None:
                        cell_text += f" ({row_object.reason})"
                return cell_text
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        row_object = self._tree_list[position.row]
        if row_object is None:
            return TableCell, {"can_focus": False}

        match (position.column):
            case "Job ID" | "Job Name" | "Runtime" | "Memory":
                return TableCell, {}
            case "CPUs" | "GPUs":
                return EditableIntTableCell, {"min_value": 0, "placeholder": "None"}
            case "Node":
                return TableCell, {"placeholder": "", "can_focus": False}
            case "Timelimit":
                return EditableTableCell, {}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self._tree_list[position.row]
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


class NodeListModel(InteractiveTableModel):
    title = "Nodes"

    def __init__(self, app: App, table_widget: InteractiveTable):
        super().__init__(app, table_widget)
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "Node Name": {"ratio": 1, "no_wrap": True},
            "State": {"ratio": 1, "no_wrap": True},
            "CPU Load": {"justify": "center", "ratio": 2, "no_wrap": True},
            "GPU Load": {"justify": "center", "ratio": 2, "no_wrap": True},
            "Uptime": {"justify": "right", "ratio": 1, "no_wrap": True},
        }
        self.bind("ctrl+r", "prompt_reboot_reason_selected_node(False)", "Reboot Node")
        self.bind("ctrl+x", "prompt_reboot_reason_selected_node(True)", "Force Reboot")

    async def prompt_for_reason(self, target_node: str, target_state: str):
        await self.app.prompt(
            f"Specify a reason for the new {target_state} state",
            f"model.set_node_state('{target_node}', '{target_state}')",
        )

    async def action_prompt_reboot_reason_selected_node(self, force: bool):
        try:
            current_selection = self.table_widget.selection_position
            selected_node = self._data[current_selection.row]
        except (AttributeError, IndexError):
            return await self.app.display_error("No Node selected")

        return await self.app.prompt(
            f'Specify the Reboot reason for "{selected_node.node_name}"',
            f"model.reboot_node({current_selection.row}, {force})",
        )

    async def action_reboot_node(
        self, row: int, force: bool, reason: str, confirmed: bool
    ):
        if not confirmed:
            return
        try:
            node_to_reboot = self._data[row]
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

    async def load_data(self):
        self._data = await Node.all()

    def get_columns(self) -> Iterable[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    async def get_cell(self, position: TablePosition) -> str | None:
        row_object = self._data[position.row]
        match (position.column):
            case "Node Name":
                return row_object.node_name
            case "State":
                cell_text = row_object.state
                if row_object.reason is not None:
                    cell_text += f" ({row_object.reason})"
                return cell_text
            case "CPU Load":
                allocated, total = row_object.cpu_allocation
                return f"{allocated} / {total}"
            case "GPU Load":
                allocated, total = row_object.gpu_allocation
                return f"{allocated} / {total}"
            case "Uptime":
                return str(row_object.uptime) if row_object.uptime else None
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        row_object = self._data[position.row]
        current_load: float = 0
        match (position.column):
            case "Node Name" | "Uptime":
                return TableCell, {"can_focus": False}
            case "CPU Load":
                try:
                    allocated, total = row_object.cpu_allocation
                    current_load = (int(allocated) / int(total)) * 100
                except ValueError:
                    pass
                return ProgressTableCell, {
                    "can_focus": False,
                    "fill_percent": current_load,
                }
            case "GPU Load":
                try:
                    allocated, total = row_object.gpu_allocation
                    current_load = (int(allocated) / int(total)) * 100
                except ValueError:
                    pass
                return ProgressTableCell, {
                    "can_focus": False,
                    "fill_percent": current_load,
                }
            case "State":
                return EditableChoiceTableCell, {"choices": Node.STATES}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self._data[position.row]
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
