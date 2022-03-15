from typing import Any, Dict, Iterable, Mapping, Sequence, Tuple, Type
from io import StringIO

from rich.tree import Tree
from rich.console import Console
from textual.app import App

from dlmanage.widgets.interactive_table import (
    EditableChoiceTableCell,
    EditableIntTableCell,
    EditableTableCell,
    InteractiveTableModel,
    InteractiveTable,
    TableCell,
    TablePosition,
)
from dlmanage.slurmbridge import (
    Job,
    SlurmAccountManagerError,
    SlurmObjectException,
    User,
    Account,
    Association,
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


class AssociationListModel(InteractiveTableModel):
    title = "Users and Groups"

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
            try:
                await Account.create(account=accountname)
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
                await User.create(user=username, account="root")
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

    async def refresh(self):
        await self.load_data()
        await self.table_widget.refresh_data_from_model()
        self.table_widget.refresh()

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
                cell_class = (
                    EditableTableCell if isinstance(row_object, User) else TableCell
                )
                return cell_class, {}
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
                return TableCell, {"placeholder": "<home dir does not exist>"}
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
    def __init__(self, app: App, table_widget: InteractiveTable):
        super().__init__(app, table_widget)
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "user": {"ratio": 2, "no_wrap": True},
            "Job ID": {"ratio": 2, "no_wrap": True},
            "CPUs": {"justify": "center", "ratio": 1, "no_wrap": True},
            "GPUs": {"justify": "center", "ratio": 1, "no_wrap": True},
            "Memory": {"justify": "center", "ratio": 1, "no_wrap": True},
            "Timelimit": {"justify": "right", "ratio": 2, "no_wrap": True},
            "Runtime": {"justify": "right", "ratio": 2, "no_wrap": True},
        }

        self.bind("ctrl+x", "cancel_job", "Cancel Job")
        self.bind("ctrl+h", "hold_job", "Put on Hold")
        self.bind("ctrl+r", "unhold_job", "Remove Hold")

    async def load_data(self):

        self._data = Job.all()

    def get_columns(self) -> Iterable[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    async def get_cell(self, position: TablePosition) -> str | None:
        row_object = self._data[position.row]
        match (position.column):
            case "user":
                return row_object.user_id
            case "Job ID":
                cell_text = row_object.job_id
                if row_object.array_task_id is not None:
                    cell_text += f"[{row_object.array_task_id}]"
                return cell_text
            case "CPUs":
                return row_object.cpus
            case "GPUs":
                return row_object.gpus
            case "Memory":
                return row_object.mem
            case "Timelimit":
                return row_object.run_time
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
        match (position.column):
            case "user" | "Job ID" | "Runtime":
                return TableCell, {}
            case "CPUs" | "GPUs":
                return EditableIntTableCell, {"min_value": 0}
            case "Memory" | "Timelimit":
                return EditableTableCell, {}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        pass
