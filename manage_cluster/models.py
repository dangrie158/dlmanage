from typing import Any, Dict, Mapping, Sequence, Tuple, Type
from widgets.interactive_table import (
    EditableChoiceTableCell,
    EditableIntTableCell,
    EditableTableCell,
    InteractiveTableModel,
    TableCell,
    TablePosition,
)
from slurmbridge import User, Account, AssociationBaseObject


async def get_effective_gres(
    object: AssociationBaseObject, resource: str
) -> Tuple[AssociationBaseObject, str | None]:
    value = None
    while object is not None and value is None:
        value = await getattr(object, resource)
        object = await object.parent

    return object, value


class UserListModel(InteractiveTableModel):
    title = "All Users"

    def __init__(self):
        self._columns = {
            "user": {"ratio": 2, "no_wrap": True},
            "account": {"ratio": 2, "no_wrap": True},
            "CPUs": {"justify": "center", "ratio": 2, "no_wrap": True},
            "GPUs": {"justify": "center", "ratio": 2, "no_wrap": True},
            "Timelimit": {"justify": "right", "ratio": 3, "no_wrap": True},
            "Home Directory": {"justify": "right", "ratio": 2, "no_wrap": True},
        }

    async def load_data(self):
        self._data = await User.all()
        self._available_accounts = await Account.all()

    def get_columns(self) -> Sequence[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    async def get_cell(self, position: TablePosition) -> str | None:
        column, row = position
        row_data = self._data[row]
        cell_text: str | None = None
        match column:
            case "user":
                cell_text = row_data.user
            case "account":
                cell_text = row_data.default_account
            case "CPUs":
                cell_text = await row_data.max_cpus
            case "GPUs":
                cell_text = await row_data.max_gpus
            case "Timelimit":
                cell_text = row_data.grp_wall
            case "Home Directory":
                cell_text = row_data.home_directory
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

        return cell_text

    async def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        cell_placeholder = "<undefined>"
        row_data = self._data[position.row]
        match position.column:
            case "user":
                return TableCell, {}
            case "account":
                return EditableChoiceTableCell, {
                    "choices": [account.account for account in self._available_accounts]
                }
            case "CPUs":
                if await row_data.max_cpus is None:
                    object, limit = await get_effective_gres(row_data, "max_cpus")
                    if limit is not None:
                        cell_placeholder = f"{limit} <shared with all in {object!r}>"
                return EditableIntTableCell, {
                    "min_val": 0,
                    "placeholder": cell_placeholder,
                }
            case "GPUs":
                if await row_data.max_gpus is None:
                    object, limit = await get_effective_gres(row_data, "max_gpus")
                    if limit is not None:
                        cell_placeholder = f"{limit} <shared with all in {object!r}>"
                return EditableIntTableCell, {
                    "min_val": 0,
                    "placeholder": cell_placeholder,
                }
            case "Timelimit":
                return EditableTableCell, {}
            case "Home Directory":
                return TableCell, {"placeholder": "<home dir does not exist>"}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        affected_object = self._data[position.row]
        match position.column:
            case "account":
                new_account = await Account.get(account=new_value)
                await affected_object.set_account(new_account)
            case "CPUs":
                affected_object.set_max_cpus(new_value)
                await affected_object.save()
            case "GPUs":
                affected_object.set_max_gpus(new_value)
                await affected_object.save()
            case "Timelimit":
                affected_object.grp_wall = new_value
                await affected_object.save()
            case unknown_name:
                raise AttributeError(f"Can't update column {unknown_name}")

    async def on_row_delete(self, position: TablePosition) -> None:
        pass

    async def on_row_add(self, position: TablePosition) -> None:
        pass


class AccountListModel(InteractiveTableModel):
    title = "All Accounts"

    def __init__(self):
        self._columns = {
            "account": {"ratio": 1, "no_wrap": True},
            "CPUs for all Members": {"justify": "center", "ratio": 1, "no_wrap": True},
            "GPUs for all Members": {"justify": "center", "ratio": 1, "no_wrap": True},
            "Timelimit for all Members": {
                "justify": "right",
                "ratio": 1,
                "no_wrap": True,
            },
        }

    async def load_data(self):
        self._data = await Account.all()

    def get_columns(self) -> Sequence[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    async def get_cell(self, position: TablePosition) -> str | None:
        column, row = position
        row_data = self._data[row]
        cell_text: str | None = None
        match column:
            case "account":
                cell_text = row_data.account
            case "CPUs for all Members":
                cell_text = await row_data.max_cpus
            case "GPUs for all Members":
                cell_text = await row_data.max_gpus
            case "Timelimit for all Members":
                cell_text = row_data.grp_wall
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

        return cell_text

    async def get_cell_class(
        self, position: TablePosition
    ) -> Tuple[Type[TableCell], Dict[str, Any]]:
        cell_placeholder = "<undefined>"
        row_data = self._data[position.row]
        match position.column:
            case "account":
                return TableCell, {}
            case "CPUs for all Members":
                if await row_data.max_cpus is None:
                    object, limit = await get_effective_gres(row_data, "max_cpus")
                    if limit is not None:
                        cell_placeholder = f"{limit} <inherited from {object!r}>"
                return EditableIntTableCell, {
                    "min_val": 0,
                    "placeholder": cell_placeholder,
                }
            case "GPUs for all Members":
                if await row_data.max_gpus is None:
                    object, limit = await get_effective_gres(row_data, "max_gpus")
                    if limit is not None:
                        cell_placeholder = f"{limit} <inherited from {object!r}>"
                return EditableIntTableCell, {
                    "min_val": 0,
                    "placeholder": cell_placeholder,
                }
            case "Timelimit for all Members":
                return EditableTableCell, {}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")

    async def on_cell_update(self, position: TablePosition, new_value: str) -> None:
        pass

    async def on_row_delete(self, position: TablePosition) -> None:
        pass

    async def on_row_add(self, position: TablePosition) -> None:
        pass


class JobListModel(UserListModel):
    pass