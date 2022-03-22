from collections import defaultdict
from contextlib import suppress
from os import path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Tuple, Type
from io import StringIO

from rich.tree import Tree
from rich.console import Console
from textual.app import App
from dlmanage.slurmbridge.scontrol import SlurmControlError

from dlmanage.widgets.interactive_table import (
    ClickableTableCell,
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


class AssociationListModel(InteractiveTableModel[User | Account]):
    title = "Users and Accounts"

    def __init__(self):
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "account": {"ratio": 3, "no_wrap": True},
            "user": {"ratio": 2, "no_wrap": True},
            "CPUs": {"justify": "center", "ratio": 2, "no_wrap": True},
            "GPUs": {"justify": "center", "ratio": 2, "no_wrap": True},
            "Timelimit": {"justify": "right", "ratio": 2, "no_wrap": True},
            "Home Directory": {"justify": "right", "ratio": 2, "no_wrap": True},
        }

    async def load_data(self):
        self._data = await Association.all()
        self._available_accounts = await Account.all()
        self.account_tree = build_account_tree(self._data[0])

    def get_columns(self):
        return self._columns.keys()

    def get_num_rows(self):
        return len(self._data)

    def get_column_kwargs(self, column_name: str):
        return self._columns.get(column_name, {})

    def get_data_object_for_row(self, row: int):
        return self._data[row]

    async def get_cell(self, position: TablePosition):
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
                    "placeholder": "<not found>",
                }
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")


class JobListModel(InteractiveTableModel[Job]):
    title = "Jobs"

    def __init__(self):
        super().__init__()
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "Job ID": {"ratio": 2, "no_wrap": True},
            "Job Name": {"justify": "center", "ratio": 3, "no_wrap": True},
            "CPUs": {"justify": "right", "ratio": 1, "no_wrap": True},
            "GPUs": {"justify": "right", "ratio": 1, "no_wrap": True},
            "Memory": {"justify": "right", "ratio": 1, "no_wrap": True},
            "Timelimit": {"justify": "right", "ratio": 2, "no_wrap": True},
            "Output": {"justify": "center", "ratio": 1, "no_wrap": True},
            "Node": {"justify": "right", "ratio": 1, "no_wrap": True},
            "Runtime": {"justify": "right", "ratio": 2, "no_wrap": True},
        }

    async def load_data(self):
        unordered_jobs = await Job.all()

        def sort_function(job: Job):
            # None states are always at the top, then go all running jobs,
            # followed by a list of completed and pending jobs
            match job.job_state:
                case None:
                    return str(job.username) + "0" + str(job.job_id_with_array)
                case "RUNNING":
                    return str(job.username) + "1" + str(job.job_id_with_array)
                case other:
                    return str(job.username) + str(other) + str(job.job_id_with_array)

        sorted_jobs = sorted(unordered_jobs, key=sort_function)
        self._job_tree, self._tree_list = build_job_tree(sorted_jobs, "username")

    def get_columns(self) -> Iterable[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._job_tree)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    def get_data_object_for_row(self, row: int):
        return self._tree_list[row]

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
            case "Output":
                can_be_viewed = row_object.std_out is not None and path.exists(
                    row_object.std_out
                )
                return "view" if can_be_viewed else None
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
            case "Output":
                can_be_viewed = row_object.std_out is not None and path.exists(
                    row_object.std_out
                )
                cell_class = ClickableTableCell if can_be_viewed else TableCell
                return cell_class, {"placeholder": "n/a"}
            case unknown_name:
                raise AttributeError(f"Unknown column: {unknown_name}")


class NodeListModel(InteractiveTableModel[Node]):
    title = "Nodes"

    def __init__(self):
        self._columns: Mapping[str, Mapping[str, Any]] = {
            "Node Name": {"ratio": 1, "no_wrap": True},
            "State": {"ratio": 1, "no_wrap": True},
            "CPU Load": {"justify": "center", "ratio": 2, "no_wrap": True},
            "GPU Load": {"justify": "center", "ratio": 2, "no_wrap": True},
            "Uptime": {"justify": "right", "ratio": 1, "no_wrap": True},
        }

    async def load_data(self):
        self._data = await Node.all()

    def get_columns(self) -> Iterable[str]:
        return self._columns.keys()

    def get_num_rows(self) -> int:
        return len(self._data)

    def get_column_kwargs(self, column_name: str) -> Mapping[str, Any]:
        return self._columns.get(column_name, {})

    def get_data_object_for_row(self, row: int):
        return self._data[row]

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
