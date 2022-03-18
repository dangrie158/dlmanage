from __future__ import annotations
import asyncio
from copy import copy

import dataclasses
from datetime import datetime, timedelta
from functools import cached_property
from pathlib import Path
from typing import (
    Any,
    ClassVar,
    Dict,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    cast,
)
from dlmanage.slurmbridge.common import (
    get_gres_value,
    update_gres_value,
    find_home_directory,
)
from dlmanage.slurmbridge.cliobject import field
from dlmanage.slurmbridge.sacctmgr import (
    SlurmAccountManagerError,
    SlurmAccountManagerObject,
    WritableSlurmAccountManagerObject,
)
from dlmanage.slurmbridge.scontrol import SlurmControlError, SlurmControlObject
from dlmanage.slurmbridge.scancel import SlurmCancelObject

AssociationType = TypeVar("AssociationType", bound="Association")


class Association(SlurmAccountManagerObject[AssociationType]):
    query_options = ("tree",)

    _: dataclasses.KW_ONLY
    id: str = field(read_only=True)
    parent_id: str = field(repr=False, read_only=True)
    parent_name: str = field(repr=False, read_only=True)
    parent_object: Association | None = field(synthetic=True)
    nesting_level: int = field(repr=False, synthetic=True)
    cluster: str = field(repr=False, read_only=True)
    account: str = field(read_only=True)
    user: str | None = field(read_only=True)
    partition: str = field(repr=False)
    children: List[Association] = field(default_factory=lambda: list(), synthetic=True)
    grp_tres_mins: str | None = dataclasses.field(repr=False, default=None)
    grp_tres_run_mins: str | None = dataclasses.field(repr=False, default=None)
    grp_tres: str | None = dataclasses.field(repr=False, default=None)
    grp_jobs: str | None = dataclasses.field(repr=False, default=None)
    grp_submit_jobs: str | None = dataclasses.field(repr=False, default=None)
    grp_wall: str | None = dataclasses.field(repr=False, default=None)
    max_tres_mins_per_job: str | None = dataclasses.field(repr=False, default=None)
    max_tres_per_job: str | None = dataclasses.field(repr=False, default=None)
    max_tres_per_node: str | None = dataclasses.field(repr=False, default=None)
    max_wall_duration_per_job: str | None = dataclasses.field(repr=False, default=None)
    fairshare: Optional[str] = dataclasses.field(repr=False, default=None)
    max_jobs: Optional[str] = dataclasses.field(repr=False, default=None)
    max_submit_jobs: Optional[str] = dataclasses.field(repr=False, default=None)
    qos: Optional[str] = dataclasses.field(repr=False, default=None)

    @classmethod
    def _response_to_instances(
        cls, response: Sequence[Dict[str, str]]
    ) -> Sequence[AssociationType]:
        instances: list[AssociationType] = []
        # thanks to the "tree" query option we get the results in order and the
        # account names are prefixed with spaces to represent the nesting level
        # so we can rebuild the hierarchy tree keeping track of the information
        hierarchy_stack: Dict[int, Association] = {}
        for data in response:
            attributes = cls._response_to_attributes(data)
            account_response = attributes.pop("account") or ""
            nesting_level = account_response.count(" ")

            # find the parent of the instance
            parent: Association | None = None
            if nesting_level > 0:
                parent = hierarchy_stack[nesting_level - 1]

            # create the instance based on whether a username is given or not
            instance_type = Account if attributes["user"] is None else User
            account = account_response.strip()
            instance = instance_type(  # type:ignore
                parent_object=parent,
                account=account,
                nesting_level=nesting_level,
                **attributes,
            )
            instance = cast(AssociationType, instance)

            # build the tree
            if parent is not None:
                parent.children.append(instance)
            hierarchy_stack[nesting_level] = instance
            instances.append(instance)
        return instances

    @property
    def max_gpus(self):
        return get_gres_value(self.grp_tres, "gres/gpu")

    @max_gpus.setter
    def max_gpus(self, new_val):
        self.grp_tres = update_gres_value(self.grp_tres, "gres/gpu", new_val)

    @property
    def max_cpus(self):
        return get_gres_value(self.grp_tres, "cpu")

    @max_cpus.setter
    def max_cpus(self, new_val):
        self.grp_tres = update_gres_value(self.grp_tres, "cpu", new_val)


class Account(Association["Account"], WritableSlurmAccountManagerObject["Account"]):
    query_options = ("withassoc",)
    account: str = field(primary_key=True)
    parent: str = field(write_only=True, default=None)

    async def set_parent(self, new_paernt: str):
        filters = {"Account": self.account}
        updates = {"parent": new_paernt}
        await self._scattmgr_write("modify", updates, filters)
        self.parent = new_paernt
        await self.refresh_from_db()

    def __str__(self) -> str:
        return f"Account {self.account}"


class User(Association["User"], WritableSlurmAccountManagerObject["User"]):
    query_options = ("withassoc",)

    user: str = field(primary_key=True)
    account: str = field(primary_key=True)
    default_account: str | None = field(default=None, write_only=True)

    async def set_account(self, new_account: Account):
        old_account = self

        # create a new user to generate the association with User + Account
        new_user = copy(self)
        del new_user.default_account
        new_user.default_account = new_account.account
        updates, filters = new_user._to_query()
        await self._scattmgr_write("create", updates | filters, {})

        # delete the old association (this is done by removing the user with an additional account filter)
        if old_account is not None:
            _, filters = self._to_query()
            filters["account"] = old_account.account
            await self._scattmgr_write("delete", {}, filters)

        await self.refresh_from_db()

    async def set_new_username(self, new_name: str):
        if not new_name:
            raise SlurmAccountManagerError("can't set an empty name for a user")
        filters = {"User": self.user}
        updates = {"NewName": new_name}
        await self._scattmgr_write("modify", updates, filters)
        del self.user
        self.user = new_name
        await self.refresh_from_db()

    @cached_property
    def home_directory(self) -> str | None:
        return find_home_directory(self.user)

    def __str__(self) -> str:
        return f"User {self.user} in {self.account}"


class QOS(SlurmAccountManagerObject["QOS"]):
    user: str = field(primary_key=True)
    default_account: str
    grp_tres_mins: str
    grp_tres_run_mins: str
    grp_tres: str
    grp_jobs: str
    grp_submit_jobs: str
    grp_wall: str
    max_tres_mins_per_job: str
    max_tres_per_job: str
    max_tres_per_node: str
    max_wall_duration_per_job: str
    max_prio_threshold: str
    # QOS specific
    max_jobs_accure_per_account: str
    max_jobs_accure_per_user: str
    max_jobs_per_account: str
    max_jobs_per_user: str
    max_submit_jobs_per_account: str
    max_submit_jobs_per_user: str
    max_tres_per_account: str
    max_tres_per_user: str


@dataclasses.dataclass
class Job(SlurmControlObject["Job"], SlurmCancelObject):
    job_id: str = field(primary_key=True)
    _: dataclasses.KW_ONLY
    job_name: str = field(repr=False, read_only=True)
    job_state: str = field(repr=False, read_only=True)
    run_time: str = field(repr=False, read_only=True)
    time_limit: str = field(repr=False)
    node_list: str = field(repr=False, read_only=True)
    tres: str = field(repr=False, read_only=True)
    user_id: str = field(repr=False, read_only=True)
    group_id: str = field(repr=False, read_only=True)
    account: str = field(repr=False, read_only=True)
    gres: Optional[str] = field(repr=False, write_only=True, default=None)
    num_cpus: Optional[str] = field(repr=False, write_only=True, default=None)
    std_out: Optional[str] = field(repr=False, write_only=True, default=None)
    std_err: Optional[str] = field(repr=False, write_only=True, default=None)
    array_task_id: Optional[str] = field(repr=False, read_only=True, default=None)
    reason: Optional[str] = field(repr=False, read_only=True, default=None)

    @cached_property
    def username(self) -> str:
        # the username returned by scontrol has the user_id in parentesis (e.g. "griesshaber(1234)")
        user_name, *_ = self.user_id.split("(")
        return user_name

    @cached_property
    def job_id_with_array(self):
        job_id = self.job_id
        if self.array_task_id is not None:
            job_id += f"[{self.array_task_id}]"
        return job_id

    @property
    def cpus(self) -> str | None:
        return get_gres_value(self.tres, "cpu")

    async def set_cpus(self, new_value: str):
        self.num_cpus = f"{new_value}"
        return await self.save()

    @property
    def memory(self) -> str | None:
        return get_gres_value(self.tres, "mem")

    @property
    def gpus(self) -> str | None:
        return get_gres_value(self.tres, "gres/gpu")

    async def set_gpus(self, new_value: str):
        self.gres = f"gpu:{new_value}"
        return await self.save()

    async def get_user(self) -> User:
        user_name = self.username
        return await User.get(user=user_name, account=self.account)

    async def get_account(self) -> Account:
        return await Account.get(account=self.account)

    async def hold(self):
        if self.job_state == "RUNNING":
            raise SlurmControlError(
                self.__class__, f"Job {self.job_id} is already running"
            )
        return await self._scontrol_execute("uhold", self.job_id)

    async def release(self):
        if self.job_state == "RUNNING":
            raise SlurmControlError(
                self.__class__, f"Job {self.job_id} is already running"
            )
        return await self._scontrol_execute("release", self.job_id)

    async def cancel(self):
        signal = "SIGTERM" if self.job_state == "RUNNING" else None
        return await self._scancel_execute(self.job_id, signal)

    async def kill(self):
        signal = "SIGKILL" if self.job_state == "RUNNING" else None
        return await self._scancel_execute(self.job_id, signal)


@dataclasses.dataclass
class Node(SlurmControlObject["Node"]):
    query_options = ("--future",)
    query_type = Literal["ValueOnly"]
    STATES: ClassVar[List[str]] = ["CANCEL_REBOOT", "DOWN", "DRAIN", "FUTURE", "RESUME"]

    node_name: str = field(primary_key=True)
    _: dataclasses.KW_ONLY
    cpualloc: str | None = field(repr=False, read_only=True, default=None)
    cputot: str | None = field(repr=False, read_only=True, default=None)
    cpuload: str | None = field(repr=False, read_only=True, default=None)
    gres: str | None = field(repr=False, read_only=True, default=None)
    gres_used: str | None = field(repr=False, read_only=True, default=None)
    boot_time: str | None = field(repr=False, read_only=True, default=None)
    state: str | None = field(repr=False, read_only=True, default=None)
    reason: str | None = field(repr=False, read_only=True, default=None)

    @cached_property
    def uptime(self) -> timedelta | None:
        if self.boot_time is None:
            return None

        boot_time: datetime
        try:
            boot_time = datetime.fromisoformat(self.boot_time)
        except ValueError:
            return None

        uptime = datetime.now() - boot_time
        # remove the microsecond resolution
        uptime = timedelta(uptime.days, uptime.seconds)
        return uptime

    @cached_property
    def cpu_allocation(self) -> Tuple[str, str]:
        return self.cpualloc or "n", self.cputot or "a"

    @cached_property
    def gpu_allocation(self) -> Tuple[str, str]:
        """
        gres:'gpu:turing:4'
        gres_used:'gpu:turing:0(IDX:N/A)'
        """
        if self.gres is None or self.gres_used is None:
            return ("n", "a")

        # example gres string: "gpu:turing:4,gpu:ampere:4"
        gres_entries = self.gres.split(",")
        gres_used_entries = self.gres_used.split(",")

        avaliable_gpus = sum(int(entry.split(":")[-1]) for entry in gres_entries)
        # example gres_used string: "gpu:turing:0(IDX:N/A)"
        used_gpus = sum(
            int(entry.split("(")[0].split(":")[-1]) for entry in gres_used_entries
        )
        return str(used_gpus), str(avaliable_gpus)

    @cached_property
    def allocated_gpus(self) -> Sequence[str]:
        # example gres_used string: "gpu:turing:0(IDX:N/A)"
        return []

    async def reboot(self, reason: str, force=False):
        args = []
        if force:
            args.append("ASAP")
        args.append("nextstate=RESUME")
        args.append(f"reason=´{reason}")
        return await self._scontrol_execute("reboot", self.node_name, args)

    async def set_state(self, new_state: str, reason: Optional[str] = None):
        _, filters = self._to_query()
        new_values = {"State": new_state}
        if reason is not None:
            new_values["Reason"] = reason
        await self._scontrol_update(new_values, filters)
        await self.refresh_from_db()
