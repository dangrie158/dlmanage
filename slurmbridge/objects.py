from __future__ import annotations
from copy import copy

import dataclasses
from functools import cached_property
from typing import Dict, List, Optional, Sequence, TypeVar, cast
from slurmbridge.sacctmgr import (
    SlurmAccountManagerError,
    field,
    SlurmObject,
    WritableSlurmObject,
)
from slurmbridge.common import get_gres_value, update_gres_value, find_home_directory

AssociationType = TypeVar("AssociationType", bound="Association")


class Association(SlurmObject[AssociationType]):
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


class Account(Association["Account"], WritableSlurmObject["Account"]):
    query_options = ("withassoc",)
    account: str = field(primary_key=True)
    parent: str = field(write_only=True, default=None)

    async def set_parent(self, new_paernt):
        filters = {"Account": self.account}
        updates = {"parent": new_paernt}
        await self._scattmgr_write("modify", updates, filters)
        self.parent = new_paernt
        await self.refresh_from_db()

    def __str__(self) -> str:
        return f"Account {self.account}"


class User(Association["User"], WritableSlurmObject["User"]):
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


class QOS(SlurmObject["QOS"]):
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
class Job:
    job_id: str
    job_name: str
    job_state: str
    run_time: str
    time_limit: str
    tres: str
    user_id: str
    group_id: str
    array_task_id: Optional[str] = None
    reason: Optional[str] = None

    async def get_user(self) -> User:
        return await User.get(user=self.user_id, account=self.group_id)

    async def get_account(self) -> Account:
        return await Account.get(account=self.group_id)

    @property
    def cpus(self) -> str | None:
        return get_gres_value(self.tres, "cpu")

    @property
    def mem(self) -> str | None:
        return get_gres_value(self.tres, "mem")

    @property
    def gpus(self) -> str | None:
        return get_gres_value(self.tres, "gres/gpu")

    @classmethod
    def all(cls) -> Sequence[Job]:
        return [
            Job(
                "37780",
                "fixed-mixed-loss-05-metatrain",
                "RUNNING",
                "0-23:20:15",
                "4-00:00:00",
                "cpu=30,mem=128G,node=1,billing=4",
                "griesshaber",
                "employee",
            ),
            Job(
                "37780",
                "fixed-mixed-loss-05-finetune-16",
                "PENDING",
                "0-00:00:00",
                "4-00:00:00",
                "cpu=4,mem=16G,node=1,billing=4,gres/gpu=1",
                "griesshaber",
                "employee",
                "0-169",
                "Dependency",
            ),
            Job(
                "37780",
                "fixed-mixed-loss-05-finetune-8",
                "PENDING",
                "0-00:00:00",
                "4-00:00:00",
                "cpu=4,mem=16G,node=1,billing=4,gres/gpu=1",
                "griesshaber",
                "employee",
                "0-169",
                "Dependency",
            ),
            Job(
                "37780",
                "fixed-mixed-loss-05-finetune-4",
                "PENDING",
                "0-00:00:00",
                "4-00:00:00",
                "cpu=4,mem=16G,node=1,billing=4,gres/gpu=1",
                "griesshaber",
                "employee",
                "0-169",
                "Dependency",
            ),
        ]
