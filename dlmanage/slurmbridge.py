from __future__ import annotations

import dataclasses
from functools import cached_property
from abc import ABC
from glob import glob
from importlib.metadata import metadata
import re
import asyncio
import shutil
from copy import copy
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

SACCTMGR_PATH = shutil.which("sacctmgr")


def camel_to_snake_case(input: str) -> str:
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", input).lower()


def snake_to_camel_case(input: str) -> str:
    return "".join([part.title() for part in input.split("_")])


def get_gres_value(haystack: str, needle: str) -> Optional[str]:
    if haystack is None:
        return None

    parts = haystack.split(",")
    for kv_pair in parts:
        if "=" not in kv_pair:
            continue

        key, value = kv_pair.split("=")
        if key == needle:
            return value

    return None


def update_gres_value(haystack: str, needle: str, new_value: str) -> str:
    if haystack is None:
        haystack = ""

    if new_value == None:
        new_value = "-1"

    new_haystack_parts: list[str] = []
    parts = haystack.split(",")
    for kv_pair in parts:
        if "=" not in kv_pair:
            continue

        key, current_value = kv_pair.split("=")
        value = new_value if key == needle else current_value
        new_haystack_parts.append(f"{key}={value}")

    # if we could not update an old value, add a new entry
    if needle not in haystack:
        new_haystack_parts.append(f"{needle}={new_value}")

    return ",".join(new_haystack_parts)


def find_home_directory(username: str) -> str | None:
    if len(username) == 0:
        return None

    patterns = (
        f"/home/stud/{username[0]}/{username}",
        f"/home/ma/{username[0]}/{username}",
        f"/home/*/{username}/",
    )
    for candidate_pattern in patterns:
        candidates = glob(candidate_pattern)
        if len(candidates) > 0:
            return candidates[0]
    return None


class SlurmAccountManagerError(Exception):
    def __str__(self) -> str:
        return f"{self.args[0]}"


class SlurmObjectException(Exception):
    def __init__(self, Object_class):
        self.Object_class = Object_class

    def __str__(self) -> str:
        return f"{self.Object_class.__name__} - {self.__class__.__name__}"


class NotFound(SlurmObjectException):
    pass


class MultipleObjectReturned(SlurmObjectException):
    pass


ObjectType = TypeVar("ObjectType", bound="SlurmObject")
WritableObjectType = TypeVar("WritableObjectType", bound="WritableSlurmObject")

READONLY = {"readonly": True}
WRITEONLY = {"writeonly": True}
SYNTHETIC = {"synthetic": True}
PRIMARYKEY = {"primarykey": True}


def field(
    *,
    default=dataclasses.MISSING,
    default_factory=dataclasses.MISSING,
    init=True,
    repr=True,
    hash=None,
    compare=True,
    metadata=None,
    kw_only=dataclasses.MISSING,
    read_only=False,
    synthetic=False,
    write_only=False,
    primary_key=False,
):
    if metadata is None:
        metadata = {}
    if read_only:
        metadata |= READONLY
    if synthetic:
        metadata |= SYNTHETIC | READONLY
    if primary_key:
        metadata |= PRIMARYKEY | READONLY
    if write_only:
        metadata |= WRITEONLY

    return dataclasses.field(
        default=default,
        default_factory=default_factory,
        init=init,
        repr=repr,
        hash=hash,
        compare=compare,
        metadata=metadata,
        kw_only=kw_only,
    )


class SlurmObject(ABC, Generic[ObjectType]):
    query_options: ClassVar[Sequence[str]] = tuple()
    _primary_key_fields: List[str]
    _read_only_fields: List[str]
    _write_only_fields: List[str]
    _synthetic_fields: List[str]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        cls = dataclasses.dataclass(cls)
        cls._primary_key_fields = cls._collect_all_fields_of_type(PRIMARYKEY)
        cls._read_only_fields = cls._collect_all_fields_of_type(READONLY)
        cls._write_only_fields = cls._collect_all_fields_of_type(WRITEONLY)
        cls._synthetic_fields = cls._collect_all_fields_of_type(SYNTHETIC)

    @classmethod
    def _collect_all_fields_of_type(cls, field_type: Dict[str, bool]):
        type_key = list(field_type.keys())[0]
        return [
            field.name
            for field in dataclasses.fields(cls)
            if field.metadata is not None and field.metadata.get(type_key, False)
        ]

    def __setattr__(self, __name: str, __value: Any) -> None:
        # make writing to read-only fields a runtime error
        if hasattr(self, __name) and __name in self._read_only_fields:
            raise AttributeError(f"{__name} is a read-only field")
        return super().__setattr__(__name, __value)

    @classmethod
    async def _run_scattmgr_command(
        cls,
        verb: str,
        *arguments: str,
        error_ok=False,
    ) -> Tuple[int | None, str]:
        if SACCTMGR_PATH is None:
            raise ImportError("sacctmgr could not be found in path.")

        process = await asyncio.create_subprocess_exec(
            SACCTMGR_PATH,
            verb,
            *arguments,
            "--parsable2",  # seperate values by a PIPE
            "--noheader",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        process_output = (stderr if len(stderr) > 0 else stdout).decode("ascii")

        if not error_ok and process.returncode != 0:
            raise SlurmAccountManagerError(process_output)

        return process.returncode, process_output

    @classmethod
    async def _scattmgr_show(
        cls,
        fields: Sequence[str],
        filters: Optional[Mapping[str, str]] = None,
    ) -> Sequence[Dict[str, str]]:
        format_string = "format=" + ",".join(fields)

        filter_args: list[str] = []
        if filters is not None and len(filters) > 0:
            filter_args.append("where")
            for column, value in filters.items():
                filter_args.append(f"{column}={value}")

        exit_code, process_output = await cls._run_scattmgr_command(
            "show", cls.__name__, *cls.query_options, format_string, *filter_args
        )

        return [
            dict(zip(fields, line.split("|"))) for line in process_output.splitlines()
        ]

    @classmethod
    def _response_to_attributes(
        cls: Type[ObjectType], response_data: Dict[str, str]
    ) -> Dict[str, str | None]:
        empty_variants = ("0-00:00:00", "", "0", "00:00:00")
        return {
            # some values will always return a value although they are empty
            # we need to ignore them here to make sure not to set these values
            # when saving the object again
            camel_to_snake_case(key): value if value not in empty_variants else None
            for key, value in response_data.items()
        }

    @classmethod
    def _response_to_instances(
        cls: Type[ObjectType], response: Sequence[Dict[str, str]]
    ) -> Sequence[ObjectType]:
        instances: list[ObjectType] = []
        for data in response:
            attributes = cls._response_to_attributes(data)
            instance = cls(**attributes)
            instances.append(instance)
        return instances

    def _to_query(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        update_fields: Dict[str, str] = {}
        filter_fields: Dict[str, str] = {}

        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.name in self._primary_key_fields:
                filter_fields[snake_to_camel_case(field.name)] = value
            elif (
                field.name not in (self._read_only_fields + self._synthetic_fields)
                and value
            ):
                update_fields[snake_to_camel_case(field.name)] = value

        return update_fields, filter_fields

    @classmethod
    async def filter(cls: Type[ObjectType], **filters: str) -> Sequence[ObjectType]:
        object_fields = [
            snake_to_camel_case(field.name)
            for field in dataclasses.fields(cls)
            if field.name not in (cls._synthetic_fields + cls._write_only_fields)
        ]
        object_data = await cls._scattmgr_show(
            object_fields,
            filters,
        )
        instances = cls._response_to_instances(object_data)
        # only return the queried type of instance
        return [instance for instance in instances if isinstance(instance, cls)]

    @classmethod
    async def all(cls) -> Sequence[ObjectType]:
        return await cls.filter()

    @classmethod
    async def get(cls, **filters: str) -> ObjectType:
        user_list = await cls.filter(**filters)
        if len(user_list) == 0:
            raise NotFound(cls)
        if len(user_list) > 1:
            raise MultipleObjectReturned(cls)
        return user_list[0]

    async def refresh_from_db(self):
        _, filters = self._to_query()
        new_object = await self.get(**filters)
        for field in dataclasses.fields(new_object):
            del self.__dict__[field.name]
            setattr(self, field.name, getattr(new_object, field.name))


class WritableSlurmObject(SlurmObject[WritableObjectType]):
    @classmethod
    async def _scattmgr_write(
        cls,
        verb: str,
        new_values: Mapping[str, str],
        filters: Mapping[str, str],
    ) -> Sequence[str]:
        filter_args: list[str] = []
        if len(filters) > 0:
            filter_args.append("where")
            for column, value in filters.items():
                filter_args.append(f"{column}={value}")

        update_args: list[str] = []
        if len(new_values) > 0:
            if verb == "modify":
                update_args.append("set")
            for field_name, new_value in new_values.items():
                update_args.append(f"{field_name}={new_value}")

        exit_code, process_output = await cls._run_scattmgr_command(
            verb,
            cls.__name__,
            *filter_args,
            *update_args,
            "--immediate",
            error_ok=True,
        )

        if exit_code != 0:
            raise SlurmAccountManagerError(process_output)

        if verb == "create":
            if "Nothing new added." in process_output:
                return []
            else:
                return [""]

        modified_objects = []
        # the processoutput is grouped into one or more sections where
        # the first section always lists the modified objects and other
        # sections may list related modifications
        section = 0
        for line in process_output.splitlines():
            # section headers are formatted like: "Modified <objecttype>..."
            if line.endswith("..."):
                section += 1
                continue
            if section == 1:
                modified_objects.append(line.strip())
            else:
                break

        return modified_objects

    @classmethod
    async def create(cls: Type[WritableObjectType], **attrs) -> WritableObjectType:
        created_objects = await cls._scattmgr_write("create", attrs, {})
        if len(created_objects) == 1:
            return await cls.get(**attrs)
        else:
            raise SlurmAccountManagerError(
                f"Failed to create new {cls.__name__}. Maybe the object already existed?"
            )

    async def save(self) -> bool:
        created = False
        updates, filters = self._to_query()

        updated_keys = await self._scattmgr_write("modify", updates, filters)
        if len(updated_keys) == 0:
            # the object was not yet present in the db, create a new one
            await self._scattmgr_write("create", updates | filters, {})
            created = True
        elif len(updated_keys) > 1:
            raise AssertionError(
                f"Modified more than a single Object!. Modified keys: {updated_keys}"
            )

        await self.refresh_from_db()

        return created

    async def delete(self) -> bool:
        _, filters = self._to_query()

        updated_keys = await self._scattmgr_write("delete", {}, filters)
        if len(updated_keys) > 1:
            raise AssertionError(
                f"Deleted more than a single Object!. Deleted keys: {updated_keys}"
            )

        return len(updated_keys) == 1


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
    default_account: str | None = dataclasses.field(default=None, metadata=WRITEONLY)

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
