from __future__ import annotations

import dataclasses
from datetime import datetime
from functools import cached_property
from glob import glob
import os
import re
import subprocess
import shutil
from typing import (
    Any,
    ClassVar,
    Dict,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
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
    pass


class SlurmObjectException(Exception):
    def __init__(self, Object_class):
        self.Object_class = Object_class

    def __repr__(self) -> str:
        return f"{self.Object_class.__name__} - {self.__class__.__name__}"


class NotFound(SlurmObjectException):
    pass


class MultipleObjectReturned(SlurmObjectException):
    pass


class ReadOnlyStringField(str):
    pass


class PrimaryStringField(ReadOnlyStringField):
    pass


ObjectType = TypeVar("ObjectType", bound="SlurmObject")


@dataclasses.dataclass
class SlurmObject(Generic[ObjectType]):
    query_options: ClassVar[Sequence[str]] = tuple()

    def __setattr__(self, __name: str, __value: Any) -> None:
        if hasattr(self, __name) and __name in self._read_only_fields:
            raise AttributeError(f"{__name} is a read-only field")
        return super().__setattr__(__name, __value)

    @cached_property
    def _primary_key_fields(self) -> Sequence[str]:
        pk_field_types = [PrimaryStringField, *PrimaryStringField.__subclasses__()]
        pk_field_type_names = [cls.__name__ for cls in pk_field_types]
        return [
            field.name
            for field in dataclasses.fields(self)
            if field.type in pk_field_type_names
        ]

    @cached_property
    def _read_only_fields(self) -> Sequence[str]:
        ro_field_types = [ReadOnlyStringField, *ReadOnlyStringField.__subclasses__()]
        ro_field_type_names = [cls.__name__ for cls in ro_field_types]
        return [
            field.name
            for field in dataclasses.fields(self)
            if field.type in ro_field_type_names
        ]

    @classmethod
    def _run_scattmgr_command(
        cls,
        verb: str,
        *arguments: str,
        error_ok=False,
    ) -> str:
        if SACCTMGR_PATH is None:
            raise ImportError("sacctmgr could not be found in path.")

        process = subprocess.run(
            args=[
                SACCTMGR_PATH,
                verb,
                *arguments,
                "--parsable2",  # seperate values by a PIPE
                "--noheader",
            ],
            capture_output=True,
            timeout=5,
        )

        process_output = (
            process.stderr if len(process.stderr) > 0 else process.stdout
        ).decode("ascii")

        if not error_ok and process.returncode != 0:
            raise SlurmAccountManagerError(process_output)

        return process_output

    @classmethod
    def _scattmgr_show(
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

        process_output = cls._run_scattmgr_command(
            "show", cls.__name__, *cls.query_options, format_string, *filter_args
        )

        return [
            dict(zip(fields, line.split("|"))) for line in process_output.splitlines()
        ]

    @classmethod
    def _scattmgr_write(
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

        process_output = cls._run_scattmgr_command(
            verb, cls.__name__, *filter_args, *update_args, "--immediate", error_ok=True
        )

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
    def _response_to_instances(
        cls: Type[ObjectType], response: Sequence[Dict[str, str]]
    ) -> Sequence[ObjectType]:
        instances: list[ObjectType] = []
        empty_variants = ("0-00:00:00", "", "0", "00:00:00")
        for data in response:
            attrs = {
                # some values will always return a value although they are empty
                # we need to ignore them here to make sure not to set these values
                # when saving the object again
                camel_to_snake_case(key): value if value not in empty_variants else None
                for key, value in data.items()
            }
            instance = cls(**attrs)
            instances.append(instance)
        return instances

    def _to_query(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        update_fields: Dict[str, str] = {}
        filter_fields: Dict[str, str] = {}

        for field, value in dataclasses.asdict(self).items():
            if field in self._primary_key_fields:
                filter_fields[snake_to_camel_case(field)] = value
            elif field not in self._read_only_fields and value:
                update_fields[snake_to_camel_case(field)] = value

        return update_fields, filter_fields

    @classmethod
    def filter(cls, **filters: str) -> Sequence[ObjectType]:
        Object_fields = [
            snake_to_camel_case(field.name) for field in dataclasses.fields(cls)
        ]
        object_data = cls._scattmgr_show(
            Object_fields,
            filters,
        )
        return cls._response_to_instances(object_data)

    @classmethod
    def all(cls) -> Sequence[ObjectType]:
        return cls.filter()

    @classmethod
    def get(cls, **filters: str) -> ObjectType:
        user_list = cls.filter(**filters)
        if len(user_list) == 0:
            raise NotFound(cls)
        if len(user_list) > 1:
            raise MultipleObjectReturned(cls)
        return user_list[0]

    @classmethod
    def create(cls: Type[ObjectType], **attrs) -> ObjectType:
        new_object = cls(**attrs)
        created = new_object.save()
        if not created:
            raise AssertionError(
                f"Failed to create new {cls.__name__}. Maybe the object already existed?"
            )
        return new_object

    def save(self) -> bool:
        created = False
        updates, filters = self._to_query()

        updated_keys = self._scattmgr_write("modify", updates, filters)
        if len(updated_keys) == 0:
            # the object was not yet present in the db, create a new one
            self._scattmgr_write("create", updates | filters, {})
            created = True
        elif len(updated_keys) > 1:
            raise AssertionError(
                f"Modified more than a single Object!. Modified keys: {updated_keys}"
            )

        self.refresh_from_db()

        return created

    def refresh_from_db(self):
        _, filters = self._to_query()
        new_object = self.get(**filters)
        for field in dataclasses.fields(new_object):
            del self.__dict__[field.name]
            setattr(self, field.name, getattr(new_object, field.name))

    def delete(self) -> bool:
        _, filters = self._to_query()

        updated_keys = self._scattmgr_write("delete", {}, filters)
        if len(updated_keys) > 1:
            raise AssertionError(
                f"Deleted more than a single Object!. Deleted keys: {updated_keys}"
            )

        return len(updated_keys) == 1

    def copy(self) -> ObjectType:
        attributes = dataclasses.asdict(self)
        return self.__class__(**attributes)


@dataclasses.dataclass
class AssociationBaseObject(SlurmObject[ObjectType]):
    _: dataclasses.KW_ONLY
    parent_id: ReadOnlyStringField = dataclasses.field(repr=False)
    parent_name: ReadOnlyStringField = dataclasses.field(repr=False)
    grp_tres_mins: Optional[str] = dataclasses.field(repr=False, default=None)
    grp_tres_run_mins: Optional[str] = dataclasses.field(repr=False, default=None)
    grp_tres: Optional[str] = dataclasses.field(repr=False, default=None)
    grp_jobs: Optional[str] = dataclasses.field(repr=False, default=None)
    grp_submit_jobs: Optional[str] = dataclasses.field(repr=False, default=None)
    grp_wall: Optional[str] = dataclasses.field(repr=False, default=None)
    max_tres_mins_per_job: Optional[str] = dataclasses.field(repr=False, default=None)
    max_tres_per_job: Optional[str] = dataclasses.field(repr=False, default=None)
    max_tres_per_node: Optional[str] = dataclasses.field(repr=False, default=None)
    max_wall_duration_per_job: Optional[str] = dataclasses.field(
        repr=False, default=None
    )
    fairshare: Optional[str] = dataclasses.field(repr=False, default=None)
    max_jobs: Optional[str] = dataclasses.field(repr=False, default=None)
    max_submit_jobs: Optional[str] = dataclasses.field(repr=False, default=None)
    qos: Optional[str] = dataclasses.field(repr=False, default=None)

    @property
    def parent(self) -> Association | None:
        if self.parent_id:
            return Association.get(id=self.parent_id)
        return None

    @property
    def association(self) -> Association:
        raise NotImplemented

    @property
    def max_gpus(self):
        return get_gres_value(self.association.grp_tres, "gres/gpu")

    @max_gpus.setter
    def max_gpus(self, new_val):
        self.grp_tres = update_gres_value(self.grp_tres, "gres/gpu", new_val)

    @property
    def max_cpus(self):
        return get_gres_value(self.association.grp_tres, "cpu")

    @max_cpus.setter
    def max_cpus(self, new_val):
        self.grp_tres = update_gres_value(self.grp_tres, "cpu", new_val)


@dataclasses.dataclass
class User(AssociationBaseObject["User"], SlurmObject["User"]):
    query_options: ClassVar[Sequence[str]] = ("withassoc",)

    user: PrimaryStringField
    default_account: str

    @property
    def account(self) -> Account | None:
        try:
            return Account.get(account=self.default_account)
        except NotFound:
            return None

    def set_account(self, new_account: Account):
        old_account = self.account

        # create a new user to generate the association with User + Account
        new_user = self.copy()
        del new_user.default_account
        new_user.default_account = new_account.account
        updates, filters = new_user._to_query()
        self._scattmgr_write("create", updates | filters, {})

        # delete the old association (this is done by removing the user with an additional account filter)
        if old_account is not None:
            _, filters = self._to_query()
            filters["account"] = old_account.account
            self._scattmgr_write("delete", {}, filters)

        self.refresh_from_db()

    @property
    def association(self) -> Association:
        return Association.get(user=self.user, account=self.default_account)

    @cached_property
    def home_directory(self) -> str | None:
        return find_home_directory(self.user)


@dataclasses.dataclass
class Account(AssociationBaseObject["Account"], SlurmObject["Account"]):
    account: PrimaryStringField

    @property
    def association(self) -> Association:
        return Association.get(user="", account=self.account)


@dataclasses.dataclass
class Association(AssociationBaseObject["Association"], SlurmObject["Association"]):
    cluster: PrimaryStringField
    account: PrimaryStringField
    user: PrimaryStringField
    partition: str

    @property
    def association(self) -> Association:
        return self

    def __repr__(self):
        if self.user:
            return f"User {self.user}"

        return f"Account {self.account}"


@dataclasses.dataclass
class QOS(SlurmObject["QOS"]):
    user: PrimaryStringField
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
