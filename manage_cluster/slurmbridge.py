from __future__ import annotations

import dataclasses
from functools import cached_property
import re
import subprocess
import shutil
from sys import stderr
from typing import (
    Any,
    Dict,
    Generic,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)
from xml.dom.minidom import Attr

SACCTMGR_PATH = shutil.which("sacctmgr")


def camel_to_snake_case(input: str) -> str:
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", input).lower()


def snake_to_camel_case(input: str) -> str:
    return "".join([part.title() for part in input.split("_")])


class SlurmAccountManagerError(Exception):
    pass


class SlurmResourceException(Exception):
    def __init__(self, resource_class):
        self.resource_class = resource_class

    def __repr__(self) -> str:
        return f"{self.resource_class.__name__} - {self.__class__.__name__}"


class NotFound(SlurmResourceException):
    pass


class MultipleObjectReturned(SlurmResourceException):
    pass


class ReadOnlyStringField(str):
    pass


class PrimaryStringField(ReadOnlyStringField):
    pass


ResourceType = TypeVar("ResourceType")


@dataclasses.dataclass
class SlurmResource(Generic[ResourceType]):
    def __setattr__(self, __name: str, __value: Any) -> None:
        if hasattr(self, __name) and __name in self._read_only_fields:
            raise AttributeError(f"{__name} is a reead-only field")
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
        *arguments: str,
    ) -> str:
        if SACCTMGR_PATH is None:
            raise ImportError("sacctmgr could not be found in path.")

        process = subprocess.run(
            args=[
                SACCTMGR_PATH,
                *arguments,
                "--parsable2",  # seperate values by a PIPE
                "--noheader",
            ],
            capture_output=True,
            timeout=5,
        )

        if process.returncode != 0:
            error_message = process.stderr if len(process.stderr)>0 else process.stdout
            raise SlurmAccountManagerError(error_message.decode("ascii"))

        return process.stdout.decode("ascii")

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
            "show", cls.__name__, format_string, *filter_args
        )

        return [
            dict(zip(fields, line.split("|"))) for line in process_output.splitlines()
        ]

    @classmethod
    def _scattmgr_modify(
        cls,
        new_values: Mapping[str, str],
        filters: Mapping[str, str],
    ) -> Sequence[str]:
        filter_args: list[str] = []
        if len(filters) > 0:
            filter_args.append("where")
            for column, value in filters.items():
                filter_args.append(f"{column}={value}")

        update_args: list[str] = ["set"]
        for field_name, new_value in new_values.items():
            update_args.append(f"{field_name}={new_value}")

        process_output = cls._run_scattmgr_command(
            "modify", cls.__name__, *filter_args, *update_args, "--immediate"
        )

        return [entry.strip() for entry in process_output.splitlines()[1:]]

    @classmethod
    def _response_to_instances(
        cls: Type[ResourceType], response: Sequence[Dict[str, str]]
    ) -> Sequence[ResourceType]:
        instances: list[ResourceType] = []
        for data in response:
            attrs = {camel_to_snake_case(key): value for key, value in data.items()}
            instance = cls(**attrs)
            instances.append(instance)
        return instances

    def _to_query(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        update_fields: Dict[str, str] = {}
        filter_fields: Dict[str, str] = {}

        for field, value in dataclasses.asdict(self).items():
            if field in self._primary_key_fields:
                filter_fields[snake_to_camel_case(field)] = value
            elif field not in self._read_only_fields:
                update_fields[snake_to_camel_case(field)] = value

        return update_fields, filter_fields

    @classmethod
    def filter(cls, **filters: str) -> Sequence[ResourceType]:
        resource_fields = [
            snake_to_camel_case(field.name) for field in dataclasses.fields(cls)
        ]
        user_data = cls._scattmgr_show(
            resource_fields,
            filters,
        )
        return cls._response_to_instances(user_data)

    @classmethod
    def all(cls) -> Sequence[ResourceType]:
        return cls.filter()

    @classmethod
    def get(cls, **filters: str) -> ResourceType:
        user_list = cls.filter(**filters)
        if len(user_list) == 0:
            raise NotFound(cls)
        if len(user_list) > 1:
            raise MultipleObjectReturned(cls)
        return user_list[0]

    def save(self):
        updates, filters = self._to_query()
        updated_keys = self._scattmgr_modify(updates, filters)
        if len(updated_keys) != 1:
            raise AssertionError(
                f"Modified more than a single Object!. Modified keys: {updated_keys}"
            )


@dataclasses.dataclass
class User(SlurmResource["User"]):
    user: PrimaryStringField
    default_account: str


@dataclasses.dataclass
class Association(SlurmResource["Association"]):
    cluster: PrimaryStringField
    account: PrimaryStringField
    user: PrimaryStringField
    partition: str
    grp_jobs: str
    grp_tres: str
    grp_submit: str
    grp_tres_mins: str
    grp_wall: str
    max_jobs: str
    max_tres_per_job: str
    max_tres_per_node: str
    default_qos: str
