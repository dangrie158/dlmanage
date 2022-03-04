from __future__ import annotations

from dataclasses import dataclass
from email import message
from importlib.resources import Resource
import re
import subprocess
import shutil
from typing import Dict, Generic, List, Mapping, Optional, Sequence, Type, TypeVar

SACCTMGR_PATH = shutil.which("sacctmgr")


def camel_to_snake_case(input: str) -> str:
    return re.sub("([a-z0-9])([A-Z])", r"\1_\2", input).lower()


def snake_to_camel_case(input: str) -> str:
    return "".join([part.title() for part in input.split("_")])


class SlurmResourceException(Exception):
    def __init__(self, resource_class):
        self.resource_class = resource_class

    def __repr__(self) -> str:
        return f"{self.resource_class.__name__} - {self.__class__.__name__}"


class NotFound(SlurmResourceException):
    pass


class MultipleObjectReturned(SlurmResourceException):
    pass


class PrimaryStringField(str):
    pass


ResourceType = TypeVar("ResourceType")


@dataclass
class SlurmResource(Generic[ResourceType]):
    def __init_subclass__(cls) -> None:
        super().__init_subclass__()

        pk_fields: List[str] = []
        for field_name, type_name in cls.__annotations__.items():
            if type_name == PrimaryStringField.__name__:
                pk_fields.append(field_name)

        if len(pk_fields) == 0:
            raise AssertionError(
                "You need to specify at least one field with type PrimaryStringField"
            )
        setattr(cls, "_primary_key_fields", pk_fields)

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
            # check=True,
        )
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
    ) -> None:
        filter_args: list[str] = []
        if len(filters) > 0:
            filter_args.append("where")
            for column, value in filters.items():
                filter_args.append(f"{column}={value}")

        update_args: list[str] = ["set"]
        for field_name, new_value in new_values.items():
            update_args.append(f"{field_name}={new_value}")

        cls._run_scattmgr_command(
            "modify", cls.__name__, *filter_args, *update_args, "--immediate"
        )

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

    def _to_query(self: ResourceType) -> Dict[str, str]:
        query_fields: Dict[str, str] = {}

        for field, value in self.__dict__.items():
            query_fields[snake_to_camel_case(field)] = value

        return query_fields

    @classmethod
    def filter(cls, **filters: str) -> Sequence[ResourceType]:
        resource_fields = [
            snake_to_camel_case(field_name)
            for field_name in cls.__dataclass_fields__.keys()
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
        params = self._to_query()
        pk_fields = self.__class__._primary_key_fields
        pk_field_row_names = [
            snake_to_camel_case(field_name) for field_name in pk_fields
        ]

        # remove the pk fields as we can't update those
        for pk_in_query in pk_field_row_names:
            params.pop(pk_in_query)

        print(params)
        self._scattmgr_modify(
            params, {field: getattr(self, field) for field in pk_fields}
        )


@dataclass
class User(SlurmResource["User"]):
    user: PrimaryStringField
    default_account: str


@dataclass
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
    grp_tres: str
    max_jobs: str
    max_tres_per_job: str
    max_tres_per_node: str
    default_qos: str
