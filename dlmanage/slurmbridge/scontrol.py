from __future__ import annotations

import dataclasses
import asyncio
import shutil
from typing import (
    ClassVar,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
)
from dlmanage.slurmbridge.cliobject import SlurmCLIObject, SlurmObjectException

from dlmanage.slurmbridge.common import (
    camel_to_snake_case,
    snake_to_camel_case,
)

SCONTROL_PATH = shutil.which("scontrol")


class SlurmControlError(SlurmObjectException):
    def __str__(self) -> str:
        return f"{self.args[1]}"


SlurmControlObjectType = TypeVar("SlurmControlObjectType", bound="SlurmControlObject")


class SlurmControlObject(SlurmCLIObject[SlurmControlObjectType]):
    query_options: ClassVar[Sequence[str]] = tuple()
    _primary_key_field: ClassVar[str]

    def __init_subclass__(cls) -> None:
        super().__init_subclass__()
        if len(cls._primary_key_fields) != 1:
            raise SlurmControlError(
                cls,
                f"{cls.__name__} must define exactly one field with primary_key=True",
            )
        cls._primary_key_field = cls._primary_key_fields[0]

    @classmethod
    async def _run_scontrol_command(
        cls,
        verb: str,
        *arguments: str,
        error_ok=False,
    ) -> Tuple[int | None, str]:
        if SCONTROL_PATH is None:
            raise ImportError("scontrol could not be found in path.")

        process = await asyncio.create_subprocess_exec(
            SCONTROL_PATH,
            verb,
            *arguments,
            "--oneline",
            "--detail",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await process.communicate()

        process_output = (stderr if len(stderr) > 0 else stdout).decode("ascii")

        if not error_ok and process.returncode != 0:
            raise SlurmControlError(cls, process_output)

        return process.returncode, process_output

    @classmethod
    async def _scontrol_show(
        cls,
        fields: Sequence[str],
        filter: Optional[str] = None,
    ) -> Sequence[Dict[str, str]]:
        filter_args = [filter] if filter is not None else []

        exit_code, process_output = await cls._run_scontrol_command(
            "show", cls.__name__, *filter_args, *cls.query_options
        )
        if exit_code != 0:
            raise SlurmControlError(cls, process_output)

        all_objects = []
        case_insensitive_fields = [field_name.lower() for field_name in fields]
        # we use --oneline to make sure we have exactly 1 object per line
        for object_reponse in process_output.splitlines():
            # attributes are seperated by any whitespace
            object_attributes = {}
            for attribute_string in object_reponse.split():
                # sometimes an attribute is not followed by a value (e.g. "Name=")
                # so we pack second argument to a list but thanks to maxsplit=1
                # this list is either empty or has exactly one entry
                attribute_name, *value = attribute_string.split("=", maxsplit=1)
                # we need to use case insensitive matching here since slurm
                # uses SREAMINGCASE for abbrevations (e.g. TRES instead of Tres)
                if attribute_name.lower() in case_insensitive_fields:
                    object_attributes[attribute_name] = (
                        value[0] if len(value) == 1 else ""
                    )
            all_objects.append(object_attributes)

        return all_objects

    @classmethod
    async def _scontrol_update(
        cls,
        new_values: Mapping[str, str],
        filter: str,
    ):
        update_args: list[str] = []
        for field_name, new_value in new_values.items():
            update_args.append(f"{field_name}={new_value}")

        exit_code, process_output = await cls._run_scontrol_command(
            "update",
            cls.__name__,
            filter,
            *update_args,
            error_ok=True,
        )

        if exit_code != 0:
            raise SlurmControlError(cls, process_output)

    @classmethod
    def _response_to_attributes(
        cls: Type[SlurmControlObjectType], response_data: Dict[str, str]
    ) -> Dict[str, str | None]:
        empty_variants = ("(null)", "N/A")
        return {
            # some values will always return a value although they are empty
            # we need to ignore them here to make sure not to set these values
            # when saving the object again
            camel_to_snake_case(key): value if value not in empty_variants else None
            for key, value in response_data.items()
        }

    @classmethod
    def _response_to_instances(
        cls: Type[SlurmControlObjectType], response: Sequence[Dict[str, str]]
    ) -> Sequence[SlurmControlObjectType]:
        instances: list[SlurmControlObjectType] = []
        for data in response:
            attributes = cls._response_to_attributes(data)
            instance = cls(**attributes)
            instances.append(instance)
        return instances

    def _to_query(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        update_fields: Dict[str, str] = {}
        filter_values = {}

        for field in dataclasses.fields(self):
            value = getattr(self, field.name)
            if field.name in self._primary_key_fields:
                filter_values[field.name] = value
            elif (
                field.name not in (self._read_only_fields + self._synthetic_fields)
                and value
            ):
                update_fields[snake_to_camel_case(field.name)] = value

        return update_fields, filter_values

    @classmethod
    async def filter(
        cls: Type[SlurmControlObjectType], **filters: str
    ) -> Sequence[SlurmControlObjectType]:
        if len(filters) > 1:
            raise SlurmControlError(
                cls,
                f"{cls.__name__} only supports a single filter attribute: {cls._primary_key_field}",
            )
        filter_value = filters.get(cls._primary_key_field, None)
        object_fields = [
            snake_to_camel_case(field.name)
            for field in dataclasses.fields(cls)
            if field.name not in (cls._synthetic_fields + cls._write_only_fields)
        ]
        object_data = await cls._scontrol_show(object_fields, filter_value)
        instances = cls._response_to_instances(object_data)
        # only return the queried type of instance
        return [instance for instance in instances if isinstance(instance, cls)]

    async def save(self):
        updates, filters = self._to_query()

        await self._scontrol_update(updates, list(filters.values())[0])
        await self.refresh_from_db()
