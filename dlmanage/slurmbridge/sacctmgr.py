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

SACCTMGR_PATH = shutil.which("sacctmgr")


class SlurmAccountManagerError(SlurmObjectException):
    def __str__(self) -> str:
        return f"{self.args[1]}"


AccountManagerObjectType = TypeVar(
    "AccountManagerObjectType", bound="SlurmAccountManagerObject"
)


class SlurmAccountManagerObject(SlurmCLIObject[AccountManagerObjectType]):
    query_options: ClassVar[Sequence[str]] = tuple()

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
            raise SlurmAccountManagerError(cls, process_output)

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
        cls: Type[AccountManagerObjectType], response_data: Dict[str, str]
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
        cls: Type[AccountManagerObjectType], response: Sequence[Dict[str, str]]
    ) -> Sequence[AccountManagerObjectType]:
        instances: list[AccountManagerObjectType] = []
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
    async def filter(
        cls: Type[AccountManagerObjectType], **filters: str
    ) -> Sequence[AccountManagerObjectType]:
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


WritableAccountManagerObjectType = TypeVar(
    "WritableAccountManagerObjectType", bound="WritableSlurmAccountManagerObject"
)


class WritableSlurmAccountManagerObject(
    SlurmAccountManagerObject[WritableAccountManagerObjectType]
):
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
            raise SlurmAccountManagerError(cls, process_output)

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
    async def create(
        cls: Type[WritableAccountManagerObjectType], **attrs
    ) -> WritableAccountManagerObjectType:
        created_objects = await cls._scattmgr_write("create", attrs, {})
        if len(created_objects) == 1:
            return await cls.get(**attrs)
        else:
            raise SlurmAccountManagerError(
                cls,
                f"Failed to create new {cls.__name__}. Maybe the object already existed?",
            )

    async def save(self, allow_multiple_affected=False) -> bool:
        created = False
        updates, filters = self._to_query()

        updated_keys = await self._scattmgr_write("modify", updates, filters)
        if len(updated_keys) == 0:
            # the object was not yet present in the db, create a new one
            await self._scattmgr_write("create", updates | filters, {})
            created = True
        elif len(updated_keys) > 1 and not allow_multiple_affected:
            raise SlurmAccountManagerError(
                f"Modified more than a single Object!. Modified keys: {updated_keys}"
            )

        await self.refresh_from_db()

        return created

    async def delete(self) -> bool:
        _, filters = self._to_query()

        updated_keys = await self._scattmgr_write("delete", {}, filters)
        if len(updated_keys) > 1:
            raise SlurmAccountManagerError(
                f"Deleted more than a single Object!. Deleted keys: {updated_keys}"
            )

        return len(updated_keys) == 1
