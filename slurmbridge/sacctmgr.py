from __future__ import annotations

import dataclasses
from abc import ABC
import asyncio
import shutil
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
)

from slurmbridge.common import (
    NotFound,
    camel_to_snake_case,
    snake_to_camel_case,
    MultipleObjectReturned,
)

SACCTMGR_PATH = shutil.which("sacctmgr")


class SlurmAccountManagerError(Exception):
    def __str__(self) -> str:
        return f"{self.args[0]}"


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
