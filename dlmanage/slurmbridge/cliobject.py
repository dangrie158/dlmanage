from abc import ABC, abstractmethod
import dataclasses
from typing import Any, ClassVar, Generic, Sequence, Tuple, Type, TypeVar, Dict, List


READONLY = {"readonly": True}
WRITEONLY = {"writeonly": True}
SYNTHETIC = {"synthetic": True}
PRIMARYKEY = {"primarykey": True}


class SlurmObjectException(Exception):
    def __init__(self, object_class, *args):
        self.object_class = object_class
        super().__init__(object_class, *args)

    def __str__(self) -> str:
        return f"{self.object_class.__name__} - {self.__class__.__name__}"


class NotFound(SlurmObjectException):
    pass


class MultipleObjectReturned(SlurmObjectException):
    pass


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


ObjectType = TypeVar("ObjectType", bound="SlurmCLIObject")


class SlurmCLIObject(ABC, Generic[ObjectType]):
    _primary_key_fields: ClassVar[List[str]]
    _read_only_fields: ClassVar[List[str]]
    _write_only_fields: ClassVar[List[str]]
    _synthetic_fields: ClassVar[List[str]]

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

    @classmethod
    async def all(cls) -> Sequence[ObjectType]:
        return await cls.filter()

    @classmethod
    async def get(cls, **filters: str) -> ObjectType:
        object_list = await cls.filter(**filters)
        if len(object_list) == 0:
            raise NotFound(cls)
        if len(object_list) > 1:
            raise MultipleObjectReturned(cls)
        return object_list[0]

    async def refresh_from_db(self):
        _, filters = self._to_query()
        new_object = await self.get(**filters)
        for field in dataclasses.fields(new_object):
            del self.__dict__[field.name]
            setattr(self, field.name, getattr(new_object, field.name))

    @classmethod
    @abstractmethod
    async def filter(cls: Type[ObjectType], **filters: str) -> Sequence[ObjectType]:
        ...

    @abstractmethod
    def _to_query(self) -> Tuple[Dict[str, str], Dict[str, str]]:
        ...
