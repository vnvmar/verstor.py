from __future__ import annotations
from typing import ClassVar, Never
from uuid import uuid4

from semver import Version
from pydantic import BaseModel, ConfigDict, Field, JsonValue

type JsonObject = dict[str, JsonValue]
INITIAL_VERSION = Version.parse("1.0.0")

class EntityBase(BaseModel):
    tag: ClassVar[str]

    def patch[T: EntityBase](self: T, **changes: Never) -> T:
        fields = type(self).model_fields
        unknown = sorted(key for key in changes if key not in fields)
        if unknown:
            joined = ", ".join(repr(key) for key in unknown)
            raise TypeError(f"Unknown patch field(s): {joined}")

        data = self.model_dump()
        data.update(changes)
        return type(self).model_validate(data)

    def ref[T: EntityBase](self: T, id: str | None = None) -> EntityRef[T]:
        return EntityRef[T].of(type(self), id)

    def ver[T: EntityBase](self: T, id: str | None = None, version: str | Version | None = None) -> EntityVersion[T]:
        return EntityVersion[T].from_model(self, id, version)


class EntityRef[T: EntityBase](BaseModel):
    id: str = Field(
        default_factory=lambda: str(uuid4()),
        description="ID of the referenced instance."
    )

    t: type[T]
    model_config = ConfigDict(arbitrary_types_allowed=True)

    @classmethod
    def of(cls, t: type[T], id: str | None = None) -> EntityRef[T]:
        return cls(t=t, id=id or str(uuid4()))

    @property
    def tag(self) -> str:
        return self.t.tag

    def ref(self, id: str | None = None) -> EntityRef[T]:
        return self.of(self.t, id or self.id)


class EntityVersion[T: EntityBase](EntityRef[T]):
    version: str = Field(
        default_factory=lambda: str(INITIAL_VERSION),
        description="Version of the referenced instance."
    )
    entity: T = Field(
        default=...,
        description="Versioned entity."
    )

    @classmethod
    def from_model(cls, entity: T, id: str | None = None, version: str | Version | None = None) -> EntityVersion[T]:
        _version = str(version) if isinstance(version, Version) else version
        return cls(t=type(entity), entity=entity, id=id or str(uuid4()), version=_version or str(INITIAL_VERSION))

    def ref(self, id: str | None = None) -> EntityRef[T]:
        return EntityRef[T].of(self.t, id or self.id)

    def ver(self, id: str | None = None, version: str | Version | None = None) -> EntityVersion[T]:
        return EntityVersion[T].from_model(self.entity, id or self.id, version or self.version)


def ref[T: EntityBase](entity: T | type[T], id: str | None) -> EntityRef[T]:
    return EntityRef[T].of(entity if isinstance(entity, type) else type(entity), id)

def ver[T: EntityBase](entity: T, id: str | None = None, version: str | Version | None = None) -> EntityVersion[T]:
    return EntityVersion[T].from_model(entity, id, version)
