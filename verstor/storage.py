
import json
from pathlib import Path
from typing import ClassVar, Mapping, Protocol, Self, overload, runtime_checkable

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient
from conf import Config
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator
from semver import Version

from verstor.types import INITIAL_VERSION, EntityBase, EntityRef, EntityVersion

type _GetDispatchKey = type[EntityVersion[EntityBase]] | type[EntityRef[EntityBase]] | type[type]
type _SetDispatchKey = type[EntityVersion[EntityBase]] | type[EntityRef[EntityBase]] | type[str] | None
type _ResolvedEntityTarget[T: EntityBase] = tuple[type[T], str, str]


@runtime_checkable
class Storage(Protocol):
    @overload
    def get[T: EntityBase](self, target: EntityVersion[T]) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: EntityRef[T]) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: type[T], id: str) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: type[T], id: str, version: str) -> EntityVersion[T]: ...
    def get[T: EntityBase](
        self,
        target: EntityRef[T] | EntityVersion[T] | type[T],
        id: str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: EntityVersion[T]) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: EntityRef[T]) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: str) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: str, version: str) -> EntityVersion[T]: ...
    def set[T: EntityBase](
        self,
        entity: T,
        target: EntityRef[T] | EntityVersion[T] | str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]: ...


class FileStorage(BaseModel):
    base: Path = Field(
        default=...,
        description="Base path of the file storage.",
        kw_only=False,
    )
    active: ClassVar[str] = "active.json"

    def resolve_entity_dir(self, tag: str, id: str) -> Path:
        return self.base / tag / id

    def resolve_active_path(self, tag: str, id: str) -> Path:
        return self.resolve_entity_dir(tag, id) / self.active

    def resolve_version_path(self, tag: str, id: str, version: str | Version) -> Path:
        return self.resolve_entity_dir(tag, id) / str(version)

    def read_active_version(self, tag: str, id: str) -> str:
        content = self.resolve_active_path(tag, id).read_text(encoding="utf-8")
        version = json.loads(content)
        if not isinstance(version, str):
            raise TypeError("active.json must contain a JSON string version")
        return version

    def write_active_version(self, tag: str, id: str, version: str) -> None:
        path = self.resolve_active_path(tag, id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(version), encoding="utf-8")

    def list_versions(self, tag: str, id: str) -> list[Version]:
        versions: list[Version] = []
        directory = self.resolve_entity_dir(tag, id)
        if not directory.exists():
            return versions
        for path in directory.iterdir():
            if path.name == self.active or not path.is_file():
                continue
            try:
                versions.append(Version.parse(path.name))
            except ValueError:
                continue
        return sorted(versions)


class _StorageDispatchMixin:
    _GET_DISPATCH: ClassVar[Mapping[_GetDispatchKey, str]] = {
        EntityVersion: "_get_from_entity_version_target",
        EntityRef: "_get_from_entity_ref_target",
        type: "_get_from_entity_type_target",
    }
    _SET_DISPATCH: ClassVar[Mapping[_SetDispatchKey, str]] = {
        None: "_set_without_target",
        EntityVersion: "_set_with_entity_version_target",
        EntityRef: "_set_with_entity_ref_target",
        str: "_set_with_id_target",
    }

    def _resolve_get_dispatch_key[T: EntityBase](self, target: EntityRef[T] | EntityVersion[T] | type[T]) -> _GetDispatchKey:
        if isinstance(target, EntityVersion):
            return EntityVersion
        if isinstance(target, EntityRef):
            return EntityRef
        return type

    def _resolve_set_dispatch_key[T: EntityBase](
        self,
        target: EntityRef[T] | EntityVersion[T] | str | None,
    ) -> _SetDispatchKey:
        if target is None:
            return None
        if isinstance(target, EntityVersion):
            return EntityVersion
        if isinstance(target, EntityRef):
            return EntityRef
        return str

    def _get_entity_version[T: EntityBase](
        self,
        target: EntityRef[T] | EntityVersion[T] | type[T],
        id: str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]:
        dispatch_key = self._resolve_get_dispatch_key(target)
        handler_name = self._GET_DISPATCH[dispatch_key]
        entity_type, entity_id, resolved_version = getattr(self, handler_name)(target, id, version)
        return self._read_entity_version(entity_type, entity_id, resolved_version)

    def _set_entity_version[T: EntityBase](
        self,
        entity: T,
        target: EntityRef[T] | EntityVersion[T] | str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]:
        dispatch_key = self._resolve_set_dispatch_key(target)
        handler_name = self._SET_DISPATCH[dispatch_key]
        return getattr(self, handler_name)(entity, target, version)

    def _get_from_entity_version_target[T: EntityBase](
        self,
        target: EntityVersion[T],
        id: str | None,
        version: str | None,
    ) -> _ResolvedEntityTarget[T]:
        if id is not None or version is not None:
            raise TypeError("id and version cannot be passed with EntityVersion targets")
        return target.t, target.id, target.version

    def _get_from_entity_ref_target[T: EntityBase](
        self,
        target: EntityRef[T],
        id: str | None,
        version: str | None,
    ) -> _ResolvedEntityTarget[T]:
        if id is not None or version is not None:
            raise TypeError("id and version cannot be passed with EntityRef targets")
        return target.t, target.id, self.read_active_version(target.tag, target.id)

    def _get_from_entity_type_target[T: EntityBase](
        self,
        target: type[T],
        id: str | None,
        version: str | None,
    ) -> _ResolvedEntityTarget[T]:
        if id is None:
            raise TypeError("id is required when target is an entity type")
        resolved_version = version if version is not None else self.read_active_version(target.tag, id)
        return target, id, resolved_version

    def _set_without_target[T: EntityBase](
        self,
        entity: T,
        _: None,
        version: str | None,
    ) -> EntityVersion[T]:
        if version is not None:
            raise TypeError("version cannot be passed without a target")
        ref = EntityRef[T].of(type(entity))
        resolved_version = str(INITIAL_VERSION)
        self._write_entity(entity, ref.id, resolved_version)
        self.write_active_version(entity.tag, ref.id, resolved_version)
        return entity.ver(ref.id, resolved_version)

    def _set_with_entity_version_target[T: EntityBase](
        self,
        entity: T,
        target: EntityVersion[T],
        version: str | None,
    ) -> EntityVersion[T]:
        if version is not None:
            raise TypeError("version cannot be passed with EntityVersion targets")
        self._write_entity(entity, target.id, target.version)
        self._update_active_if_current(target.tag, target.id, target.version)
        return entity.ver(target.id, target.version)

    def _set_with_entity_ref_target[T: EntityBase](
        self,
        entity: T,
        target: EntityRef[T],
        version: str | None,
    ) -> EntityVersion[T]:
        if version is not None:
            raise TypeError("version cannot be passed with EntityRef targets")
        resolved_version = self._next_version(target.tag, target.id)
        self._write_entity(entity, target.id, resolved_version)
        self.write_active_version(target.tag, target.id, resolved_version)
        return entity.ver(target.id, resolved_version)

    def _set_with_id_target[T: EntityBase](
        self,
        entity: T,
        target: str,
        version: str | None,
    ) -> EntityVersion[T]:
        if version is not None:
            self._write_entity(entity, target, version)
            self._update_active_if_current(entity.tag, target, version)
            return entity.ver(target, version)

        resolved_version = self._next_version(entity.tag, target)
        self._write_entity(entity, target, resolved_version)
        self.write_active_version(entity.tag, target, resolved_version)
        return entity.ver(target, resolved_version)

    def _next_version(self, tag: str, id: str) -> str:
        versions = self.list_versions(tag, id)
        return str(versions[-1].bump_patch()) if versions else str(INITIAL_VERSION)

    def _update_active_if_current(self, tag: str, id: str, version: str) -> None:
        if self._active_exists(tag, id) and self.read_active_version(tag, id) == version:
            self.write_active_version(tag, id, version)

    def _read_entity_version[T: EntityBase](self, entity_type: type[T], id: str, version: str) -> EntityVersion[T]:
        entity = entity_type.model_validate_json(self._read_entity_text(entity_type, id, version))
        return entity.ver(id, version)

    def _read_entity_text[T: EntityBase](self, entity_type: type[T], id: str, version: str) -> str:
        raise NotImplementedError

    def _write_entity(self, entity: EntityBase, id: str, version: str) -> None:
        raise NotImplementedError

    def _active_exists(self, tag: str, id: str) -> bool:
        raise NotImplementedError

    def list_versions(self, tag: str, id: str) -> list[Version]: ...

    def read_active_version(self, tag: str, id: str) -> str: ...

    def write_active_version(self, tag: str, id: str, version: str) -> None: ...


class LocalStorage(FileStorage, _StorageDispatchMixin):

    @overload
    def get[T: EntityBase](self, target: EntityVersion[T]) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: EntityRef[T]) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: type[T], id: str) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: type[T], id: str, version: str) -> EntityVersion[T]: ...
    def get[T: EntityBase](
        self,
        target: EntityRef[T] | EntityVersion[T] | type[T],
        id: str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]:
        return self._get_entity_version(target, id, version)

    @overload
    def set[T: EntityBase](self, entity: T) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: EntityVersion[T]) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: EntityRef[T]) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: str) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: str, version: str) -> EntityVersion[T]: ...
    def set[T: EntityBase](
        self,
        entity: T,
        target: EntityRef[T] | EntityVersion[T] | str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]:
        return self._set_entity_version(entity, target, version)

    def _read_entity_text[T: EntityBase](self, entity_type: type[T], id: str, version: str) -> str:
        path = self.resolve_version_path(entity_type.tag, id, version)
        return path.read_text(encoding="utf-8")

    def _active_exists(self, tag: str, id: str) -> bool:
        return self.resolve_active_path(tag, id).exists()

    def _write_entity(self, entity: EntityBase, id: str, version: str) -> None:
        path = self.resolve_version_path(entity.tag, id, version)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(entity.model_dump_json(indent=4), encoding="utf-8")


class AzureBlobStorage(_StorageDispatchMixin, BaseModel):
    container: str = Field(
        default=...,
        description="Azure Blob Storage container name.",
        kw_only=False,
    )
    base: str = Field(
        default="",
        description="Optional blob name prefix for stored entities.",
    )
    connection_string: str | None = Field(
        default=None,
        description="Azure Storage connection string.",
        repr=False,
    )
    blob_service_client: BlobServiceClient | None = Field(
        default=None,
        description="Preconfigured Azure BlobServiceClient.",
        repr=False,
    )
    active: ClassVar[str] = "active.json"
    model_config = ConfigDict(arbitrary_types_allowed=True)
    _container_client: ContainerClient | None = PrivateAttr(default=None)

    @classmethod
    def from_config(cls, config: Config | str | Path) -> Self:
        loaded = config if isinstance(config, Config) else Config(config)
        storage = loaded.azure.storage
        base = loaded.maybe.azure.storage.base
        return cls(
            connection_string=str(storage.connection_string),
            container=str(storage.container),
            base=str(base),
        )

    @model_validator(mode="after")
    def validate_client_source(self) -> Self:
        has_connection_string = self.connection_string is not None
        has_blob_service_client = self.blob_service_client is not None
        if has_connection_string == has_blob_service_client:
            raise ValueError("Provide exactly one of connection_string or blob_service_client")
        return self

    @property
    def container_client(self) -> ContainerClient:
        if self._container_client is None:
            client = self.blob_service_client
            if client is None:
                if self.connection_string is None:
                    raise ValueError("connection_string is required when blob_service_client is not provided")
                client = BlobServiceClient.from_connection_string(self.connection_string)
            self._container_client = client.get_container_client(self.container)
        return self._container_client

    def resolve_entity_prefix(self, tag: str, id: str) -> str:
        path_parts = [self.base.strip("/"), tag, id]
        return "/".join(part for part in path_parts if part)

    def resolve_active_name(self, tag: str, id: str) -> str:
        return f"{self.resolve_entity_prefix(tag, id)}/{self.active}"

    def resolve_version_name(self, tag: str, id: str, version: str | Version) -> str:
        return f"{self.resolve_entity_prefix(tag, id)}/{version}"

    def read_active_version(self, tag: str, id: str) -> str:
        content = self._read_blob_text(self.resolve_active_name(tag, id))
        version = json.loads(content)
        if not isinstance(version, str):
            raise TypeError("active.json must contain a JSON string version")
        return version

    def write_active_version(self, tag: str, id: str, version: str) -> None:
        self._write_blob_text(self.resolve_active_name(tag, id), json.dumps(version))

    def list_versions(self, tag: str, id: str) -> list[Version]:
        versions: list[Version] = []
        prefix = f"{self.resolve_entity_prefix(tag, id)}/"
        for blob in self.container_client.list_blobs(name_starts_with=prefix):
            name = blob.name
            filename = name.removeprefix(prefix)
            if "/" in filename or filename == self.active:
                continue
            try:
                versions.append(Version.parse(filename))
            except ValueError:
                continue
        return sorted(versions)

    @overload
    def get[T: EntityBase](self, target: EntityVersion[T]) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: EntityRef[T]) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: type[T], id: str) -> EntityVersion[T]: ...
    @overload
    def get[T: EntityBase](self, target: type[T], id: str, version: str) -> EntityVersion[T]: ...
    def get[T: EntityBase](
        self,
        target: EntityRef[T] | EntityVersion[T] | type[T],
        id: str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]:
        return self._get_entity_version(target, id, version)

    @overload
    def set[T: EntityBase](self, entity: T) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: EntityVersion[T]) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: EntityRef[T]) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: str) -> EntityVersion[T]: ...
    @overload
    def set[T: EntityBase](self, entity: T, target: str, version: str) -> EntityVersion[T]: ...
    def set[T: EntityBase](
        self,
        entity: T,
        target: EntityRef[T] | EntityVersion[T] | str | None = None,
        version: str | None = None,
    ) -> EntityVersion[T]:
        return self._set_entity_version(entity, target, version)

    def _read_entity_text[T: EntityBase](self, entity_type: type[T], id: str, version: str) -> str:
        return self._read_blob_text(self.resolve_version_name(entity_type.tag, id, version))

    def _active_exists(self, tag: str, id: str) -> bool:
        return self._blob_exists(self.resolve_active_name(tag, id))

    def _write_entity(self, entity: EntityBase, id: str, version: str) -> None:
        self._write_blob_text(self.resolve_version_name(entity.tag, id, version), entity.model_dump_json(indent=4))

    def _read_blob_text(self, name: str) -> str:
        try:
            data = self.container_client.get_blob_client(name).download_blob(encoding="utf-8").readall()
        except ResourceNotFoundError as error:
            raise FileNotFoundError(name) from error
        return data

    def _write_blob_text(self, name: str, content: str) -> None:
        self.container_client.get_blob_client(name).upload_blob(content, overwrite=True)

    def _blob_exists(self, name: str) -> bool:
        try:
            return self.container_client.get_blob_client(name).exists()
        except ResourceNotFoundError:
            return False
