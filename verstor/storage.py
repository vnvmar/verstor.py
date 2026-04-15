
import json
from pathlib import Path
from typing import ClassVar, Protocol, Self, runtime_checkable

from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient, ContainerClient
from conf import Config
from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, model_validator
from semver import Version

from verstor.types import INITIAL_VERSION, EntityBase, EntityRef, EntityVersion


@runtime_checkable
class Storage(Protocol):
    def get[T: EntityBase](self, target: EntityRef[T] | EntityVersion[T]) -> EntityVersion[T]: ...
    def set[T: EntityBase](self, entity: T, target: EntityRef[T] | EntityVersion[T] | None = None) -> EntityVersion[T]: ...


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

class LocalStorage(FileStorage):

    def get[T: EntityBase](self, target: EntityRef[T] | EntityVersion[T]) -> EntityVersion[T]:
        if isinstance(target, EntityVersion):
            version = target.version
        else:
            version = self.read_active_version(target.tag, target.id)

        path = self.resolve_version_path(target.tag, target.id, version)
        entity = target.t.model_validate_json(path.read_text(encoding="utf-8"))
        return entity.ver(target.id, version)

    def set[T: EntityBase](self, entity: T, target: EntityRef[T] | EntityVersion[T] | None = None) -> EntityVersion[T]:
        if target is None:
            ref = EntityRef[T].of(type(entity))
            version = str(INITIAL_VERSION)
            self._write_entity(entity, ref.id, version)
            self.write_active_version(entity.tag, ref.id, version)
            return entity.ver(ref.id, version)

        if isinstance(target, EntityVersion):
            self._write_entity(entity, target.id, target.version)
            active_path = self.resolve_active_path(target.tag, target.id)
            if active_path.exists() and self.read_active_version(target.tag, target.id) == target.version:
                self.write_active_version(target.tag, target.id, target.version)
            return entity.ver(target.id, target.version)

        versions = self.list_versions(target.tag, target.id)
        version = str(versions[-1].bump_patch()) if versions else str(INITIAL_VERSION)
        self._write_entity(entity, target.id, version)
        self.write_active_version(target.tag, target.id, version)
        return entity.ver(target.id, version)

    def _write_entity(self, entity: EntityBase, id: str, version: str) -> None:
        path = self.resolve_version_path(entity.tag, id, version)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(entity.model_dump_json(indent=4), encoding="utf-8")


class AzureBlobStorage(BaseModel):
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

    def get[T: EntityBase](self, target: EntityRef[T] | EntityVersion[T]) -> EntityVersion[T]:
        if isinstance(target, EntityVersion):
            version = target.version
        else:
            version = self.read_active_version(target.tag, target.id)

        content = self._read_blob_text(self.resolve_version_name(target.tag, target.id, version))
        entity = target.t.model_validate_json(content)
        return entity.ver(target.id, version)

    def set[T: EntityBase](self, entity: T, target: EntityRef[T] | EntityVersion[T] | None = None) -> EntityVersion[T]:
        if target is None:
            ref = EntityRef[T].of(type(entity))
            version = str(INITIAL_VERSION)
            self._write_entity(entity, ref.id, version)
            self.write_active_version(entity.tag, ref.id, version)
            return entity.ver(ref.id, version)

        if isinstance(target, EntityVersion):
            self._write_entity(entity, target.id, target.version)
            if self._blob_exists(self.resolve_active_name(target.tag, target.id)):
                if self.read_active_version(target.tag, target.id) == target.version:
                    self.write_active_version(target.tag, target.id, target.version)
            return entity.ver(target.id, target.version)

        versions = self.list_versions(target.tag, target.id)
        version = str(versions[-1].bump_patch()) if versions else str(INITIAL_VERSION)
        self._write_entity(entity, target.id, version)
        self.write_active_version(target.tag, target.id, version)
        return entity.ver(target.id, version)

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
