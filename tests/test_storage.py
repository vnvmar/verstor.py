from __future__ import annotations

import json
import os
from pathlib import Path
from typing import ClassVar
from uuid import uuid4

import pytest
from azure.core.exceptions import ResourceNotFoundError
from azure.storage.blob import BlobServiceClient
from conf import Config

from verstor.storage import AzureBlobStorage, LocalStorage
from verstor.types import EntityBase, ref

TAG="names"

class NameModel(EntityBase):
    tag: ClassVar[str] = TAG
    name: str


def test_set_without_target_creates_initial_version_and_active_pointer(tmp_path: Path) -> None:
    storage = LocalStorage(base=tmp_path)

    draft = NameModel(name="draft")
    saved = storage.set(draft)

    assert saved.version == "1.0.0"
    assert json.loads((tmp_path / TAG / saved.id / "active.json").read_text(encoding="utf-8")) == "1.0.0"
    assert storage.get(saved.ref(saved.id)).entity == draft


def test_get_with_entity_version_bypasses_active_pointer(tmp_path: Path) -> None:
    storage = LocalStorage(base=tmp_path)
    created = storage.set(NameModel(name="v1"))
    storage.set(NameModel(name="v2"), created.ref())

    loaded = storage.get(NameModel(name="placeholder").ver(created.id, "1.0.0"))

    assert loaded.version == "1.0.0"
    assert loaded.entity == NameModel(name="v1")


def test_set_with_entity_ref_creates_next_patch_and_updates_active(tmp_path: Path) -> None:
    storage = LocalStorage(base=tmp_path)
    created = storage.set(NameModel(name="v1"))

    updated = storage.set(NameModel(name="v2"), created.ref())

    assert updated.id == created.id
    assert updated.version == "1.0.1"
    assert json.loads((tmp_path / TAG / created.id / "active.json").read_text(encoding="utf-8")) == "1.0.1"
    assert storage.get(created.ref()).entity == NameModel(name="v2")


def test_set_with_entity_version_overwrites_specific_version_without_changing_active(tmp_path: Path) -> None:
    storage = LocalStorage(base=tmp_path)
    created = storage.set(NameModel(name="v1"))
    active = storage.set(NameModel(name="v2"), created.ref())
    placeholder = NameModel(name="placeholder").ver(created.id, "1.0.0")

    overwritten = storage.set(
        NameModel(name="rewritten-v1"),
        placeholder,
    )

    assert overwritten.version == "1.0.0"
    assert storage.get(placeholder).entity == NameModel(name="rewritten-v1")
    assert storage.get(created.ref()).version == active.version
    assert storage.get(created.ref()).entity == NameModel(name="v2")


def test_get_entity_ref_without_active_json_raises(tmp_path: Path) -> None:
    storage = LocalStorage(base=tmp_path)
    entity_dir = tmp_path / TAG / "instance-1"
    entity_dir.mkdir(parents=True)
    (entity_dir / "1.0.0").write_text(NameModel(name="draft").model_dump_json(), encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        storage.get(ref(NameModel, "instance-1"))


def test_get_entity_ref_with_missing_active_version_file_raises(tmp_path: Path) -> None:
    storage = LocalStorage(base=tmp_path)
    entity_dir = tmp_path / TAG / "instance-1"
    entity_dir.mkdir(parents=True)
    (entity_dir / "active.json").write_text(json.dumps("1.0.0"), encoding="utf-8")

    with pytest.raises(FileNotFoundError):
        storage.get(ref(NameModel, "instance-1"))


def write_config(path: Path, content: object) -> Path:
    path.write_text(json.dumps(content), encoding="utf-8")
    return path


def test_azure_blob_storage_from_config_path_loads_storage_settings(tmp_path: Path) -> None:
    path = write_config(
        tmp_path / "config.json",
        {
            "azure": {
                "storage": {
                    "connection_string": "UseDevelopmentStorage=true",
                    "container": "models",
                    "base": "prefix",
                }
            }
        },
    )

    storage = AzureBlobStorage.from_config(path)

    assert storage.connection_string == "UseDevelopmentStorage=true"
    assert storage.container == "models"
    assert storage.base == "prefix"
    assert storage.blob_service_client is None


def test_azure_blob_storage_from_config_instance_loads_storage_settings(tmp_path: Path) -> None:
    path = write_config(
        tmp_path / "config.json",
        {
            "azure": {
                "storage": {
                    "connection_string": "UseDevelopmentStorage=true",
                    "container": "models",
                }
            }
        },
    )

    storage = AzureBlobStorage.from_config(Config(path))

    assert storage.connection_string == "UseDevelopmentStorage=true"
    assert storage.container == "models"
    assert storage.base == ""


def test_azure_blob_storage_from_config_requires_connection_string(tmp_path: Path) -> None:
    path = write_config(tmp_path / "config.json", {"azure": {"storage": {"container": "models"}}})

    with pytest.raises(EnvironmentError, match="CONNECTION_STRING"):
        AzureBlobStorage.from_config(path)


def test_azure_blob_storage_from_config_requires_container(tmp_path: Path) -> None:
    path = write_config(
        tmp_path / "config.json",
        {"azure": {"storage": {"connection_string": "UseDevelopmentStorage=true"}}},
    )

    with pytest.raises(EnvironmentError, match="CONTAINER"):
        AzureBlobStorage.from_config(path)


def azure_storage_from_env() -> AzureBlobStorage:
    config_path = os.environ.get("VERSTOR_AZURE_STORAGE_CONFIG")
    if config_path is None:
        pytest.skip("VERSTOR_AZURE_STORAGE_CONFIG is not set")

    storage = AzureBlobStorage.from_config(config_path)
    if storage.connection_string is None:
        pytest.skip("Azure storage config did not provide a connection string")

    client = BlobServiceClient.from_connection_string(storage.connection_string)
    try:
        client.get_container_client(storage.container).get_container_properties()
    except ResourceNotFoundError:
        pytest.skip(f"Azure container {storage.container!r} does not exist")

    storage.base = f"{storage.base.strip('/')}/pytest/{uuid4()}".strip("/")
    return storage



def test_azure_set_without_target_creates_initial_version_and_active_pointer() -> None:
    storage = azure_storage_from_env()

    draft = NameModel(name="draft")
    saved = storage.set(draft)

    assert saved.version == "1.0.0"
    assert storage.read_active_version(TAG, saved.id) == "1.0.0"
    assert storage.get(saved.ref(saved.id)).entity == draft


def test_azure_get_with_entity_version_bypasses_active_pointer() -> None:
    storage = azure_storage_from_env()
    created = storage.set(NameModel(name="v1"))
    storage.set(NameModel(name="v2"), created.ref())

    loaded = storage.get(NameModel(name="placeholder").ver(created.id, "1.0.0"))

    assert loaded.version == "1.0.0"
    assert loaded.entity == NameModel(name="v1")


def test_azure_set_with_entity_ref_creates_next_patch_and_updates_active() -> None:
    storage = azure_storage_from_env()
    created = storage.set(NameModel(name="v1"))

    updated = storage.set(NameModel(name="v2"), created.ref())

    assert updated.id == created.id
    assert updated.version == "1.0.1"
    assert storage.read_active_version(TAG, created.id) == "1.0.1"
    assert storage.get(created.ref()).entity == NameModel(name="v2")


def test_azure_set_with_entity_version_overwrites_specific_version_without_changing_active() -> None:
    storage = azure_storage_from_env()
    created = storage.set(NameModel(name="v1"))
    active = storage.set(NameModel(name="v2"), created.ref())
    placeholder = NameModel(name="placeholder").ver(created.id, "1.0.0")

    overwritten = storage.set(
        NameModel(name="rewritten-v1"),
        placeholder,
    )

    assert overwritten.version == "1.0.0"
    assert storage.get(placeholder).entity == NameModel(name="rewritten-v1")
    assert storage.get(created.ref()).version == active.version
    assert storage.get(created.ref()).entity == NameModel(name="v2")


def test_azure_get_entity_ref_without_active_json_raises() -> None:
    storage = azure_storage_from_env()
    entity = NameModel(name="draft")
    storage.container_client.get_blob_client(storage.resolve_version_name(TAG, "instance-1", "1.0.0")).upload_blob(
        entity.model_dump_json(),
        overwrite=True,
    )

    with pytest.raises(FileNotFoundError):
        storage.get(ref(NameModel, "instance-1"))


def test_azure_get_entity_ref_with_missing_active_version_file_raises() -> None:
    storage = azure_storage_from_env()
    storage.write_active_version(TAG, "instance-1", "1.0.0")

    with pytest.raises(FileNotFoundError):
        storage.get(ref(NameModel, "instance-1"))
