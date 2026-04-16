# Verstor

Verstor is a small typed version store for Pydantic models. It stores entities
by type tag and id, keeps an `active.json` pointer for the current version, and
lets callers read either the active version or a specific historical version.

The package currently includes:

- `EntityBase`, `EntityRef`, and `EntityVersion` for typed references.
- `LocalStorage` for filesystem-backed storage.
- `AzureBlobStorage` for Azure Blob Storage-backed storage.
- `AzureBlobStorage.from_config(...)` for config-backed Azure construction.
- Semver patch bumps when updating an existing entity by reference.

## Installation

The project is currently intended to be used from Git:

```bash
git clone --recurse-submodules git@github.com:vnvmar/verstor.py.git
cd verstor.py
uv sync
```

`conf-py` is vendored as a Git submodule under `vendor/conf-py`, and the root
project depends on the built local wheel in `libs/`.

## Basic Usage

Define stored models by subclassing `EntityBase` and giving each model type a
stable storage tag:

```python
from pathlib import Path
from typing import ClassVar

from verstor import EntityBase, LocalStorage


class Name(EntityBase):
    tag: ClassVar[str] = "names"
    name: str


storage = LocalStorage(base=Path("versions"))

saved = storage.set(Name(name="first"))
print(saved.id)       # generated entity id
print(saved.version)  # "1.0.0"

loaded = storage.get(Name, saved.id)
assert loaded.entity == Name(name="first")
```

Calling `set` with an existing id writes the next patch version and updates the
active pointer:

```python
updated = storage.set(Name(name="second"), saved.id)

assert updated.id == saved.id
assert updated.version == "1.0.1"
assert storage.get(Name, saved.id).entity == Name(name="second")
```

Pass the model type, id, and version to read a specific historical version.
Pass an id and version to `set` to overwrite a specific version without changing
which version is active:

```python
assert storage.get(Name, saved.id, "1.0.0").entity == Name(name="first")

storage.set(Name(name="rewritten first"), saved.id, "1.0.0")
assert storage.get(Name, saved.id, "1.0.0").entity == Name(name="rewritten first")
assert storage.get(Name, saved.id).version == "1.0.1"
```

## Patching Entities

`EntityBase.patch(...)` returns a copied entity with only the provided fields
changed. Updates are validated by Pydantic:

```python
renamed = loaded.entity.patch(name="third")

assert renamed == Name(name="third")
assert loaded.entity == Name(name="first")
```

For strict field-name and field-type checking, define a local typed forwarding
method on the entity:

```python
from types import EllipsisType
from typing import Callable, Never, Self, cast, override


class Name(EntityBase):
    tag: ClassVar[str] = "names"
    name: str

    @override
    def patch(self, *, name: str | EllipsisType = ..., **changes: Never) -> Self:
        updates: dict[str, object] = {}
        if name is not ...:
            updates["name"] = name
        updates.update(cast(dict[str, object], changes))
        patch = cast(Callable[..., Self], super().patch)
        return patch(**updates)
```

The forwarding method lets basedpyright check calls like `entity.patch(name="third")`
against each concrete entity class while keeping the runtime behavior on
`EntityBase`.

## Storage Layout

`LocalStorage` writes JSON files under:

```text
{base}/{tag}/{id}/active.json
{base}/{tag}/{id}/{version}
```

`active.json` contains the active version as a JSON string. Version files contain
the serialized Pydantic model.

`AzureBlobStorage` uses the same logical layout as blob names, optionally
prefixed by its `base` value:

```text
{base}/{tag}/{id}/active.json
{base}/{tag}/{id}/{version}
```

## Azure Blob Storage

Construct Azure storage directly from user-provided values:

```python
from verstor import AzureBlobStorage


storage = AzureBlobStorage(
    connection_string="UseDevelopmentStorage=true",
    container="verstor",
    base="versions",
)
```

Or pass an already configured Azure SDK client:

```python
from azure.storage.blob import BlobServiceClient
from verstor import AzureBlobStorage


client = BlobServiceClient.from_connection_string("UseDevelopmentStorage=true")

storage = AzureBlobStorage(
    blob_service_client=client,
    container="verstor",
    base="versions",
)
```

The Azure container must already exist. `AzureBlobStorage` does not create it.

## Azure Configuration

`AzureBlobStorage.from_config(...)` accepts either a `conf.Config` instance or a
path to a supported config file. Use nested `azure.storage` settings:

```json
{
  "azure": {
    "storage": {
      "connection_string": "UseDevelopmentStorage=true",
      "container": "verstor",
      "base": "versions"
    }
  }
}
```

```python
from verstor import AzureBlobStorage


storage = AzureBlobStorage.from_config("config.json")
```

For `.env` files, `conf-py` supports scoped flat keys:

```dotenv
AZURE_STORAGE_CONNECTION_STRING=UseDevelopmentStorage=true
AZURE_STORAGE_CONTAINER=verstor
AZURE_STORAGE_BASE=versions
```

## Development

Run the test suite and type checker:

```bash
uv run pytest
uv run basedpyright
```

Run the targeted lint check used for this package:

```bash
uv run ruff check verstor/storage.py verstor/__init__.py tests/test_storage.py pyproject.toml
```

The full repository also contains the vendored `conf-py` submodule. A repo-wide
ruff run may report style choices from that vendored test suite.
