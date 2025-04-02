"""
HuggingFace Hub.

Unlike the other schemes, we don't implement a `SchemeClient` subclass here because
`huggingface_hub` handles the caching logic internally in essentially the same way.
"""

from pathlib import Path
from typing import Optional

import huggingface_hub as hf_hub
import requests  # type: ignore[import-untyped]
from huggingface_hub.utils import (
    EntryNotFoundError,
    RepositoryNotFoundError,
    RevisionNotFoundError,
)

from ..common import PathOrStr
from ..version import VERSION


def hf_hub_download(
    model_identifier: str, filename: Optional[str], cache_dir: Optional[PathOrStr] = None
) -> Path:
    revision: Optional[str]
    if "@" in model_identifier:
        repo_id = model_identifier.split("@")[0]
        revision = model_identifier.split("@")[1]
    else:
        repo_id = model_identifier
        revision = None

    if filename is not None:
        return Path(
            hf_hub.hf_hub_download(
                repo_id=repo_id,
                filename=filename,
                revision=revision,
                library_name="cached_path",
                library_version=VERSION,
                cache_dir=cache_dir,
            )
        )
    else:
        return Path(hf_hub.snapshot_download(repo_id, revision=revision, cache_dir=cache_dir))


def hf_get_from_cache(url: str, cache_dir: Optional[PathOrStr] = None) -> Path:
    if cache_dir is not None:
        cache_dir = Path(cache_dir).expanduser()
        cache_dir.mkdir(parents=True, exist_ok=True)

    # Remove the 'hf://' prefix
    identifier = url[5:]

    if identifier.count("/") > 1:
        filename = "/".join(identifier.split("/")[2:])
        model_identifier = "/".join(identifier.split("/")[:2])
        return hf_hub_download(model_identifier, filename, cache_dir)
    elif identifier.count("/") == 1:
        # 'hf://' URLs like 'hf://xxxx/yyyy' are potentially ambiguous,
        # because this could refer to either:
        #  1. the file 'yyyy' in the 'xxxx' repository, or
        #  2. the repo 'yyyy' under the user/org name 'xxxx'.
        # We default to (1), but if we get a 404 error or 401 error then we try (2)
        try:
            model_identifier, filename = identifier.split("/")
            return hf_hub_download(model_identifier, filename, cache_dir)
        except (RepositoryNotFoundError, RevisionNotFoundError, EntryNotFoundError):
            return hf_hub_download(identifier, None, cache_dir)
        except requests.exceptions.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in {401, 404}:
                return hf_hub_download(identifier, None, cache_dir)
            else:
                raise
        except ValueError:
            return hf_hub_download(identifier, None, cache_dir)
    else:
        return hf_hub_download(identifier, None, cache_dir)
