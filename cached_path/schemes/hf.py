"""
HuggingFace Hub.

Unlike the other schemes, we don't implement a `SchemeClient` subclass here because
`huggingface_hub` handles the caching logic internally in essentially the same way.
"""

import os
from typing import Optional

import huggingface_hub as hf_hub
import requests

from cached_path.common import PathOrStr
from cached_path.file_lock import FileLock
from cached_path.meta import Meta
from cached_path.version import VERSION


def hf_hub_download(
    url: str, model_identifier: str, filename: Optional[str], cache_dir: PathOrStr
) -> str:
    revision: Optional[str]
    if "@" in model_identifier:
        repo_id = model_identifier.split("@")[0]
        revision = model_identifier.split("@")[1]
    else:
        repo_id = model_identifier
        revision = None

    if filename is not None:
        hub_url = hf_hub.hf_hub_url(repo_id=repo_id, filename=filename, revision=revision)
        cache_path = str(
            hf_hub.cached_download(
                url=hub_url,
                library_name="cached_path",
                library_version=VERSION,
                cache_dir=cache_dir,
            )
        )
        # HF writes it's own meta '.json' file which uses the same format we used to use and still
        # support, but is missing some fields that we like to have.
        # So we overwrite it when it we can.
        with FileLock(cache_path + ".lock", read_only_ok=True):
            meta = Meta.from_path(cache_path + ".json")
            # The file HF writes will have 'resource' set to the 'http' URL corresponding to the 'hf://' URL,
            # but we want 'resource' to be the original 'hf://' URL.
            if meta.resource != url:
                meta.resource = url
                meta.to_file()
    else:
        cache_path = str(hf_hub.snapshot_download(repo_id, revision=revision, cache_dir=cache_dir))
        # Need to write the meta file for snapshot downloads if it doesn't exist.
        with FileLock(cache_path + ".lock", read_only_ok=True):
            if not os.path.exists(cache_path + ".json"):
                meta = Meta.new(
                    url,
                    cache_path,
                    extraction_dir=True,
                )
                meta.to_file()
    return cache_path


def hf_get_from_cache(url: str, cache_dir: PathOrStr) -> str:
    # Remove the 'hf://' prefix
    identifier = url[5:]

    if identifier.count("/") > 1:
        filename = "/".join(identifier.split("/")[2:])
        model_identifier = "/".join(identifier.split("/")[:2])
        return hf_hub_download(url, model_identifier, filename, cache_dir)
    elif identifier.count("/") == 1:
        # 'hf://' URLs like 'hf://xxxx/yyyy' are potentially ambiguous,
        # because this could refer to either:
        #  1. the file 'yyyy' in the 'xxxx' repository, or
        #  2. the repo 'yyyy' under the user/org name 'xxxx'.
        # We default to (1), but if we get a 404 error then we try (2).
        try:
            model_identifier, filename = identifier.split("/")
            return hf_hub_download(url, model_identifier, filename, cache_dir)
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 404:
                return hf_hub_download(url, identifier, None, cache_dir)
            raise
    else:
        return hf_hub_download(url, identifier, None, cache_dir)
