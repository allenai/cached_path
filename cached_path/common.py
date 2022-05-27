import os
from os import PathLike
from pathlib import Path
from typing import Tuple, Union
from urllib.parse import urlparse

PathOrStr = Union[str, PathLike]

CACHE_DIRECTORY: PathOrStr = Path(
    os.getenv("CACHED_PATH_CACHE_ROOT", Path.home() / ".cache" / "cached_path")
)
"""
The default global cache directory.
"""


def _split_cloud_path(url: str, provider: str) -> Tuple[str, str]:
    """Split a full s3 path into the bucket name and path."""
    parsed = urlparse(url)
    if not parsed.netloc or not parsed.path:
        raise ValueError("bad {} path {}".format(provider, url))
    bucket_name = parsed.netloc
    provider_path = parsed.path
    # Remove '/' at beginning of path.
    if provider_path.startswith("/"):
        provider_path = provider_path[1:]
    return bucket_name, provider_path


def set_cache_dir(cache_dir: PathOrStr) -> None:
    """
    Set the global default cache directory.
    """
    global CACHE_DIRECTORY
    CACHE_DIRECTORY = Path(cache_dir)


def get_cache_dir() -> Path:
    """
    Get the global default cache directory.
    """
    return Path(CACHE_DIRECTORY)
