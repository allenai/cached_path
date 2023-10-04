"""
The idea behind **cached-path** is to provide a unified, simple, extendable interface for accessing
both local and remote files.
This can be used behind other APIs that need to access files agnostic to where they are located.

For remote files, **cached-path** supports several different schemes out-of-the-box in addition
``http`` and ``https``, including ``s3`` for AWS S3, ``gs`` for Google Cloud Storage,
and ``hf`` for HuggingFace Hub. See :func:`cached_path.cached_path()` for more details.

You can also extend **cached-path** to support other schemes with :func:`add_scheme_client()`.
"""

from ._cached_path import cached_path
from .bytes_range import get_bytes_range
from .common import get_cache_dir, set_cache_dir
from .progress import get_download_progress
from .schemes import SchemeClient, add_scheme_client
from .util import (
    check_tarfile,
    filename_to_url,
    find_latest_cached,
    is_url_or_existing_file,
    resource_to_filename,
)

__all__ = [
    "cached_path",
    "get_bytes_range",
    "get_cache_dir",
    "set_cache_dir",
    "get_download_progress",
    "SchemeClient",
    "add_scheme_client",
    "check_tarfile",
    "filename_to_url",
    "find_latest_cached",
    "is_url_or_existing_file",
    "resource_to_filename",
]
