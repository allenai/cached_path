"""
The idea behind **cached-path** is to provide a unified, simple, extendable interface for accessing
both local and remote files.
This can be used behind other APIs that need to access files agnostic to where they are located.

For remote files, **cached-path** supports several different schemes out-of-the-box in addition
``http`` and ``https``, including ``s3`` for AWS S3, ``gs`` for Google Cloud Storage,
and ``hf`` for HuggingFace Hub. See :func:`cached_path.cached_path()` for more details.

You can also extend **cached-path** to support other schemes with :func:`add_scheme_client()`.
"""

from cached_path._cached_path import cached_path
from cached_path.common import set_cache_dir, get_cache_dir, file_friendly_logging
from cached_path.schemes import add_scheme_client, SchemeClient
from cached_path.util import (
    resource_to_filename,
    filename_to_url,
    find_latest_cached,
    check_tarfile,
)
