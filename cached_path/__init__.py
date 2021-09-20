"""
The idea behind cached-path is to provide a unified, simple interface for accessing both local and remote files.
This can be used behind other APIs that need to access files agnostic to where they are located.
"""

from cached_path._cached_path import cached_path
from cached_path.common import CACHE_DIRECTORY
from cached_path.util import (
    resource_to_filename,
    filename_to_url,
    find_latest_cached,
    check_tarfile,
)
