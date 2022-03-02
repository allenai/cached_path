import logging
import os
import tempfile
from pathlib import Path

from .common import PathOrStr

logger = logging.getLogger(__name__)


class CacheFile:
    """
    This is a context manager that makes robust caching easier.

    On `__enter__`, an IO handle to a temporarily file is returned, which can
    be treated as if it's the actual cache file.

    On `__exit__`, the temporarily file is renamed to the cache file. If anything
    goes wrong while writing to the temporary file, it will be removed.
    """

    def __init__(self, cache_filename: PathOrStr, mode: str = "w+b", suffix: str = ".tmp") -> None:
        self.cache_filename = Path(cache_filename)
        self.cache_directory = os.path.dirname(self.cache_filename)
        self.mode = mode
        self.temp_file = tempfile.NamedTemporaryFile(
            self.mode, dir=self.cache_directory, delete=False, suffix=suffix
        )

    def __enter__(self):
        return self.temp_file

    def __exit__(self, exc_type, exc_value, traceback):
        self.temp_file.close()
        if exc_value is None:
            # Success.
            logger.debug(
                "Renaming temp file %s to cache at %s", self.temp_file.name, self.cache_filename
            )
            # Rename the temp file to the actual cache filename.
            os.replace(self.temp_file.name, self.cache_filename)
            return True
        # Something went wrong, remove the temp file.
        logger.debug("removing temp file %s", self.temp_file.name)
        os.remove(self.temp_file.name)
        return False
