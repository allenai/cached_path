import os

import pytest

from cached_path.cache_file import CacheFile
from cached_path.testing import BaseTestClass


class TestCacheFile(BaseTestClass):
    def test_temp_file_removed_on_error(self):
        cache_filename = self.TEST_DIR / "cache_file"
        with pytest.raises(IOError, match="I made this up"):
            with CacheFile(cache_filename) as handle:
                raise IOError("I made this up")
        assert not os.path.exists(handle.name)
        assert not os.path.exists(cache_filename)
