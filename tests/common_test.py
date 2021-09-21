from cached_path import common
from cached_path.testing import BaseTestClass


class TestSetCacheDir(BaseTestClass):
    def setup_method(self):
        super().setup_method()
        self.initial_value = common.CACHE_DIRECTORY

    def test_toggle_ffl(self):
        common.set_cache_dir(self.TEST_DIR / "foo")
        assert common.get_cache_dir() == self.TEST_DIR / "foo"

    def teardown_method(self):
        common.set_cache_dir(self.initial_value)


class TestSetFileFriendlyLogging:
    def setup_method(self):
        self.initial_value = common.FILE_FRIENDLY_LOGGING

    def test_toggle_ffl(self):
        common.file_friendly_logging(True)
        assert common.FILE_FRIENDLY_LOGGING
        common.file_friendly_logging(False)
        assert not common.FILE_FRIENDLY_LOGGING

    def teardown_method(self):
        common.file_friendly_logging(self.initial_value)
