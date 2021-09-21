import logging
import os
import pathlib
import shutil
import tempfile

from cached_path.common import set_cache_dir, get_cache_dir

TEST_DIR = tempfile.mkdtemp(prefix="cached_path_tests")


class BaseTestClass:
    """
    A custom testing class that disables some of the more verbose
    logging and that creates and destroys a temp directory as a test fixture.
    """

    PROJECT_ROOT = (pathlib.Path(__file__).parent / "..").resolve()
    MODULE_ROOT = PROJECT_ROOT / "cached_path"
    TOOLS_ROOT = MODULE_ROOT / "tools"
    TESTS_ROOT = PROJECT_ROOT / "tests"
    FIXTURES_ROOT = PROJECT_ROOT / "test_fixtures"

    def setup_method(self):
        logging.basicConfig(
            format="%(asctime)s - %(levelname)s - %(name)s - %(message)s", level=logging.DEBUG
        )
        # Disabling some of the more verbose logging statements that typically aren't very helpful
        # in tests.
        logging.getLogger("urllib3.connectionpool").disabled = True

        self.TEST_DIR = pathlib.Path(TEST_DIR)

        os.makedirs(self.TEST_DIR, exist_ok=True)

        self._initial_cache_dir = get_cache_dir()
        set_cache_dir(self.TEST_DIR)

    def teardown_method(self):
        set_cache_dir(self._initial_cache_dir)
        shutil.rmtree(self.TEST_DIR)
