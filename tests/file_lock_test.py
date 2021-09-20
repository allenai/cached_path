import os

from filelock import Timeout
import pytest

from cached_path.file_lock import FileLock
from cached_path.testing import BaseTestClass


class TestFileLock(BaseTestClass):
    def setup_method(self):
        super().setup_method()

        # Set up a regular lock and a read-only lock.
        open(self.TEST_DIR / "lock", "a").close()
        open(self.TEST_DIR / "read_only_lock", "a").close()
        os.chmod(self.TEST_DIR / "read_only_lock", 0o555)

        # Also set up a read-only directory.
        os.mkdir(self.TEST_DIR / "read_only_dir", 0o555)

    def test_locking(self):
        with FileLock(self.TEST_DIR / "lock"):
            # Trying to acquire the lock again should fail.
            with pytest.raises(Timeout):
                with FileLock(self.TEST_DIR / "lock", timeout=0.1):
                    pass

        # Trying to acquire a lock when lacking write permissions on the file should fail.
        with pytest.raises(PermissionError):
            with FileLock(self.TEST_DIR / "read_only_lock"):
                pass

        # But this should only issue a warning if we set the `read_only_ok` flag to `True`.
        with pytest.warns(UserWarning, match="Lacking permissions"):
            with FileLock(self.TEST_DIR / "read_only_lock", read_only_ok=True):
                pass

        # However this should always fail when we lack write permissions and the file lock
        # doesn't exist yet.
        with pytest.raises(PermissionError):
            with FileLock(self.TEST_DIR / "read_only_dir" / "lock", read_only_ok=True):
                pass
