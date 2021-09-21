import json
import os
import pathlib

import pytest

from cached_path.util import resource_to_filename, filename_to_url
from cached_path.testing import BaseTestClass


class TestUtils(BaseTestClass):
    def test_resource_to_filename(self):
        for url in [
            "http://allenai.org",
            "http://allennlp.org",
            "https://www.google.com",
            "http://pytorch.org",
            "https://allennlp.s3.amazonaws.com" + "/long" * 20 + "/url",
        ]:
            filename = resource_to_filename(url)
            assert "http" not in filename
            with pytest.raises(FileNotFoundError):
                filename_to_url(filename, cache_dir=self.TEST_DIR)
            pathlib.Path(os.path.join(self.TEST_DIR, filename)).touch()
            with pytest.raises(FileNotFoundError):
                filename_to_url(filename, cache_dir=self.TEST_DIR)
            json.dump(
                {"url": url, "etag": None},
                open(os.path.join(self.TEST_DIR, filename + ".json"), "w"),
            )
            back_to_url, etag = filename_to_url(filename, cache_dir=self.TEST_DIR)
            assert back_to_url == url
            assert etag is None

    def test_resource_to_filename_with_etags(self):
        for url in [
            "http://allenai.org",
            "http://allennlp.org",
            "https://www.google.com",
            "http://pytorch.org",
        ]:
            filename = resource_to_filename(url, etag="mytag")
            assert "http" not in filename
            pathlib.Path(os.path.join(self.TEST_DIR, filename)).touch()
            json.dump(
                {"url": url, "etag": "mytag"},
                open(os.path.join(self.TEST_DIR, filename + ".json"), "w"),
            )
            back_to_url, etag = filename_to_url(filename, cache_dir=self.TEST_DIR)
            assert back_to_url == url
            assert etag == "mytag"
        baseurl = "http://allenai.org/"
        assert resource_to_filename(baseurl + "1") != resource_to_filename(baseurl, etag="1")

    def test_resource_to_filename_with_etags_eliminates_quotes(self):
        for url in [
            "http://allenai.org",
            "http://allennlp.org",
            "https://www.google.com",
            "http://pytorch.org",
        ]:
            filename = resource_to_filename(url, etag='"mytag"')
            assert "http" not in filename
            pathlib.Path(os.path.join(self.TEST_DIR, filename)).touch()
            json.dump(
                {"url": url, "etag": "mytag"},
                open(os.path.join(self.TEST_DIR, filename + ".json"), "w"),
            )
            back_to_url, etag = filename_to_url(filename, cache_dir=self.TEST_DIR)
            assert back_to_url == url
            assert etag == "mytag"
