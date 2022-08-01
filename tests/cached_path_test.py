import shutil
import tempfile
import time
from collections import Counter
from pathlib import Path

import pytest
import responses
from flaky import flaky
from requests.exceptions import ConnectionError, HTTPError

from cached_path._cached_path import cached_path, get_from_cache
from cached_path.meta import Meta
from cached_path.schemes.http import HttpClient, RecoverableServerError
from cached_path.testing import BaseTestClass
from cached_path.util import _lock_file_path, _meta_file_path, resource_to_filename


class TestCachedPathHttp(BaseTestClass):
    @staticmethod
    def set_up_glove(url: str, byt: bytes, change_etag_every: int = 1000):
        # Mock response for the datastore url that returns glove vectors
        responses.add(
            responses.GET,
            url,
            body=byt,
            status=200,
            content_type="application/gzip",
            headers={"Content-Length": str(len(byt))},
        )

        etags_left = change_etag_every
        etag = "0"

        def head_callback(_):
            """
            Writing this as a callback allows different responses to different HEAD requests.
            In our case, we're going to change the ETag header every `change_etag_every`
            requests, which will allow us to simulate having a new version of the file.
            """
            nonlocal etags_left, etag
            headers = {"ETag": etag}
            # countdown and change ETag
            etags_left -= 1
            if etags_left <= 0:
                etags_left = change_etag_every
                etag = str(int(etag) + 1)
            return (200, headers, "")

        responses.add_callback(responses.HEAD, url, callback=head_callback)

    def setup_method(self):
        super().setup_method()
        self.glove_file = self.FIXTURES_ROOT / "embeddings/glove.6B.100d.sample.txt.gz"
        with open(self.glove_file, "rb") as glove:
            self.glove_bytes = glove.read()

    def test_offline_mode_fallback(self, monkeypatch):
        # Ensures `cached_path` just returns the path to the latest cached version
        # of the resource when there's no internet connection, or another recoverable error
        # occurs.

        # First we mock the `get_etag` method so that it raises a `ConnectionError`,
        # like it would if there was no internet connection.
        def mocked_http_etag(self):
            raise ConnectionError

        monkeypatch.setattr(HttpClient, "get_etag", mocked_http_etag)

        url = "https://github.com/allenai/allennlp/blob/master/some-fake-resource"

        # We'll create two cached versions of this fake resource using two different etags.
        etags = ['W/"3e5885bfcbf4c47bc4ee9e2f6e5ea916"', 'W/"3e5885bfcbf4c47bc4ee9e2f6e5ea918"']
        filenames = [self.TEST_DIR / resource_to_filename(url, etag) for etag in etags]
        for filename, etag in zip(filenames, etags):
            meta = Meta(
                resource=url,
                cached_path=str(filename),
                creation_time=time.time(),
                etag=etag,
                size=2341,
            )
            meta.to_file()
            with open(filename, "w") as f:
                f.write("some random data")
            # os.path.getmtime is only accurate to the second.
            time.sleep(1.1)

        # Should know to ignore lock files and extraction directories.
        with open(_lock_file_path(filenames[-1]), "w") as f:
            f.write("")
        (filenames[-1].parent / (filenames[-1].name + "-extracted")).mkdir()

        # The version corresponding to the last etag should be returned, since
        # that one has the latest "last modified" time.
        assert get_from_cache(url)[0] == filenames[-1]

        # We also want to make sure this works when the latest cached version doesn't
        # have a corresponding etag.
        filename = self.TEST_DIR / resource_to_filename(url)
        meta = Meta(resource=url, cached_path=str(filename), creation_time=time.time(), size=2341)
        meta.to_file()
        with open(filename, "w") as f:
            f.write("some random data")

        assert get_from_cache(url)[0] == filename

    @responses.activate
    def test_get_from_cache(self):
        url = "http://fake.datastore.com/glove.txt.gz"
        self.set_up_glove(url, self.glove_bytes, change_etag_every=2)

        filename, _ = get_from_cache(url)
        assert filename == self.TEST_DIR / resource_to_filename(url, etag="0")
        meta = Meta.from_path(_meta_file_path(filename))
        assert meta.resource == url

        # We should have made one HEAD request and one GET request.
        method_counts = Counter(call.request.method for call in responses.calls)
        assert len(method_counts) == 2
        assert method_counts["HEAD"] == 1
        assert method_counts["GET"] == 1

        # And the cached file should have the correct contents
        with open(filename, "rb") as cached_file:
            assert cached_file.read() == self.glove_bytes

        # A second call to `get_from_cache` should make another HEAD call
        # but not another GET call.
        filename2, _ = get_from_cache(url)
        assert filename2 == filename

        method_counts = Counter(call.request.method for call in responses.calls)
        assert len(method_counts) == 2
        assert method_counts["HEAD"] == 2
        assert method_counts["GET"] == 1

        with open(filename2, "rb") as cached_file:
            assert cached_file.read() == self.glove_bytes

        # A third call should have a different ETag and should force a new download,
        # which means another HEAD call and another GET call.
        filename3, _ = get_from_cache(url)
        assert filename3 == self.TEST_DIR / resource_to_filename(url, etag="1")

        method_counts = Counter(call.request.method for call in responses.calls)
        assert len(method_counts) == 2
        assert method_counts["HEAD"] == 3
        assert method_counts["GET"] == 2

        with open(filename3, "rb") as cached_file:
            assert cached_file.read() == self.glove_bytes

    @responses.activate
    def test_http_200(self):
        url = "http://fake.datastore.com/glove.txt.gz"
        self.set_up_glove(url, self.glove_bytes)

        # non-existent file
        with pytest.raises(FileNotFoundError):
            filename = cached_path(self.FIXTURES_ROOT / "does_not_exist" / "fake_file.tar.gz")

        # unparsable URI
        with pytest.raises(ValueError):
            filename = cached_path("fakescheme://path/to/fake/file.tar.gz")

        # existing file as path
        assert cached_path(self.glove_file) == self.glove_file

        # caches urls
        filename = cached_path(url)

        assert len(responses.calls) == 2
        assert filename == self.TEST_DIR / resource_to_filename(url, etag="0")

        with open(filename, "rb") as cached_file:
            assert cached_file.read() == self.glove_bytes

        # archives
        filename = cached_path(
            self.FIXTURES_ROOT / "common" / "quote.tar.gz!quote.txt",
            extract_archive=True,
        )
        with open(filename, "r") as f:
            assert f.read().startswith("I mean, ")

    @responses.activate
    def test_http_404(self):
        url_404 = "http://fake.datastore.com/does-not-exist"
        byt = b"Does not exist"
        for method in (responses.GET, responses.HEAD):
            responses.add(
                method,
                url_404,
                body=byt,
                status=404,
                headers={"Content-Length": str(len(byt))},
            )

        with pytest.raises(FileNotFoundError):
            cached_path(url_404)

    @responses.activate
    def test_http_500(self):
        url_500 = "http://fake.datastore.com/server-error"
        byt = b"Server error"
        for method in (responses.GET, responses.HEAD):
            responses.add(
                method,
                url_500,
                body=byt,
                status=500,
                headers={"Content-Length": str(len(byt))},
            )

        with pytest.raises(HTTPError):
            cached_path(url_500)

    @responses.activate
    def test_http_502(self):
        url_502 = "http://fake.datastore.com/server-error"
        byt = b"Server error"
        for method in (responses.GET, responses.HEAD):
            responses.add(
                method,
                url_502,
                body=byt,
                status=502,
                headers={"Content-Length": str(len(byt))},
            )

        with pytest.raises(RecoverableServerError):
            cached_path(url_502)


class TestCachedPathLocalFiles(BaseTestClass):
    def test_path_with_home_shortcut(self):
        with tempfile.NamedTemporaryFile(dir=Path.home()) as tmp_file:
            full_path = Path(tmp_file.name)
            fname = full_path.name
            short_path = f"~/{fname}"
            assert cached_path(short_path) == full_path
            assert cached_path(Path(short_path)) == full_path


class TestCachedPathWithArchive(BaseTestClass):
    def setup_method(self):
        super().setup_method()
        self.tar_file = self.TEST_DIR / "utf-8.tar.gz"
        shutil.copyfile(
            self.FIXTURES_ROOT / "utf-8_sample" / "archives" / "utf-8.tar.gz", self.tar_file
        )
        self.zip_file = self.TEST_DIR / "utf-8.zip"
        shutil.copyfile(
            self.FIXTURES_ROOT / "utf-8_sample" / "archives" / "utf-8.zip", self.zip_file
        )

    def test_extract_with_external_symlink(self):
        dangerous_file = self.FIXTURES_ROOT / "common" / "external_symlink.tar.gz"
        with pytest.raises(ValueError):
            cached_path(dangerous_file, extract_archive=True)

    def check_extracted(self, extracted: Path):
        assert extracted.is_dir()
        assert extracted.parent == self.TEST_DIR
        assert (extracted / "dummy.txt").is_file()
        assert (extracted / "folder/utf-8_sample.txt").is_file()
        assert _meta_file_path(extracted).is_file()

    def test_cached_path_extract_local_tar(self):
        extracted = cached_path(self.tar_file, extract_archive=True)
        self.check_extracted(extracted)

    def test_cached_path_extract_local_zip(self):
        extracted = cached_path(self.zip_file, extract_archive=True)
        self.check_extracted(extracted)

    @responses.activate
    def test_cached_path_extract_remote_tar(self):
        url = "http://fake.datastore.com/utf-8.tar.gz"
        byt = open(self.tar_file, "rb").read()

        responses.add(
            responses.GET,
            url,
            body=byt,
            status=200,
            content_type="application/tar+gzip",
            headers={"Content-Length": str(len(byt))},
        )
        responses.add(
            responses.HEAD,
            url,
            status=200,
            headers={"ETag": "fake-etag"},
        )

        extracted = cached_path(url, extract_archive=True)
        assert extracted.name.endswith("-extracted")
        self.check_extracted(extracted)

    @responses.activate
    def test_cached_path_extract_remote_zip(self):
        url = "http://fake.datastore.com/utf-8.zip"
        byt = open(self.zip_file, "rb").read()

        responses.add(
            responses.GET,
            url,
            body=byt,
            status=200,
            content_type="application/zip",
            headers={"Content-Length": str(len(byt))},
        )
        responses.add(
            responses.HEAD,
            url,
            status=200,
            headers={"ETag": "fake-etag"},
        )

        extracted = cached_path(url, extract_archive=True)
        assert extracted.name.endswith("-extracted")
        self.check_extracted(extracted)


class TestCachedPathGs(BaseTestClass):
    @flaky
    def test_cache_blob(self):
        path = cached_path("gs://allennlp-public-models/bert-xsmall-dummy.tar.gz")
        assert path.is_file()
        meta = Meta.from_path(_meta_file_path(path))
        assert meta.etag is not None

    @flaky
    def test_cache_and_extract_blob(self):
        path = cached_path(
            "gs://allennlp-public-models/bert-xsmall-dummy.tar.gz", extract_archive=True
        )
        assert path.is_dir()
        meta = Meta.from_path(_meta_file_path(path))
        assert meta.extraction_dir
        assert meta.etag is not None

    @flaky
    def test_file_not_found(self):
        with pytest.raises(FileNotFoundError):
            cached_path("gs://allennlp-public-models/does-not-exist")


class TestCachedPathS3(BaseTestClass):
    @flaky
    def test_cache_object(self):
        path = cached_path("s3://allennlp/datasets/squad/squad-dev-v1.1.json")
        assert path.is_file()
        meta = Meta.from_path(_meta_file_path(path))
        assert meta.etag is not None


class TestCachedPathHf(BaseTestClass):
    @flaky
    def test_cached_download_no_user_or_org(self):
        path = cached_path("hf://t5-small/config.json")
        assert path.is_file()
        assert path.parent == self.TEST_DIR
        meta = Meta.from_path(_meta_file_path(path))
        assert meta.etag is not None
        assert meta.resource == "hf://t5-small/config.json"

    @flaky
    def test_snapshot_download_no_user_or_org(self):
        # This is the smallest snapshot I could find that is not associated with a user / org.
        model_name = "distilbert-base-german-cased"
        path = cached_path(f"hf://{model_name}")
        assert path.is_dir()
        meta = Meta.from_path(_meta_file_path(path))
        assert meta.resource == f"hf://{model_name}"

    def test_snapshot_download_ambiguous_url(self):
        # URLs like 'hf://xxxx/yyyy' are potentially ambiguous,
        # because this could refer to either:
        #  1. the file 'yyyy' in the 'xxxx' repository, or
        #  2. the repo 'yyyy' under the user/org name 'xxxx'.
        # We default to (1), but if we get a 404 error or 401 error then we try (2).
        model_name = "lysandre/test-simple-tagger-tiny"
        path = cached_path(f"hf://{model_name}")  # should resolve to option 2.
        assert path.is_dir()
        meta = Meta.from_path(_meta_file_path(path))
        assert meta.resource == f"hf://{model_name}"
