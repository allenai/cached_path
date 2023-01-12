from cached_path.schemes import (
    SchemeClient,
    add_scheme_client,
    get_scheme_client,
    get_supported_schemes,
)
from cached_path.util import is_url_or_existing_file


def test_supported_schemes():
    assert "hf" in get_supported_schemes()


class CustomSchemeClient(SchemeClient):
    scheme = "foo"

    def get_etag(self):
        return "AAA"

    def get_size(self):
        return None

    def get_resource(self, temp_file):
        pass


def test_add_scheme():
    assert "foo" not in get_supported_schemes()
    assert not is_url_or_existing_file("foo://bar")

    add_scheme_client(CustomSchemeClient)

    assert "foo" in get_supported_schemes()
    assert is_url_or_existing_file("foo://bar")
    assert isinstance(get_scheme_client("foo://bar"), CustomSchemeClient)
