import pytest


@pytest.fixture(autouse=True)
def doctest_fixtures(
    doctest_namespace,
    tmp_path,
):
    doctest_namespace["cache_dir"] = tmp_path
