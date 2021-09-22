import pytest

from cached_path.schemes.gs import GsClient


def test_split_gcs_path():
    # Test splitting good urls.
    assert GsClient.split_gcs_path("gs://my-bucket/subdir/file.txt") == (
        "my-bucket",
        "subdir/file.txt",
    )
    assert GsClient.split_gcs_path("gs://my-bucket/file.txt") == ("my-bucket", "file.txt")

    # Test splitting bad urls.
    with pytest.raises(ValueError):
        GsClient.split_gcs_path("gs://")
        GsClient.split_gcs_path("gs://myfile.txt")
        GsClient.split_gcs_path("myfile.txt")
