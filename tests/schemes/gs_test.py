import pytest

from cached_path.schemes.gs import split_gcs_path


def test_split_gcs_path():
    # Test splitting good urls.
    assert split_gcs_path("gs://my-bucket/subdir/file.txt") == ("my-bucket", "subdir/file.txt")
    assert split_gcs_path("gs://my-bucket/file.txt") == ("my-bucket", "file.txt")

    # Test splitting bad urls.
    with pytest.raises(ValueError):
        split_gcs_path("gs://")
        split_gcs_path("gs://myfile.txt")
        split_gcs_path("myfile.txt")
