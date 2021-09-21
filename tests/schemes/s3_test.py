import pytest

from cached_path.schemes.s3 import split_s3_path


def test_split_s3_path():
    # Test splitting good urls.
    assert split_s3_path("s3://my-bucket/subdir/file.txt") == ("my-bucket", "subdir/file.txt")
    assert split_s3_path("s3://my-bucket/file.txt") == ("my-bucket", "file.txt")

    # Test splitting bad urls.
    with pytest.raises(ValueError):
        split_s3_path("s3://")
        split_s3_path("s3://myfile.txt")
        split_s3_path("myfile.txt")
