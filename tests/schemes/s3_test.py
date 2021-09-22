import pytest

from cached_path.schemes.s3 import S3Client


def test_split_s3_path():
    # Test splitting good urls.
    assert S3Client.split_s3_path("s3://my-bucket/subdir/file.txt") == (
        "my-bucket",
        "subdir/file.txt",
    )
    assert S3Client.split_s3_path("s3://my-bucket/file.txt") == ("my-bucket", "file.txt")

    # Test splitting bad urls.
    with pytest.raises(ValueError):
        S3Client.split_s3_path("s3://")
        S3Client.split_s3_path("s3://myfile.txt")
        S3Client.split_s3_path("myfile.txt")
