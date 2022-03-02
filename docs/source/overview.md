Overview
========

The main functionality of **cached-path** is provided by the function {func}`~cached_path.cached_path()`.

## Basic usage

{func}`~cached_path.cached_path()` has a single positional argument that is either the path to a local file or the URL of a remote resource.

For example, assuming the file `README.md` exists locally, the returned by is just
the same path that was provided:

```python
>>> from cached_path import cached_path
>>> print(cached_path("README.md"))
README.md
>>>
```

But for remote resources, the resource will be downloaded and cached to the cache directory:

```python
>>> from cached_path import get_cache_dir
>>> path = cached_path("https://github.com/allenai/cached_path/blob/main/README.md")
>>> assert path.is_file()
>>> assert path.parent == get_cache_dir()
>>>
```

If you were to call this again after the ETag of the resource has changed, the new version would be downloaded
and the local path returned from `cached_path()` would point to the newly downloaded version.

```{tip}
There are multiple ways to [change the cache directory](#overriding-the-default-cache-directory).
```

## Supported URL schemes

In addition to `http` and `https`, {func}`~cached_path.cached_path()` supports several other schemes such as `s3` (AWS S3), `gs` (Google Cloud Storage),
and `hf` (HuggingFace Hub).
For a full list of supported schemes and examples, check the [API documentation](api/cached_path).

You can also overwrite how any of these schemes are handled or add clients for new schemes with the {func}`~cached_path.add_scheme_client()` method.

## Working with archives

{func}`~cached_path.cached_path()` will safely extract archives (`.tar.gz` or `.zip` files) if you set the `extract_archive` argument to `True`:

```python
>>> cached_archive = cached_path(
...    "https://github.com/allenai/cached_path/releases/download/v0.1.0/cached_path-0.1.0.tar.gz",
...    extract_archive=True,
... )
>>> assert cached_archive.is_dir()
>>>
```

This works for both local and remote resources.

You can also automatically get the path to a certain file or directory within an archive by appending an exclamation mark "!" followed by
the relative path to the file within the archive to the string given to {func}`~cached_path.cached_path()`:

```python
>>> path = cached_path(
...     "https://github.com/allenai/cached_path/releases/download/v0.1.0/cached_path-0.1.0.tar.gz!cached_path-0.1.0/README.md",
...     extract_archive=True,
... )
>>> assert path.is_file()
>>>
```

## Overriding the default cache directory

There are several ways to override the default cache directory that `cached_path()` stores cached resource versions to:

1. Set the environment variable `CACHED_PATH_CACHE_ROOT`.
2. Call {func}`~cached_path.set_cache_dir()`.
3. Set the `cache_dir` argument each time you call {func}`~cached_path.cached_path()`.
