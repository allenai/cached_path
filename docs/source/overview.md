Overview
========

The main functionality of **cached-path** is provided by the function [`cached_path()`](api/cached_path).

## Basic usage

`cached_path()` has a single positional argument that is either the path to a local file or the URL of a remote resource.

For example, assuming the file `README.md` exists locally, the returned by is just
the same path that was provided:

```python
assert cached_path("README.md") == "README.md"
```

But for remote resources, the resource will be downloaded and cached to the cache directory:

```python
path = cached_path("https://github.com/allenai/cached_path/blob/main/README.md")
assert os.path.exists(path)
assert os.path.split(path)[0] == os.path.expanduser("~/.cache/cached_path")
```

If you were to call this again after the ETag of the resource has changed, a new version will be cached:

```python
path2 = cached_path("https://github.com/allenai/cached_path/blob/main/README.md")
assert path2 != path
```

```{tip}
In addition to `http` and `https`, `cached_path()` supports several other schemes such as `s3` (AWS S3), `gs` (Google Cloud Storage),
and `hf` (HuggingFace Hub).
For a full list of supported schemes and examples, check the [API documentation](api/cached_path).
```

## Working with archives

`cached_path()` will safely extract archives (`.tar.gz` or `.zip` files) if you set the `extract_archive` argument to `True`:

```python
cached_archive = cached_path(
    "https://github.com/allenai/cached_path/releases/download/v0.1.0/cached_path-0.1.0.tar.gz",
    extract_archive=True,
)
assert os.path.isdir(cached_archive)
```

This works for both local and remote resources.

You can also automatically get the path to a certain file or directory within an archive by appending an exclamation mark "!" followed by
the relative path to the file within the archive to the string given to `cached_path()`:

```python
path = cached_path(
    "https://github.com/allenai/cached_path/releases/download/v0.1.0/cached_path-0.1.0.tar.gz!README.md",
    extract_archive=True,
)
assert os.path.isfile(path)
```

## Overriding the default cache directory

There are several ways to override the default cache directory that `cached_path()` stores cached resource versions to:

1. Set the environment variable `CACHED_PATH_CACHE_ROOT`.
2. Call [`set_cache_dir()`](api/util.html#cached_path.set_cache_dir).
3. Set the [`cache_dir`](api/cached_path.html#cached_path.cached_path) argument each time you call `cached_path()`.
