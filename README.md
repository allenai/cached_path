# [cached-path](https://cached-path.readthedocs.io/)

<!-- start tagline -->

A file utility library that provides a unified, simple interface for accessing both local and remote files.
This can be used behind other APIs that need to access files agnostic to where they are located.

<!-- end tagline -->

<p align="center">
    <a href="https://github.com/allenai/cached_path/actions">
        <img alt="CI" src="https://github.com/allenai/cached_path/workflows/CI/badge.svg?event=push&branch=main">
    </a>
    <a href="https://pypi.org/project/cached_path/">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/cached_path">
    </a>
    <a href="https://cached-path.readthedocs.io/en/latest/?badge=latest">
        <img src="https://readthedocs.org/projects/cached-path/badge/?version=latest" alt="Documentation Status" />
    </a>
    <a href="https://github.com/allenai/cached_path/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/github/license/allenai/cached_path.svg?color=blue&cachedrop">
    </a>
    <br/>
</p>

## Quick links

- [Documentation](https://cached-path.readthedocs.io/)
- [PyPI Package](https://pypi.org/project/cached-path/)
- [Contributing](https://github.com/allenai/cached_path/blob/main/CONTRIBUTING.md)
- [License](https://github.com/allenai/cached_path/blob/main/LICENSE)

## Installation

<!-- start py version -->

**cached-path** requires Python 3.7 or later.

<!-- end py version -->

### Installing with `pip`

<!-- start install pip -->

**cached-path** is available [on PyPI](https://pypi.org/project/cached-path/). Just run

```bash
pip install cached-path
```

<!-- end install pip -->

### Installing from source

<!-- start install source -->

To install **cached-path** from source, first clone [the repository](https://github.com/allenai/cached_path):

```bash
git clone https://github.com/allenai/cached_path.git
cd cached_path
```

Then run

```bash
pip install -e .
```

<!-- end install source -->

## Usage

```python
from cached_path import cached_path
```

Given something that might be a URL or local path, `cached_path()` determines which.
If it's a remote resource, it downloads the file and caches it to the [cache directory](#cache-directory), and
then returns the path to the cached file. If it's already a local path,
it makes sure the file exists and returns the path.

For URLs, `http://`, `https://`, `s3://` (AWS S3), `gs://` (Google Cloud Storage), and `hf://` (HuggingFace Hub) are all supported out-of-the-box.
Optionally `beaker://` URLs in the form of `beaker://{user_name}/{dataset_name}/{file_path}` are supported, which requires [beaker-py](https://github.com/allenai/beaker-py) to be installed.

For example, to download the PyTorch weights for the model `epwalsh/bert-xsmall-dummy`
on HuggingFace, you could do:

```python
cached_path("hf://epwalsh/bert-xsmall-dummy/pytorch_model.bin")
```

For paths or URLs that point to a tarfile or zipfile, you can also add a path
to a specific file to the `url_or_filename` preceeded by a "!", and the archive will
be automatically extracted (provided you set `extract_archive` to `True`),
returning the local path to the specific file. For example:

```python
cached_path("model.tar.gz!weights.th", extract_archive=True)
```

### Using custom headers for HTTP requests

You can provide custom headers for HTTP requests, which is useful for accessing private resources that require authentication:

```python
# Using an API token for private resources (e.g. Hugging Face)
headers = {"Authorization": f"Bearer {hf_token}"}
cached_path("https://huggingface.co/api/models/private-model/resolve/main/model.bin", headers=headers)
```

This is particularly useful for downloading private files from services like Hugging Face, GitHub, or any other API that uses Bearer token authentication.

### Cache directory

By default the cache directory is `~/.cache/cached_path/`, however there are several ways to override this setting:
- set the environment variable `CACHED_PATH_CACHE_ROOT`,
- call `set_cache_dir()`, or
- set the `cache_dir` argument each time you call `cached_path()`.

## Team

<!-- start team -->

**cached-path** is developed and maintained by the AllenNLP team, backed by [the Allen Institute for Artificial Intelligence (AI2)](https://allenai.org/).
AI2 is a non-profit institute with the mission to contribute to humanity through high-impact AI research and engineering.
To learn more about who specifically contributed to this codebase, see [our contributors](https://github.com/allenai/cached_path/graphs/contributors) page.

<!-- end team -->

## License

<!-- start license -->

**cached-path** is licensed under [Apache 2.0](https://www.apache.org/licenses/LICENSE-2.0).
A full copy of the license can be found [on GitHub](https://github.com/allenai/cached_path/blob/main/LICENSE).

<!-- end license -->
