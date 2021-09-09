# cached_path

A file utility library that provides a unified, simple interface for accessing both local and remote files.
This can be used behind other APIs that need to access files agnostic to where they are located.

<p align="center">
    <a href="https://github.com/allenai/cached_path/actions">
        <img alt="CI" src="https://github.com/allenai/cached_path/workflows/CI/badge.svg?event=push&branch=main">
    </a>
    <a href="https://pypi.org/project/cached_path/">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/cached_path">
    </a>
    <a href="https://github.com/allenai/cached_path/blob/main/LICENSE">
        <img alt="License" src="https://img.shields.io/github/license/allenai/cached_path.svg?color=blue&cachedrop">
    </a>
    <a href="https://codecov.io/gh/allenai/cached_path">
        <img alt="Codecov" src="https://codecov.io/gh/allenai/cached_path/branch/main/graph/badge.svg">
    </a>
    <br/>
</p>

## Installation

`cached_path` requires Python 3.6.1 or later.

```bash
pip install cached_path
```

## Usage

```python
from cached_path import cached_path
```

Given something that might be a URL or local path, `cached_path` determines which.
If it's a remote resource, it downloads the file and caches it, and
then returns the path to the cached file. If it's already a local path,
it makes sure the file exists and returns the path.

For URLs, "http://", "https://", "s3://", "gs://", and "hf://" are all supported.
The latter corresponds to the HuggingFace Hub.

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

## Team

`cached_path` is developed and maintained by the AllenNLP team, backed by [the Allen Institute for Artificial Intelligence (AI2)](https://allenai.org/).
AI2 is a non-profit institute with the mission to contribute to humanity through high-impact AI research and engineering.
To learn more about who specifically contributed to this codebase, see [our contributors](https://github.com/allenai/cached_path/graphs/contributors) page.
