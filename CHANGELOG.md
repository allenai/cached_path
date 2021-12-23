# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [v1.0.2](https://github.com/allenai/cached_path/releases/tag/v1.0.2) - 2021-12-23

### Fixed

- Fixed snapshot downloads from HuggingFace Hub.

## [v1.0.1](https://github.com/allenai/cached_path/releases/tag/v1.0.1) - 2021-12-02

### Added

- Added support for latest version of `huggingface-hub` (v0.2.0).

## [v1.0.0](https://github.com/allenai/cached_path/releases/tag/v1.0.0) - 2021-11-29

### Removed

- Removed dependency on the `overrides` package

## [v0.3.4](https://github.com/allenai/cached_path/releases/tag/v0.3.4) - 2021-11-19

## [v0.3.3](https://github.com/allenai/cached_path/releases/tag/v0.3.3) - 2021-11-17

### Changed

- `filelock >= 3.4` required.

## [v0.3.2](https://github.com/allenai/cached_path/releases/tag/v0.3.2) - 2021-11-03

### Changed

- Updated HuggingFace Hub requirement to support 0.1.0.

## [v0.3.1](https://github.com/allenai/cached_path/releases/tag/v0.3.1) - 2021-10-07

### Fixed

- Fixed `FileLock` issue that `overrides` was complaining about.

## [v0.3.0](https://github.com/allenai/cached_path/releases/tag/v0.3.0) - 2021-09-23

### Changed

- Renamed `SchemeClient.connection_error_types` to `recoverable_errors`, and included `requests.exceptions.Timeout`.
- `HttpClient` now considers 502, 503, and 504 as `recoverable_errors`.

## [v0.2.0](https://github.com/allenai/cached_path/releases/tag/v0.2.0) - 2021-09-22

### Added

- Added function `set_cache_dir` for overriding the global default cache directory.
- Added function `get_cache_dir` for getting the global default cache directory.
- Added function `add_scheme_client` for extending `cached_path` to handle more URL schemes.
- Added function `file_friendly_logging` to turn file friendly logging on globally.

### Changed

- `_Meta` dataclass renamed to `Meta`.
- `FileLock` moved to `cached_path.file_lock`.
- `CacheFile` moved to `cached_path.cache_file`.
- The download progress bar uses 1024 instead of 1000 as the unit scale.
- AWS S3 and Google Cloud Storage downloads now have a progress bar.

### Fixed

- For HTTP resources, when the server returns a 404 `cached_path()` now raises `FileNotFoundError`
  for consistency.
- Fixed fetching ETag / MD5 hash for Google Cloud Storage resources.
- Made Google Cloud Storage requests more robust by adding a retry policy and checking MD5 sums.

## [v0.1.0](https://github.com/allenai/cached_path/releases/tag/v0.1.0) - 2021-09-09

### Added

- Added code for a file utility library that provides a unified, simple interface for accessing both local and remote files. This can be used behind other APIs that need to access files agnostic to where they are located.
