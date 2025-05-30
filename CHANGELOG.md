# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

## [v1.7.3](https://github.com/allenai/cached_path/releases/tag/v1.7.3) - 2025-05-07

### Added

- Added support for latest versions of HuggingFace-Hub.

## [v1.7.2](https://github.com/allenai/cached_path/releases/tag/v1.7.2) - 2025-04-16

### Fixed

- Fixed incompatibility issue with latest boto3/botocore release (>=1.37.34) .

## [v1.7.1](https://github.com/allenai/cached_path/releases/tag/v1.7.1) - 2025-03-11

### Fixed

- Fixed a bug with the headers.

## [v1.7.0](https://github.com/allenai/cached_path/releases/tag/v1.7.0) - 2025-03-11

### Added

- Added `headers` option for passing custom headers to HTTP requests.

## [v1.6.7](https://github.com/allenai/cached_path/releases/tag/v1.6.7) - 2025-01-14

### Added

- Added support for new release of HuggingFace-Hub.

## [v1.6.6](https://github.com/allenai/cached_path/releases/tag/v1.6.6) - 2024-12-19

### Fixed

- Made clients more robust by reusing existing underlying clients.

## [v1.6.5](https://github.com/allenai/cached_path/releases/tag/v1.6.5) - 2024-12-09

### Added

- Caching of S3, R2 and GCS cloud storage clients, for better multi-threading support.

### Fixed

- Loosened `filelock` dependency.
- Fixed issue where making too many calls to Google Cloud Storage causes `Compute Engine Metadata server unavailable` error.

## [v1.6.4](https://github.com/allenai/cached_path/releases/tag/v1.6.4) - 2024-11-20

### Added

- Added support for new version of huggingface-hub.

## [v1.6.3](https://github.com/allenai/cached_path/releases/tag/v1.6.3) - 2024-06-20

### Added

- Added support for new version of huggingface-hub.

## [v1.6.2](https://github.com/allenai/cached_path/releases/tag/v1.6.2) - 2024-03-05

### Fixed

- Updated dependencies
- Fix authentication with AWS profile for R2
- Make R2 throw FileNotFoundError instead of botocore.client.ClientError when object does not exist.

## [v1.6.0](https://github.com/allenai/cached_path/releases/tag/v1.6.0) - 2024-02-22

### Added

- Added support for R2 (`r2://*`).
- `verbose` parameter for `find_latest_cached()`
- Added support for extracting RAR files.

## [v1.5.2](https://github.com/allenai/cached_path/releases/tag/v1.5.2) - 2024-01-09

### Fixed

- Fixed a bug where certain tar files were classified as zip.

## [v1.5.1](https://github.com/allenai/cached_path/releases/tag/v1.5.1) - 2023-12-16

### Removed

- Removed official support for Python 3.7

## [v1.5.0](https://github.com/allenai/cached_path/releases/tag/v1.5.0) - 2023-10-11

### Added

- Added `get_bytes_range()` function.

## [v1.4.0](https://github.com/allenai/cached_path/releases/tag/v1.4.0) - 2023-08-02

### Added

- Added support for file paths in the form of a URL like: `file://`.

## [v1.3.5](https://github.com/allenai/cached_path/releases/tag/v1.3.5) - 2023-07-15

### Changed

- Added support for newest versions of `FileLock` and `huggingface-hub`.

## [v1.3.4](https://github.com/allenai/cached_path/releases/tag/v1.3.4) - 2023-04-06

### Fixed

- Fixed issue where progress bar would jump around for big downloads.

## [v1.3.3](https://github.com/allenai/cached_path/releases/tag/v1.3.3) - 2023-02-16

### Fixed

- Fixed handling `beaker://` URLs when using dataset ID.

## [v1.3.2](https://github.com/allenai/cached_path/releases/tag/v1.3.2) - 2023-02-15

### Changed

- Added support for newest `huggingface-hub` version.

## [v1.3.1](https://github.com/allenai/cached_path/releases/tag/v1.3.1) - 2023-01-18

### Fixed

- No more blank lines when `quiet=True`.

## [v1.3.0](https://github.com/allenai/cached_path/releases/tag/v1.3.0) - 2023-01-12

### Added

- Added optional support for `beaker://` URLs.

## [v1.2.0](https://github.com/allenai/cached_path/releases/tag/v1.2.0) - 2023-01-12

### Changed

- Downloads from HuggingFace will be passed onto the `huggingface_hub` library completely so you won't end up with duplicates of the same objects if your using other libraries that use `huggingface_hub` directly, such as `transformers`.

## [v1.1.6](https://github.com/allenai/cached_path/releases/tag/v1.1.6) - 2022-09-28

### Changed

- When we're exceeding the maximum number of retries, the exception object now contains a string message instead of the last unsuccessful request object.

## [v1.1.5](https://github.com/allenai/cached_path/releases/tag/v1.1.5) - 2022-07-05

## [v1.1.4](https://github.com/allenai/cached_path/releases/tag/v1.1.4) - 2022-06-29

### Changed

- Added support for latest `huggingface_hub` client library (v0.8.1), but dropped support for older versions.

## [v1.1.3](https://github.com/allenai/cached_path/releases/tag/v1.1.3) - 2022-06-13

### Added

- Added `quiet` parameter to `cached_path()` for turning off progress displays, and `progress` parameter for customizing displays.
- Added `SchemeClient.get_size()` method.

### Changed

- Switched to `rich` for progress displays, removed dependency on `tqdm`.

### Removed

- Removed `file_friendly_logging()` function.

## [v1.1.2](https://github.com/allenai/cached_path/releases/tag/v1.1.2) - 2022-04-08

## [v1.1.1](https://github.com/allenai/cached_path/releases/tag/v1.1.1) - 2022-03-25

### Fixed

- Fixed bug where `cached_path()` would fail to find local files with the home shortcut "~/" in their path.

## [v1.1.0](https://github.com/allenai/cached_path/releases/tag/v1.1.0) - 2022-03-03

### Changed

- Python >= 3.7 now required.
- `cached_path()` now returns a `Path` instead of a `str`.

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
