# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- The download progress bar uses 1024 instead of 1000 as the unit scale.

## [v0.1.0](https://github.com/allenai/cached_path/releases/tag/v0.1.0) - 2021-09-09

### Added

- Added code for a file utility library that provides a unified, simple interface for accessing both local and remote files. This can be used behind other APIs that need to access files agnostic to where they are located.
