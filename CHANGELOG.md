# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Added

- Make `HyperbeeBuilderError` public
- Made `prefixed` module public

### Changed

- Make `Hyperbee::traverse` return `impl Stream`.
- Make `Prefixed::traverse` strip prefix and separator from yielded keys. The method now returns `impl Stream` instead of `Traverse`.
- Now the `.sub` method, and `Prefixed` struct require configuration with `PrefixedConfig`, which has a `seperator` field. This separates the key and the prefix.
`separor` defaults to the NULL byte, which is the same as the JavaScript implementation.

### Removed

- `BlockEntry`, `KeyValue` and `Child` are now private

## [0.2.2] - 2024-02-23

### Added

- More docs
- Add Rust to JavaScript integration tests for file system data

### Changed

- Move `tracing-subscriber`from regular dependencies to dev-dependencies

### Removed

<!-- next-url -->
[Unreleased]: https://github.com/cowlicks/hyperbee/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/cowlicks/hyperbee/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/cowlicks/hyperbee/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/cowlicks/hyperbee/releases/tag/v0.2.0
