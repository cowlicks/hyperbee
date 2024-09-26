# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

<!-- next-header -->

## [Unreleased] - ReleaseDate

### Added

- CI checks for pull requests
- Enable basic replication

### Changed

- Work around some broken python & FFI tests

### Removed



## [0.5.0] - 2024-08-25

### Added

- Added `Hyperbee::from_hypercore`.
- Made `HyperbeeError` non-exhaustive [#32](https://github.com/cowlicks/hyperbee/pull/32)

### Changed

- Update to latest Hypercore with IO type erased. This resulted in 4 dependencies being removed.

### Removed

- Removed some types that should not have been public:
    * `HyperbeeBuilder` this was impossible to use anyway because it required a private type (`Tree`).
    * `HyperbeeBuilderError` consumers should use `HyperbeeError` instead.


## [0.4.0] - 2024-03-31

### Added

- Infastructure to publish python package. The package will be published when this is released.
- Added docstrings to ffi functions
- `ffi` feature now default so that we can build and distribute python packages. This will be fixed when package in split into workspaces.
- `ffi` feature added that lets us build libraries for other languages. Includes `Hyperbee`'s `.get`, `.put`, `.del`, and `.sub` methods. Only Python has been tested so far. Generating bindings is documented in the README.

### Changed

- `Debug` implementation for `Children`, `Child`, `Node` and `BlockEntry` changed to be more readable.
- On-disk binary output is now identical to JavaScript Hyperbee.
- `Hyperbee` no longer takes a generic parameter with `CoreMem` bound.

### Removed

- `Hyperbee::print` and `Hyperbee::height` are removed from the default API. They can still be accessed by enabling the `debug` feature.
- `CoreMem` trait and all usage of it.



## [0.3.1] - 2024-02-29

## [0.3.0] - 2024-02-29

### Added

- Make `prefixed` module, `HyperbeeBuilderError`, and `messages::header::Metadata` public

### Changed

- Make `Hyprebee::traverse` and `Prefixed::traverse` return `impl Stream<Item = KeyDataResult>`.
- Make `Prefixed::traverse` strip prefix and separator from yielded keys. The method now returns `impl Stream` instead of `Traverse`.
- Now the `.sub` method, and `Prefixed` struct require configuration with `PrefixedConfig`, which has a `seperator` field. This separates the key and the prefix.
`separor` defaults to the NULL byte, which is the same as the JavaScript implementation.

### Removed

- `BlockEntry`, `KeyValue`, `Node` and `Child` are now private
- You can now no longer manually construct `Traverse` or `Prefixed` structs

## [0.2.2] - 2024-02-23

### Added

- More docs
- Add Rust to JavaScript integration tests for file system data

### Changed

- Move `tracing-subscriber`from regular dependencies to dev-dependencies

### Removed

<!-- next-url -->
[Unreleased]: https://github.com/cowlicks/hyperbee/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/cowlicks/hyperbee/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/cowlicks/hyperbee/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/cowlicks/hyperbee/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/cowlicks/hyperbee/compare/v0.2.2...v0.3.0
[0.2.2]: https://github.com/cowlicks/hyperbee/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/cowlicks/hyperbee/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/cowlicks/hyperbee/releases/tag/v0.2.0
