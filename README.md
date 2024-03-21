# Hyperbee

A peer-to-peer append-only B-tree built on Hypercore. Compatible with the [JavaScript version](https://github.com/holepunchto/hyperbee).
```
$ cargo add hyperbee
```

# Usage

From the [examples](/examples/ram.rs):

```rust
use hyperbee::Hyperbee;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let hb = Hyperbee::from_ram().await?;
    // Insert "world" with key "hello"
    hb.put(b"hello", Some(b"world")).await?;

    // Get the value for key "hello"
    let Some((_seq, Some(val))) = hb.get(b"hello").await? else {
        panic!("could not get value");
    };
    assert_eq!(val, b"world");

    // Trying to get a non-exsitant key returns `None`
    let res = hb.get(b"no key here").await?;
    assert_eq!(res, None);

    // Deleting a key returns `true` if it was present
    let res = hb.del(b"hello").await?;
    assert!(res.is_some());

    // Getting deleted key returns `None`
    let res = hb.get(b"hello").await?;
    assert_eq!(res, None);

    Ok(())
}
```

## Foreign Language Bindings

We use [UniFFI](https://mozilla.github.io/uniffi-rs/) to generate libraries for other languages. To build the library for python run:
```bash
cargo build -F ffi && cargo run -F ffi --bin uniffi-bindgen generate --library target/debug/libhyperbee.so --language python --out-dir out
```
This generates a file `out/hyperbee.py`, which an be used. This file requires that `libhyperbee.so` be present alongside the `.py` file.
Distributable python packages are still a work-in-progress. Currently only Python is tested. See [the tests](tests/python.rs) for example usage.

## Parity with JS Hyperbee

- [x] full functional interoperability with JS Hyperbee files
- [x] read, write, and delete operations
- [x] in-order key streaming like JS's [`createReadStream`](https://docs.pears.com/building-blocks/hyperbee#db.createreadstream)
- [x] support `gt`, `lt`,  etc bounds for key streaming
- [x] accept compare-and-swap for `put` and `del`.
- [x] support prefixed key operations like JS's [`sub`](https://docs.pears.com/building-blocks/hyperbee#db.sub)
- [ ] one-to-one binary output

## Future work

- [x] Build FFI wrappers
- [ ] improved wire format
- [ ] configurable tree parameters

## Development

Run the tests with `$ cargo test`.

Each significant pull request should include an update the [`CHANGELOG.md`](CHANGELOG.md)

## Release

Releases are mostly handled with [`cargo release`](https://github.com/crate-ci/cargo-release).
After each Rust release. We manually release a new python package. Building the python package is done with [`maturin`](https://github.com/PyO3/maturin) and it is released with [`twine`](https://pypi.org/project/twine/).
```bash
# create a virtualenv with the tools we need
python -m venv venv && . venv/bin/activate
pip install --upgrade twine maturin

# build the wheels
python -m maturin build --release

# Upload the wheels to pypi. This uses my api token from ~/.pypirc
python -m twine upload target/wheels/hyperbeepy-<target>.whl
```
