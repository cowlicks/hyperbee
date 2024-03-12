#!/usr/bin/env bash
set -e
rm -rf out/*
# NB currently need to build with clib (which is the regular ffi stuff) because the clib and uniffi share the same libhyperbee.so file
cargo build -F clib
cargo run --bin uniffi-bindgen generate --library target/debug/libhyperbee.so --language python --out-dir out
cp target/debug/libhyperbee.so out/.
touch out/__init__.py
python - << EOF

import asyncio
from out.hyperbee import *
async def main():
    db = await hb_from_disk('tests/common/js/data/basic')
    res = await db.get(b'1')
    assert(res.seq == 2)
    assert(res.value == b'24')
    pass

if __name__ ==  '__main__':
    asyncio.run(main())
EOF

echo success
