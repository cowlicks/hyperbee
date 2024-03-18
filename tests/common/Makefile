# this is dupped from vars.mk
ROOT:= $(shell git rev-parse --show-toplevel)
RS_SOURCES := $(wildcard $(SRC)/*.rs)
C_LIB_PATH := $(ROOT)/target/debug/libhyperbee.so

# Build Rust FFI library
$(C_LIB_PATH): $(RS_SOURCES) $(ROOT)/Cargo.toml
	cd $(ROOT) && cargo build -F ffi
