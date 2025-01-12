# This file is loaded from other Makefile's so we can't use simple "include ../foo" because 
# that is resolved relative to the original Makefile.
#
# $(dir $(lastword $(MAKEFILE_LIST))) gets the dir of the current Makefile
include $(dir $(lastword $(MAKEFILE_LIST)))vars.mk

# Build Rust FFI library
$(C_LIB_PATH): $(RS_SOURCES) $(ROOT)/Cargo.toml
	cd $(ROOT) && cargo build -F ffi
