# Taken from https://github.com/alexcrichton/rust-ffi-examples/tree/master/c-to-rust
#
ifeq ($(shell uname),Darwin)
    LDFLAGS := -Wl,-dead_strip
else
    LDFLAGS := -Wl,--gc-sections -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc -lssl -lcrypto
endif

include ../vars.mk

# Buid the executable linking object file to Rust FFI library
target/hyperbee: target/test.o $(ROOT)/target/debug/libhyperbee.so
	$(CC) -o $@ $^ $(LDFLAGS)

target:
	mkdir -p $@

# Build the object file
target/test.o: test.c | target
	$(CC) -o $@ -c $<

# Build Rust FFI library
$(C_LIB_PATH): $(RS_SOURCES) $(ROOT)/Cargo.toml
	cd $(ROOT) && cargo build -F clib

test: target/hyperbee
	target/hyperbee

clean:
	rm -rf target

.PHONY := test clean
