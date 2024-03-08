# Taken from https://github.com/alexcrichton/rust-ffi-examples/tree/master/c-to-rust
#
ifeq ($(shell uname),Darwin)
    LDFLAGS := -Wl,-dead_strip
else
    LDFLAGS := -Wl,--gc-sections -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc -lssl -lcrypto
endif

ROOT:= $(shell git rev-parse --show-toplevel)
SRC := $(ROOT)/src
C_SOURCES := $(wildcard *.c);
RS_SOURCES := $(wildcard $(SRC)/*.rs)

# Buid the executable linking object file to Rust FFI library
target/hyperbee: target/test.o $(ROOT)/target/debug/libhyperbee.so
	$(CC) -o $@ $^ $(LDFLAGS)

target:
	mkdir -p $@

# Build the object file
target/test.o: test.c | target
	$(CC) -o $@ -c $<

# Build Rust FFI library
$(ROOT)/target/debug/libhyperbee.a: $(RS_SOURCES) $(ROOT)/Cargo.toml
	cd $(ROOT) && cargo build

test: target/hyperbee
	target/hyperbee

clean:
	rm -rf target

.PHONY := test clean
