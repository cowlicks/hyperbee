# Taken from https://github.com/alexcrichton/rust-ffi-examples/tree/master/c-to-rust
#
ifeq ($(shell uname),Darwin)
    LDFLAGS := -Wl,-dead_strip
else
    LDFLAGS := -Wl,--gc-sections -lgcc_s -lutil -lrt -lpthread -lm -ldl -lc -lssl -lcrypto
endif

include ../vars.mk

# Buid the executable linking object file to Rust FFI library
target/hyperbee: target/test.o $(C_LIB_PATH)
	$(CC) -o $@ $^ $(LDFLAGS)

target:
	mkdir -p $@

# Build the object file
target/test.o: test.c | target
	$(CC) -o $@ -c $<

test: target/hyperbee
	target/hyperbee

clean:
	rm -rf target

.PHONY := test clean

include ../Makefile
