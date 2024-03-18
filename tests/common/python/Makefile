include ../vars.mk


target/hyperbee.py: $(C_LIB_PATH) target/libhyperbee.so | target
	cargo run -F ffi --bin uniffi-bindgen generate --library $(C_LIB_PATH) --language python --out-dir target

target/libhyperbee.so: $(C_LIB_PATH) | target
	cp $(C_LIB_PATH) target/.

target:
	mkdir -p $@

include ../Makefile
