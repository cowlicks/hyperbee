include ../vars.mk

target/hyperbee.py: $(C_LIB_PATH) | target
	cargo run --bin uniffi-bindgen generate --library $(C_LIB_PATH) --language python --out-dir target

$(C_LIB_PATH):
	make -C ../c $@

target:
	mkdir -p $@
