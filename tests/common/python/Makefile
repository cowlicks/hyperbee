include ../vars.mk

# Build the Python Hyperbee library
$(TARGET_DEBUG_DIR)/hyperbee.py: $(C_LIB_PATH)
	cargo run -F ffi --bin uniffi-bindgen generate --library $(C_LIB_PATH) --language python --out-dir $(TARGET_DEBUG_DIR)

# Pull in the $(C_LIB_PATH) rule
include ../Makefile
