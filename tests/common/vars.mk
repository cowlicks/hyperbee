ROOT:= $(shell git rev-parse --show-toplevel)
SRC := $(ROOT)/src
C_SOURCES := $(wildcard *.c);
RS_SOURCES := $(wildcard $(SRC)/*.rs)
TARGET_DEBUG_DIR := $(ROOT)/target/debug
C_LIB_PATH := $(TARGET_DEBUG_DIR)/libhyperbee.so
