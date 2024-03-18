ROOT:= $(shell git rev-parse --show-toplevel)
SRC := $(ROOT)/src
C_SOURCES := $(wildcard *.c);
RS_SOURCES := $(wildcard $(SRC)/*.rs)
C_LIB_PATH := $(ROOT)/target/debug/libhyperbee.so
