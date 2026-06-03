CC      ?= gcc
CFLAGS  := -std=c11 -O2 -Wall -Wextra -Wpedantic -pthread \
           -D_POSIX_C_SOURCE=200809L -Isrc

ifeq ($(SANITIZE),1)
  CFLAGS += -fsanitize=thread -g -O1
endif

.PHONY: all test clean

all: test_all

test_all: test/test_all.c src/smpc.h src/dq_spsc.h src/dq_mpsc.h src/dq_spmc.h src/dq_mpmc.h src/dq_platform.h
	$(CC) $(CFLAGS) -o $@ test/test_all.c

test: test_all
	./test_all

clean:
	rm -f test_all
