CC=g++
CFLAGS+=-std=gnu++11 -g -Wall #-DSPINNING_NOTIFIER #-DTRACE
LDFLAGS+=-lpthread -lrt -lboost_system -lz -lpmem
SRCS=$(wildcard *.cc)
OBJS=$(SRCS:.cc=.out)

all: $(OBJS)

.cc.out:
	$(CC) $(CFLAGS) $< $(LDFLAGS) -o $@
clean:
	$(RM) *.o *.out
