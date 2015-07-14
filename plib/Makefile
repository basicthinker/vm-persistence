CC=g++
CFLAGS+=-std=gnu++1y -g -Wall #-DSPINNING_NOTIFIER #-DTRACE
#CFLAGS+=-DDEBUG_PLIB
LDFLAGS+=-lpthread -lrt -lboost_system -lz -lpmem
SRCS=$(wildcard *.cc)
OBJS=$(SRCS:.cc=.out)

all: $(OBJS)

.cc.out:
	$(CC) $(CFLAGS) $< $(LDFLAGS) -o $@
clean:
	$(RM) *.o *.out
