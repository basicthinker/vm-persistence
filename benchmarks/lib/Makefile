CXX = clang++
CFLAGS = -std=c++11 -Wall
INCLUDES = -I../../

DEFINE = $(if $(HT), -D$(HT))
ifeq ($(HT), SLIB)
        DEFINE += -DSVM
        LIBS = -lsitevm -lpthread -ldune
else ifeq ($(HT), SVM)
        LIBS = -lsitevm -lpthread -ldune
else ifeq ($(HT), TBB)
        LIBS = -ltbb
endif

all:	test

test:	test_hashtable.cc $(wildcard *.h)
	$(CXX) $(CFLAGS) $(DEFINE) $(INCLUDES) test_hashtable.cc $(LIBS) -o test.out

clean:
	$(RM) *.out

