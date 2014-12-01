CXX = clang++
CFLAGS = -std=c++11 -Wall
INCLUDES = -I../../slib

DEFINE = $(if $(HASHTABLE), -D$(HASHTABLE))
ifeq ($(HASHTABLE), SVM)
        LIBS = -lsitevm -lpthread -ldune
else ifeq ($(HASHTABLE), TBB)
        LIBS = -ltbb
endif

all:	test

test:	test_hashtable.cc $(wildcard *.h)
	$(CXX) $(CFLAGS) $(DEFINE) $(INCLUDES) test_hashtable.cc $(LIBS) -o test.out

clean:
	$(RM) *.out

