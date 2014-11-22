CXX = c++
CFLAGS = -std=c++11 -Wall

DEFINE = $(if $(HASHTABLE), -D$(HASHTABLE))
ifeq ($(HASHTABLE), SVM)
        LIBS = -lsitevm -lpthread -ldune
else ifeq ($(HASHTABLE), TBB)
        LIBS = -ltbb
endif

all:	test

test:	test_hashtable.cc $(wildcard *.h)
	$(CXX) $(CFLAGS) $(DEFINE) test_hashtable.cc $(LIBS) -o test.out

clean:
	$(RM) *.out

