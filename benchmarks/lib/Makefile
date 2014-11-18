CXX = c++
CFLAGS = -std=c++11 -Wall
DEFINE = -DSVM 
LIBS = -ltbb -lsitevm -lpthread -ldune

all:	test

test:	test_hashtable.cc $(wildcard *.h)
	$(CXX) $(CFLAGS) $(DEFINE) test_hashtable.cc $(LIBS) -o test.out

clean:
	$(RM) *.out

