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
        LIBS = -ltbb -lpthread
else
	HT = STL
	LIBS = -lpthread
endif

all:	test eval

test:	test_hashtable.cc $(wildcard *.h)
	$(CXX) $(CFLAGS) $(DEFINE) $(INCLUDES) test_hashtable.cc $(LIBS) \
		-o test.out

eval:	eval_hashtable.cc $(wildcard *.h)
	$(CXX) $(CFLAGS) $(DEFINE) $(INCLUDES) eval_hashtable.cc $(LIBS) \
		-o eval-$(HT).out

clean:
	$(RM) *.out

