CXX = clang++
CFLAGS = -std=c++11 -Wall
INCLUDES = -I../../ -I/home/basicthinker/Projects/sitevm_dune-master/

DEFINE = $(if $(HT), -D$(HT))
ifeq ($(HT), SLIB)
	DEFINE += -DSVM
	LIBS = -L/home/basicthinker/Projects/sitevm_dune-master/bin -L/home/basicthinker/Projects/dune/libdune/
	LIBS += -lsitevm -lpthread -ldune
else ifeq ($(HT), SVM)
	LIBS = -L/home/basicthinker/Projects/sitevm_dune-master/bin -L/home/basicthinker/Projects/dune/libdune/
	LIBS += -lsitevm -lpthread -ldune
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

