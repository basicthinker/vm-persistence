CXX = c++
CFLAGS = -fpic -Wall -c -std=c++11

INTERFACE = vm_database
DEP_PROJ = ../lib
SRCS = $(wildcard *.cc)
OBJS = $(SRCS:.cc=.o)
INCLUDES = -I$(DEP_PROJ)

YCSB_PKG = com.yahoo.ycsb.db
YCSB_DIR = ../ycsb/vmdb/src/main/java/com/yahoo/ycsb/db/

UNAME_S = $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	JDK_DIR = /usr/lib/jvm/java-7-openjdk-amd64
	INCLUDES += -I$(JDK_DIR)/include -I$(JDK_DIR)/include/linux
	LOPT = -shared
	LIB_SUFFIX = so
else ifeq ($(UNAME_S),Darwin)
	INCLUDES += -I/System/Library/Frameworks/JavaVM.framework/Headers
	LOPT = -dynamiclib
	LIB_SUFFIX = jnilib
endif

all:	test_swig compile

ycsb:	ycsb_swig compile

test:	test_swig compile
	javac test/test.java
	java test.Test

ycsb_swig:	$(INTERFACE).h $(wildcard *.swg)
	swig -java -c++ -package $(YCSB_PKG) -outdir $(YCSB_DIR) \
			$(INTERFACE).swg

test_swig:	$(INTERFACE).h $(wildcard *.swg)
	swig -java -c++ -package test -outdir ./test $(INTERFACE).swg

compile:	$(INTERFACE).h $(SRCS)
	$(CXX) $(CFLAGS) $(INTERFACE)_wrap.cxx $(SRCS) $(INCLUDES)
	$(CXX) $(LOPT) $(OBJS) $(INTERFACE)_wrap.o -o libvmdb.$(LIB_SUFFIX)
	
clean:
	$(RM) *.cxx *.o *.$(LIB_SUFFIX) *.java *.class

