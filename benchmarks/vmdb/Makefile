CXX = c++
CFLAGS = -fpic -Wall -c
LOPT = -shared

INTERFACE = vm_database
SRCS = $(wildcard *.cc)
OBJS = $(SRCS: .cc=.o)

UNAME_S = $(shell uname -s)
ifeq ($(UNAME_S),Linux)
	JDK_DIR = /usr/lib/jvm/java-7-openjdk-amd64
	INCLUDES = -I$(JDK_DIR)/include -I$(JDK_DIR)/include/linux
	LIB_SUFFIX = so
endif
ifeq ($(UNAME_S),Darwin)
	INCLUDES = -I/System/Library/Frameworks/JavaVM.framework/Headers
	LIB_SUFFIX = jnilib
endif

all:	$(INTERFACE).h $(SRCS)
	swig -java -c++ $(INTERFACE).swg
	$(CXX) $(CFLAGS) $(INTERFACE)_wrap.cxx $(SRCS) $(INCLUDES)
	$(CXX) $(LOPT) $(OBJS) $(INTERFACE)_wrap.o -o lib$(INTERFACE).$(LIB_SUFFIX)

clean:
	$(RM) *.cxx *.o *.$(LIB_SUFFIX) *.java *.class

