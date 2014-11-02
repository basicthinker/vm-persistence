CXX = g++
CFLAGS = -fpic -Wall -c
LOPT = -shared

INTERFACE = vm_database
SRCS = $(wildcard *.cc)
OBJS = $(SRCS: .cc=.o)

JDK_DIR = /usr/lib/jvm/java-7-openjdk-amd64

INCLUDES = -I$(JDK_DIR)/include -I$(JDK_DIR)/include/linux

all:	$(INTERFACE).h $(SRCS)
	swig -java -c++ $(INTERFACE).swg
	$(CXX) $(CFLAGS) $(INTERFACE)_wrap.cxx $(SRCS) $(INCLUDES)
	$(CXX) $(LOPT) $(OBJS) $(INTERFACE)_wrap.o -o lib$(INTERFACE).so

clean:
	$(RM) *.cxx *.o *.so *.java *.class

