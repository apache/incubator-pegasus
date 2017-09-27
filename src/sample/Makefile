CXX = g++
CXXFLAGS = -std=c++11 -g
INCLUDE = -I../include
LIBRARY = -Wl,-rpath-link,../lib -L../../DSN_ROOT/lib -lpegasus.clientlib -ldsn.core -lthrift -lpthread -lrt
CPPS = $(wildcard vad_stdaln/*.cpp) $(wildcard ./*.cpp)
OBJS = $(patsubst %.cpp,%.o,$(CPPS))

all: pegasus_cpp_sample

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

pegasus_cpp_sample: $(OBJS)
	$(CXX) -o $@ $(OBJS) $(LIBRARY)

clean:
	rm -f $(OBJS) pegasus_cpp_sample *.o

