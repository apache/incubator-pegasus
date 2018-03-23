# pegasus client depends on pthread, rt, dl, aio, boost_system boost_filesystem for linkage,
# no matter the shared or static library
# please make sure to install these depedencies properly
CXX = g++
CXXFLAGS = -std=c++11 -g
INCLUDE = -I../include
LIBRARY = -Wl,-rpath-link,../lib -L../lib -L../../DSN_ROOT/lib -lpegasus_client_static -lcrypto -lpthread -lrt -ldl -laio -lboost_system -lboost_filesystem
CPPS = $(wildcard vad_stdaln/*.cpp) $(wildcard ./*.cpp)
OBJS = $(patsubst %.cpp,%.o,$(CPPS))

all: pegasus_cpp_sample

%.o: %.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDE) -c -o $@ $<

pegasus_cpp_sample: $(OBJS)
	$(CXX) -o $@ $(OBJS) $(LIBRARY)

clean:
	rm -f $(OBJS) pegasus_cpp_sample *.o

