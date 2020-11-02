# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

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

