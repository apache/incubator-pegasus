<?php
require_once($argv[1]); // type.php
require_once($argv[2]); // program.php
$file_prefix = $argv[3];
?>

TARGET = <?=$_PROG->name?> 
INC_EXTRA = 
LIB_EXTRA = 
TYPE = EXE

SYS_INC = /usr/local/include /usr/include
SYS_LIBS = \
	-lpthread \
    -lstdc++ \
    -ldsn.tools.simulator \
    -ldsn.tools.common \
    -ldsn.dev \
    -ldsn.core \
	-lboost_filesystem \
	-lboost_thread \
	-lboost_regex \
    -lboost_system \
	-lboost_chrono \
	-lboost_date_time 
	
SYS_LIB_PATH = \
	-L/usr/lib/x86_64-linux-gnu \
	-L/usr/local/lib

SOURCES = $(wildcard *.cpp)
OBJS = $(SOURCES:%.cpp=%.o)

CPP = g++
CPPFLAGS = -std=c++11 -I./ $(SYS_INC:%= -I%) $(INC_EXTRA:%= -I%) -O0 -g 

ifeq (/Users/$(LOGNAME), $(HOME))  # mac
	LD = g++	 
	SYS_LIBS_ALL = $(SYS_LIBS) # -lboost_thread-mt 	
else # linux	
	LD = g++ -pthread 
	SYS_LIBS_ALL = $(SYS_LIBS) -lrt 
endif
LDFLAGS = -L./ $(SYS_LIBS_ALL) $(LIB_EXTRA:%= -l%)

AR = ar
ARFLAGS = rv

all: $(OBJS) $(LIB_EXTRA)
	echo building executable $(TARGET) ...
	$(LD) -o $(TARGET) $(OBJS) $(LDFLAGS) 

clean:
	-rm *.o
	-rm $(TARGET)

%.o : %.cpp
	$(CPP) -c $(CPPFLAGS) $< -o $@

