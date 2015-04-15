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
	-lboost_filesystem \
	-lboost_thread \
	-lboost_regex \
    -lboost_system \
	-lboost_chrono \
	-lboost_date_time \	
    -ldsn.tools.simulator \
    -ldsn.tools.common \
    -ldsn.dev \
    -ldsn.core  
	
SYS_LIB_PATH = \
	-L/usr/lib/x86_64-linux-gnu \
	-L/usr/local/lib

BIN_DIR = $(ROOT)
SOURCES = $(wildcard *.cpp)
OBJS = $(SOURCES:%.cpp=%.o)

CPP = g++
CPPFLAGS = -std=c++11 -I./ $(SYS_INC:%= -I%) $(INC_EXTRA:%= -I%) -O0 -g 

ifeq (/Users/$(LOGNAME), $(HOME))  # mac
	LD = g++	 
	SYS_LIBS_ALL = $(SYS_LIBS) # -lboost_thread-mt 	
else # linux	
	LD = g++ -pthread 
	SYS_LIBS_ALL = -lrt $(SYS_LIBS)
endif
LDFLAGS = -L$(BIN_DIR) $(SYS_LIBS_ALL) $(LIB_EXTRA:%= -l%)

AR = ar
ARFLAGS = rv

all: $(BIN_DIR) $(OBJS) $(LIB_EXTRA)
	echo building executable $(BIN_DIR)/$(TARGET) ...
	$(LD) -o $(BIN_DIR)/$(TARGET) $(OBJS) $(LDFLAGS) 

$(BIN_DIR):
	-mkdir $(BIN_DIR)

clean:
	-rm *.o
	-rm $(BIN_DIR)/$(TARGET)

%.o : %.cpp
	$(CPP) -c $(CPPFLAGS) $< -o $@

