set(MY_PROJ_NAME dsn.core.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH
	${GTEST_INCLUDE_DIRS} 
	../core ../tools/common ../tools/simulator ../tools/nfs 
	../../dev/cpp
	)
	
set(MY_PROJ_LIBS gtest
                 dsn.corelib
				 dsn.dev.cpp.core.use
                 dsn.tools.common
                 dsn.tools.simulator
                 dsn.tools.nfs)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
	set(MY_PROJ_LIBS ${MY_PROJ_LIBS} dl)
endif()

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_executable()
