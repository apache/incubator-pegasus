set(MY_PROJ_NAME dsn.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

message(STATUS "GTEST_INCLUDE_DIRS = ${GTEST_INCLUDE_DIRS}")
set(MY_PROJ_INC_PATH
	${GTEST_INCLUDE_DIRS} 
	../core/coreapi ../core/tools/common ../core/tools/simulator ../core/tools/nfs 
	../dist/failure_detector ../apps/replication/client_lib ../apps/replication/lib ../apps/replication/meta_server
	)

set(MY_PROJ_LIBS gtest_main gtest)

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_executable()
