set(MY_PROJ_NAME dsn.core.perf.tests)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH
    ${GTEST_INCLUDE_DIR} 
    ../core ../tools/common ../tools/simulator ../tools/hpc ../tools/nfs 
    )
    
set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIBS gtest
                 dsn_runtime
)

set(MY_PROJ_LIB_PATH "${GTEST_LIB_DIR}")

# Extra files that will be installed
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config-test.ini")

dsn_add_executable()
