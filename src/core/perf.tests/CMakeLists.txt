set(MY_PROJ_NAME dsn.core.perf.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "../dll/core_main.cpp")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH
    ${GTEST_INCLUDE_DIR} 
    ../core ../tools/common ../tools/simulator ../tools/hpc ../tools/nfs 
    ../../dev/cpp
    )
    
set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIBS gtest
                 dsn.dist.providers.common
                 dsn.tools.nfs
                 dsn.dev.cpp.core.use
                 dsn.tools.hpc
                 dsn.tools.simulator
                 dsn.tools.common
                 dsn.corelib
                 dsn.dev.utility
)

set(MY_PROJ_LIB_PATH "${GTEST_LIB_DIR}")

# Extra files that will be installed
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config-test.ini")

dsn_add_executable()

