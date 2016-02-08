set(MY_PROJ_NAME dsn.rep_tests.simple_kv)
add_definitions(-DDSN_IN_CORE)
set(DSN_IN_CORE "TRUE")

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "../../../../core/dll/core_main.cpp")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH ../../client_lib)

set(MY_BOOST_PACKAGES system)

set(MY_PROJ_LIBS gtest
                 dsn.tools.nfs
                 dsn.dev.cpp.core.use
                 dsn.tools.hpc
                 dsn.tools.simulator
                 dsn.tools.common
                 dsn.corelib
                 dsn.replication
                 dsn.replication.meta_server
                 dsn.replication.clientlib
                 dsn.failure_detector
                 dsn.replication.zookeeper_provider
                 zookeeper_mt
)

set(MY_PROJ_LIB_PATH 
        ${GTEST_LIB_DIR}
        ${ZOOKEEPER_LIB_DIR}
        )

# Extra files that will be installed
FILE(GLOB CASE_FILES "case-*")
set(MY_BINPLACES
    "${CMAKE_CURRENT_SOURCE_DIR}/run.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/clear.sh"
    "${CMAKE_CURRENT_SOURCE_DIR}/addcase.sh"
    "${CASE_FILES}"
)

if(WIN32)
    SET(CMAKE_EXE_LINKER_FLAGS  "${CMAKE_EXE_LINKER_FLAGS} /ignore:4049 /ignore:4217")
endif()

dsn_add_executable()
add_dependencies(${MY_PROJ_NAME} googletest)
