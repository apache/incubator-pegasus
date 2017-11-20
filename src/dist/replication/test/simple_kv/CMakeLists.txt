set(MY_PROJ_NAME dsn.rep_tests.simple_kv)
add_definitions(-DDSN_IN_CORE)
set(DSN_IN_CORE "TRUE")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH)

set(MY_BOOST_PACKAGES system filesystem)

set(MY_PROJ_LIBS gtest
                 dsn_runtime
                 dsn_layer2_stateful_type1
                 dsn_meta_server
                 dsn.replication.clientlib
                 dsn.failure_detector.multimaster
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
