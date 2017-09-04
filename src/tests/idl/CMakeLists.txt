set(MY_PROJ_NAME dsn.idl.tests)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH
    ${GTEST_INCLUDE_DIR} 
)

set(MY_PROJ_LIBS gtest
)

set(MY_PROJ_LIB_PATH
    "${GTEST_LIB_DIR}"
)

set(MY_BOOST_PACKAGES system filesystem)

# Extra files that will be installed

if (UNIX)
    set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/run.sh")
endif ()

set(MY_PROJ_BINDIRS
    "${CMAKE_CURRENT_SOURCE_DIR}/resources"
)

# For Windows style path
if (WIN32)
    STRING(REGEX REPLACE "/" "\\\\" WIN_THRIFT_INCLUDE_DIR ${THRIFT_INCLUDE_DIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_THRIFT_LIB_DIR ${THRIFT_LIB_DIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_PROTOBUF_INCLUDE_DIR ${PROTOBUF_INCLUDE_DIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_PROTOBUF_LIB_DIR ${PROTOBUF_LIB_DIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_PROTOBUF_BIN_DIR ${PROTOBUF_BIN_DIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_BOOST_INCLUDE_DIR ${BOOST_INCLUDEDIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_SOURCE_TOP_DIR ${CMAKE_SOURCE_DIR})
    STRING(REGEX REPLACE "/" "\\\\" WIN_BINARY_TOP_DIR ${CMAKE_BINARY_DIR})
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/resources/sbin/mock_install.cmd.template" "${CMAKE_CURRENT_SOURCE_DIR}/resources/sbin/mock_install.cmd")
else ()
    configure_file("${CMAKE_CURRENT_SOURCE_DIR}/resources/sbin/run.sh.template" "${CMAKE_CURRENT_SOURCE_DIR}/run.sh")
endif ()

set(MY_SERIALIZATION_TYPE
    "thrift"
    "protobuf"
)

dsn_add_executable()
