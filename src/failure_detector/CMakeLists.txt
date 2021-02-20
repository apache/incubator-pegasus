set(MY_PROJ_NAME dsn.failure_detector)

thrift_generate_cpp(
    FD_THRIFT_SRCS
    FD_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/fd.thrift
)

seT(MY_PROJ_SRC ${FD_THRIFT_SRCS})


# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_static_library()

add_subdirectory(test)

