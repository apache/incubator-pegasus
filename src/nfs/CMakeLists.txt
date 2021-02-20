set(MY_PROJ_NAME dsn_nfs)

thrift_generate_cpp(
    NFS_THRIFT_SRCS
    NFS_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/nfs.thrift
)

set(MY_PROJ_SRC ${NFS_THRIFT_SRCS})

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS dsn_aio)

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_static_library()

add_subdirectory(test)
