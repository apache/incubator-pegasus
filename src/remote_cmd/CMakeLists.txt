set(MY_PROJ_NAME dsn_dist_cmd)

thrift_generate_cpp(
    COMMAND_THRIFT_SRCS
    COMMAND_THRIFT_HDRS
    ${CMAKE_CURRENT_SOURCE_DIR}/command.thrift
)

seT(MY_PROJ_SRC ${COMMAND_THRIFT_SRCS})

set(MY_SRC_SEARCH_MODE "GLOB")

dsn_add_static_library()
