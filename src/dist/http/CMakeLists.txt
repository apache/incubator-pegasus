set(MY_PROJ_NAME dsn_http)

set(MY_PROJ_SRC "")

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS fmt)

dsn_add_static_library()

add_subdirectory(test)
