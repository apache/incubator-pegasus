set(MY_PROJ_NAME dsn_http)

set(MY_PROJ_SRC ${PROJECT_SOURCE_DIR}/thirdparty/build/Source/http-parser/http_parser.c)

set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS "")

dsn_add_static_library()

add_subdirectory(test)
