set(MY_PROJ_NAME dsn.replication.meta_server)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH 
../client_lib 
../../failure_detector 
${RAPIDJSON_INCLUDE_DIR})

set(MY_PROJ_LIBS "")

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_static_library()
add_dependencies(${MY_PROJ_NAME} rapidjson_header)