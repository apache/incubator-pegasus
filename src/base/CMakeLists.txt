set(MY_PROJ_NAME "pegasus.base")
set(DSN_IN_CORE "TRUE")

if(UNIX)
    add_compile_options(-fPIC)
endif()

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_SRC "")

set(MY_PROJ_INC_PATH "../include")

set(MY_PROJ_LIB_PATH "")

dsn_add_static_library()
