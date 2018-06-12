set(MY_PROJ_NAME pegasus.rproxylib)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "../../include" "../../base")

set(MY_PROJ_LIBS "")

set(MY_PROJ_LIB_PATH "")

add_definitions(-Wno-attributes)

dsn_add_static_library()
