set(MY_PROJ_NAME dsn.cli)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

if (UNIX)
    set(MY_PROJ_LIBS pthread)
else()
    set(MY_PROJ_LIBS "")
endif()

set(MY_PROJ_LIB_PATH "")

# Extra files that will be installed
set(MY_BINPLACES "${CMAKE_CURRENT_SOURCE_DIR}/config.ini")

dsn_add_executable()
