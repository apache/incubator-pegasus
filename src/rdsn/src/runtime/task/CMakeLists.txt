set(MY_PROJ_NAME dsn.task)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS "")

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_object()
