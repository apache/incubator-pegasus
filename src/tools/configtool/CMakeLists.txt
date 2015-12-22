set(MY_PROJ_NAME dsn.tools.configtool)
add_definitions(-DDSN_IN_CORE)
set(DSN_IN_CORE "TRUE")
# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")


set(MY_PROJ_INC_PATH "")


#set(MY_PROJ_LIB_PATH "")
set(MY_PROJ_LIBS gtest 
                 dsn.corelib
                 dsn.dev.cpp.core.use)

# Extra files that will be installed

dsn_add_executable()
