set(MY_PROJ_NAME dsn_utils)

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_BOOST_LIBS Boost::system Boost::filesystem Boost::regex)

set(MY_PROJ_LIBS crypto)

# Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_shared_library()

add_subdirectory(test)
