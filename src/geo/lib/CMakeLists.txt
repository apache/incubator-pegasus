set(MY_PROJ_NAME pegasus_geo_lib)
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_INC_PATH "")

set(MY_PROJ_LIBS ""
        # s2
        # pegasus_client_static
        # fmt
        )

set(MY_BOOST_LIBS Boost::system Boost::filesystem)

dsn_add_static_library()

target_link_libraries(${MY_PROJ_NAME} PRIVATE pegasus_base) # TODO(huangwei5): dsn_add_static_library doesnt link libs, need fix
