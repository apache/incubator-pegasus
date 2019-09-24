set(MY_PROJ_NAME "pegasus_reporter")
project(${MY_PROJ_NAME} C CXX)

# Source files under CURRENT project directory will be automatically included.
# You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "")

# Search mode for source files under CURRENT project directory?
# "GLOB_RECURSE" for recursive search
# "GLOB" for non-recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

# prometheus
set(CMAKE_PREFIX_PATH ${DSN_THIRDPARTY_ROOT};${CMAKE_PREFIX_PATH})
find_package(prometheus-cpp)#TODO(huangwei5): make it optional
# the INTERFACE_LINK_LIBRARIES of prometheus contains the absolute path of libcurl
# when we use the compiled prometheus-cpp libs, the path of libcurl should be our own path
find_package(CURL)
get_target_property(_libs prometheus-cpp::push INTERFACE_LINK_LIBRARIES)
string(REGEX REPLACE ";/.*libcurl\.a" ";${CURL_LIBRARIES}" _libs "${_libs}")
set_target_properties(prometheus-cpp::push PROPERTIES INTERFACE_LINK_LIBRARIES "${_libs}")

dsn_add_static_library()

target_link_libraries(${MY_PROJ_NAME} PUBLIC pegasus_base
                                            prometheus-cpp::push
                                            ) # TODO(huangwei5): dsn_add_static_library doesnt link libs, need fix
