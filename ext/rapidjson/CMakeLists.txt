include(ExternalProject)

if(DSN_GIT_SOURCE STREQUAL "github")
    set(target_url "https://github.com/shengofsun/rapidjson-header.git")
elseif(DSN_GIT_SOURCE STREQUAL "xiaomi")
    set(target_url "http://git.n.xiaomi.com/pegasus/rapidjson-header.git")
else()
    message(FATAL_ERROR "not support other git source")
endif()

ExternalProject_Add(rapidjson_header
    GIT_REPOSITORY ${target_url}
    GIT_TAG master
    INSTALL_COMMAND ""
)

# Specify source dir
ExternalProject_Get_Property(rapidjson_header source_dir)
set(RAPIDJSON_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)
