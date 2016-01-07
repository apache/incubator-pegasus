include(ExternalProject)

ExternalProject_Add(rapidjson_header
    GIT_REPOSITORY https://github.com/shengofsun/rapidjson-header.git
    GIT_TAG master
    INSTALL_COMMAND ""
)

# Specify source dir
ExternalProject_Get_Property(rapidjson_header source_dir)
set(RAPIDJSON_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)
