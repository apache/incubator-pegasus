include(ExternalProject)

set(target_url ${RAPIDJSON_GIT_SOURCE})
set(git_tag ${RAPIDJSON_GIT_TAG})

ExternalProject_Add(rapidjson_header
    GIT_REPOSITORY ${target_url}
    GIT_TAG ${RAPIDJSON_GIT_TAG}
    INSTALL_COMMAND ""
)

# Specify source dir
ExternalProject_Get_Property(rapidjson_header source_dir)
set(RAPIDJSON_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)
