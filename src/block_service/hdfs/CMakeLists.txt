set(MY_PROJ_NAME dsn.block_service.hdfs)

set(DIRECTIO_SRC
        ../directio_writable_file.cpp
        )

#Source files under CURRENT project directory will be automatically included.
#You can manually set MY_PROJ_SRC to include source files under other directories.
set(MY_PROJ_SRC "${DIRECTIO_SRC}")

#Search mode for source files under CURRENT project directory ?
#"GLOB_RECURSE" for recursive search
#"GLOB" for non - recursive search
set(MY_SRC_SEARCH_MODE "GLOB")

set(MY_PROJ_LIBS hdfs)

#Extra files that will be installed
set(MY_BINPLACES "")

dsn_add_static_library()

target_link_libraries(${MY_PROJ_NAME} PUBLIC hdfs ${JAVA_JVM_LIBRARY})
