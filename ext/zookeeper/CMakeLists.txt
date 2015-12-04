include(ExternalProject)

#if(NOT WIN32)
#    ExternalProject_Add(zookeeper
#        GIT_REPOSITORY https://github.com/shengofsun/zookeeper_client_c.git
#        GIT_TAG master
#        BINARY_DIR "${PROJECT_BINARY_DIR}/zookeeper"
#        INSTALL_DIR "${PROJECT_BINARY_DIR}/lib"
#        INSTALL_COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_BINARY_DIR}/zookeeper/libzookeeper_mt.a ${PROJECT_BINARY_DIR}/lib
#    )
#endif()

ExternalProject_Add(zookeeper
		GIT_REPOSITORY https://github.com/imzhenyu/zookeeper_client_c.git
		GIT_TAG master
	   #   CMAKE_ARGS -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_DEBUG:PATH=DebugLibs
	   #            -DCMAKE_ARCHIVE_OUTPUT_DIRECTORY_RELEASE:PATH=ReleaseLibs
	   #            -DCMAKE_CXX_FLAGS=${MSVC_COMPILER_DEFS}
		BINARY_DIR "${PROJECT_BINARY_DIR}/lib"
		# disable install
		INSTALL_COMMAND ""
	)

# Specify include dir
ExternalProject_Get_Property(zookeeper source_dir)
set(ZOOKEEPER_INCLUDE_DIR ${source_dir}/include PARENT_SCOPE)

# Specify MainTest's link libraries
ExternalProject_Get_Property(zookeeper binary_dir)
set(ZOOKEEPER_LIB_DIR ${binary_dir} PARENT_SCOPE)

#install(FILES "${PROJECT_BINARY_DIR}/zookeeper/libzookeeper_mt.a" DESTINATION lib)
