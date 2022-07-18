##############################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
##############################################################################

find_path(Jemalloc_INCLUDE_DIRS
    NAMES jemalloc/jemalloc.h
    PATHS ${THIRDPARTY_INSTALL_DIR}/include
    NO_DEFAULT_PATH
)

find_library(Jemalloc_SHARED_LIBRARIES
    NAMES jemalloc
    PATHS ${THIRDPARTY_INSTALL_DIR}/lib
    NO_DEFAULT_PATH
)

find_library(Jemalloc_STATIC_LIBRARIES
    NAMES libjemalloc_pic.a
    PATHS ${THIRDPARTY_INSTALL_DIR}/lib
    NO_DEFAULT_PATH
)

if(Jemalloc_INCLUDE_DIRS AND Jemalloc_SHARED_LIBRARIES AND Jemalloc_STATIC_LIBRARIES)
    set(Jemalloc_FOUND TRUE)
else()
    set(Jemalloc_FOUND FALSE)
endif()

if(Jemalloc_FOUND)
    message(STATUS "Found jemalloc header files: ${Jemalloc_INCLUDE_DIRS}")
    message(STATUS "Found jemalloc shared libs: ${Jemalloc_SHARED_LIBRARIES}")
    message(STATUS "Found jemalloc static libs: ${Jemalloc_STATIC_LIBRARIES}")
else()
    if(Jemalloc_FIND_REQUIRED)
        message(FATAL_ERROR "Not found jemalloc in ${THIRDPARTY_INSTALL_DIR}")
    endif()
endif()

mark_as_advanced(
    Jemalloc_INCLUDE_DIRS
    Jemalloc_SHARED_LIBRARIES
    Jemalloc_STATIC_LIBRARIES
)

if(Jemalloc_FOUND AND NOT (TARGET JeMalloc::JeMalloc))
    if("${JEMALLOC_LIB_TYPE}" STREQUAL "SHARED")
        add_library(JeMalloc::JeMalloc SHARED IMPORTED)
        set_target_properties(JeMalloc::JeMalloc PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES ${Jemalloc_INCLUDE_DIRS}
            IMPORTED_LOCATION ${Jemalloc_SHARED_LIBRARIES}
        )
        message(STATUS "Use jemalloc lib type: ${JEMALLOC_LIB_TYPE}")
        message(STATUS "Use jemalloc lib: ${Jemalloc_SHARED_LIBRARIES}")
    elseif("${JEMALLOC_LIB_TYPE}" STREQUAL "STATIC")
        add_library(JeMalloc::JeMalloc STATIC IMPORTED)
        set_target_properties(JeMalloc::JeMalloc PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES ${Jemalloc_INCLUDE_DIRS}
            IMPORTED_LINK_INTERFACE_LANGUAGES "C;CXX"
            IMPORTED_LOCATION ${Jemalloc_STATIC_LIBRARIES}
        )
        message(STATUS "Use jemalloc lib type: ${JEMALLOC_LIB_TYPE}")
        message(STATUS "Use jemalloc lib: ${Jemalloc_STATIC_LIBRARIES}")
    else()
        message(FATAL_ERROR "Invalid jemalloc lib type: ${JEMALLOC_LIB_TYPE}")
    endif()
endif()
