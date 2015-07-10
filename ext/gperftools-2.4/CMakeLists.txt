set(DSN_RECURSIVE_SRC FALSE)
set(DSN_EXTRA_INCLUDEDIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(DIRS ${BASE_DIR})

foreach(SD in "base;google;gperftools;third_party")
	set(DIRS ${DIRS} "${BASE_DIR}/${SD}")
endforeach()

if(MSVC)
	set(DIRS ${DIRS} "${BASE_DIR}/windows")
endif()

set(DSN_EXTRA_SRC "")

foreach(DIR in ${DIRS})
	file("GLOB"
		TEMP_SRC
		"${DIR}/*.cc"
		"${DIR}/*.cpp"
		"${DIR}/*.c"
		"${DIR}/*.h"
		"${DIR}/*.hpp"
		)
	set(DSN_EXTRA_SRC ${DSN_EXTRA_SRC} ${TEMP_SRC})
endforeach()


dsn_add_library(gperftools)
