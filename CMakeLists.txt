project (guile-dbd-odbc)

include(FindPkgConfig)

cmake_minimum_required(VERSION 3.5)

if (CMAKE_COMPILER_IS_GNUCC)
	set (CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Werror")
endif ()

pkg_search_module (GUILE REQUIRED guile-2.0)

find_library (ODBC odbc)
if (${ODBC} STREQUAL "ODBC-NOTFOUND")
	message(FATAL_ERROR "Install ODBC development package")
endif()

add_subdirectory (src)
