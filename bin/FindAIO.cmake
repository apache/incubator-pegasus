# Copyright (c) 2010-2011, Rob Jansen

# To the extent that a federal employee is an author of a portion of
# this software or a derivative work thereof, no copyright is claimed by
# the United States Government, as represented by the Secretary of the
# Navy ("GOVERNMENT") under Title 17, U.S. Code. All Other Rights 
# Reserved.

# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met:
#     * Redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above
# copyright notice, this list of conditions and the following disclaimer
# in the documentation and/or other materials provided with the
# distribution.
#     * Neither the names of the copyright owners nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.

# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
# GOVERNMENT ALLOWS FREE USE OF THIS SOFTWARE IN ITS "AS IS" CONDITION
# AND DISCLAIMS ANY LIABILITY OF ANY KIND FOR ANY DAMAGES WHATSOEVER
# RESULTING FROM THE USE OF THIS SOFTWARE.

# - Check for the presence of AIO
#
# The following variables are set when AIO is found:
#  HAVE_AIO       = Set to true, if all components of AIO
#                          have been found.
#  AIO_INCLUDES   = Include path for the header files of AIO
#  AIO_LIBRARIES  = Link these to use AIO

## -----------------------------------------------------------------------------
## Check for the header files

find_path (AIO_INCLUDES libaio.h
  PATHS /usr/local/include /usr/include ${CMAKE_EXTRA_INCLUDES}
  )

## -----------------------------------------------------------------------------
## Check for the library

find_library (AIO_LIBRARIES aio
  PATHS /usr/local/lib64 /usr/lib64 /lib64 ${CMAKE_EXTRA_LIBRARIES}
  )

## -----------------------------------------------------------------------------
## Actions taken when all components have been found

if (AIO_INCLUDES AND AIO_LIBRARIES)
  set (HAVE_AIO TRUE)
else (AIO_INCLUDES AND AIO_LIBRARIES)
  if (NOT AIO_FIND_QUIETLY)
    if (NOT AIO_INCLUDES)
      message (STATUS "Unable to find AIO header files!")
    endif (NOT AIO_INCLUDES)
    if (NOT AIO_LIBRARIES)
      message (STATUS "Unable to find AIO library files!")
    endif (NOT AIO_LIBRARIES)
  endif (NOT AIO_FIND_QUIETLY)
endif (AIO_INCLUDES AND AIO_LIBRARIES)

if (HAVE_AIO)
  if (NOT AIO_FIND_QUIETLY)
    message (STATUS "Found components for AIO")
    message (STATUS "AIO_INCLUDES = ${AIO_INCLUDES}")
    message (STATUS "AIO_LIBRARIES = ${AIO_LIBRARIES}")
  endif (NOT AIO_FIND_QUIETLY)
else (HAVE_AIO)
  if (AIO_FIND_REQUIRED)
    message (FATAL_ERROR "Could not find AIO!")
  endif (AIO_FIND_REQUIRED)
endif (HAVE_AIO)

mark_as_advanced (
  HAVE_AIO
  AIO_LIBRARIES
  AIO_INCLUDES
  )