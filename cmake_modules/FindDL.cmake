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

# - Check for the presence of DL
#
# The following variables are set when DL is found:
#  HAVE_DL       = Set to true, if all components of DL
#                          have been found.
#  DL_INCLUDES   = Include path for the header files of DL
#  DL_LIBRARIES  = Link these to use DL

## -----------------------------------------------------------------------------
## Check for the header files

find_path (DL_INCLUDES dlfcn.h
  PATHS /usr/local/include /usr/include ${CMAKE_EXTRA_INCLUDES}
  )

## -----------------------------------------------------------------------------
## Check for the library

find_library (DL_LIBRARIES dl
  PATHS /usr/local/lib64 /usr/lib64 /lib64 ${CMAKE_EXTRA_LIBRARIES}
  )

## -----------------------------------------------------------------------------
## Actions taken when all components have been found

if (DL_INCLUDES AND DL_LIBRARIES)
  set (HAVE_DL TRUE)
else (DL_INCLUDES AND DL_LIBRARIES)
  if (NOT DL_FIND_QUIETLY)
    if (NOT DL_INCLUDES)
      message (STATUS "Unable to find DL header files!")
    endif (NOT DL_INCLUDES)
    if (NOT DL_LIBRARIES)
      message (STATUS "Unable to find DL library files!")
    endif (NOT DL_LIBRARIES)
  endif (NOT DL_FIND_QUIETLY)
endif (DL_INCLUDES AND DL_LIBRARIES)

if (HAVE_DL)
  if (NOT DL_FIND_QUIETLY)
    message (STATUS "Found components for DL")
    message (STATUS "DL_INCLUDES = ${DL_INCLUDES}")
    message (STATUS "DL_LIBRARIES = ${DL_LIBRARIES}")
  endif (NOT DL_FIND_QUIETLY)
else (HAVE_DL)
  if (DL_FIND_REQUIRED)
    message (FATAL_ERROR "Could not find DL!")
  endif (DL_FIND_REQUIRED)
endif (HAVE_DL)

mark_as_advanced (
  HAVE_DL
  DL_LIBRARIES
  DL_INCLUDES
  )