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

# - Check for the presence of RT
#
# The following variables are set when RT is found:
#  HAVE_RT       = Set to true, if all components of RT
#                          have been found.
#  RT_INCLUDES   = Include path for the header files of RT
#  RT_LIBRARIES  = Link these to use RT

## -----------------------------------------------------------------------------
## Check for the header files

find_path (RT_INCLUDES time.h
  PATHS /usr/local/include /usr/include ${CMAKE_EXTRA_INCLUDES}
  )

## -----------------------------------------------------------------------------
## Check for the library

find_library (RT_LIBRARIES rt
  PATHS /usr/local/lib64 /usr/lib64 /lib64 ${CMAKE_EXTRA_LIBRARIES}
  )

## -----------------------------------------------------------------------------
## Actions taken when all components have been found

if (RT_INCLUDES AND RT_LIBRARIES)
  set (HAVE_RT TRUE)
else (RT_INCLUDES AND RT_LIBRARIES)
  if (NOT RT_FIND_QUIETLY)
    if (NOT RT_INCLUDES)
      message (STATUS "Unable to find RT header files!")
    endif (NOT RT_INCLUDES)
    if (NOT RT_LIBRARIES)
      message (STATUS "Unable to find RT library files!")
    endif (NOT RT_LIBRARIES)
  endif (NOT RT_FIND_QUIETLY)
endif (RT_INCLUDES AND RT_LIBRARIES)

if (HAVE_RT)
  if (NOT RT_FIND_QUIETLY)
    message (STATUS "Found components for RT")
    message (STATUS "RT_INCLUDES = ${RT_INCLUDES}")
    message (STATUS "RT_LIBRARIES = ${RT_LIBRARIES}")
  endif (NOT RT_FIND_QUIETLY)
else (HAVE_RT)
  if (RT_FIND_REQUIRED)
    message (FATAL_ERROR "Could not find RT!")
  endif (RT_FIND_REQUIRED)
endif (HAVE_RT)

mark_as_advanced (
  HAVE_RT
  RT_LIBRARIES
  RT_INCLUDES
  )