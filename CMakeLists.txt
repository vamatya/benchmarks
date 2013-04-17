# Copyright (c) 2007-2012 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

project(hpx_benchmarks CXX)

cmake_minimum_required(VERSION 2.8)
cmake_policy(SET CMP0015 NEW)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${CMAKE_CURRENT_SOURCE_DIR}/cmake)

################################################################################
# enable solution folders for MSVC
if(MSVC)
  set_property(GLOBAL PROPERTY USE_FOLDERS ON)
endif()

################################################################################
# Find and include HPX settings
if(NOT HPX_ROOT)
  if(NOT $ENV{HPX_ROOT} STREQUAL "")
      set(HPX_ROOT $ENV{HPX_ROOT})
  endif()
endif()
if(NOT HPX_ROOT)
  message(FATAL_ERROR "HPX_ROOT not set, unable to find hpx!")
endif()

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${HPX_ROOT}/share/cmake-${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}/Modules)

find_package(HPX REQUIRED)

################################################################################
include_directories(${CMAKE_SOURCE_DIR})
include_directories(${HPX_INCLUDE_DIR})
link_directories(${HPX_LIBRARY_DIR})

################################################################################
# just recurse into the benchmark subdirectory and execute the scripts there
add_hpx_pseudo_target(benchmarks)
add_subdirectory(benchmarks)

