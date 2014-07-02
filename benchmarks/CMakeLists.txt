# Copyright (c) 2007-2013 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(subdirs
#    network
    uts
#    performance
#    statistical
    )

foreach(subdir ${subdirs})
  add_hpx_pseudo_target(benchmarks.${subdir})
  add_subdirectory(${subdir})
  add_hpx_pseudo_dependencies(benchmarks benchmarks.${subdir})
endforeach()

