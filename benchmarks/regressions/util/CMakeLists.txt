# Copyright (c) 2007-2012 Hartmut Kaiser
# Copyright (c) 2011 Bryce Adelstein-Lelbach
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    function_argument
   )

set(function_argument_FLAGS DEPENDENCIES iostreams)

foreach(test ${benchmarks})
  set(sources
      ${test}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${test}_test
                     SOURCES ${sources}
                     ${${test}_FLAGS}
                     FOLDER "Benchmarks/Regressions/Util")

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.regressions.util.${test})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.regressions.util
                              benchmarks.regressions.util.${test})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.regressions.util.${test}
                              ${test}_test_exe)
endforeach()

