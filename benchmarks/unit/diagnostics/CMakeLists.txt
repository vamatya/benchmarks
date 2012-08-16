# Copyright (c) 2011 Bryce Adelstein-Lelbach
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    handled_exception
    unhandled_exception
   )

set(unhandled_exception_PARAMETERS FAILURE_EXPECTED)

foreach(test ${benchmarks})
  set(sources
      ${test}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${test}_test
                     SOURCES ${sources}
                     ${${test}_FLAGS}
                     FOLDER "Benchmarks/Unit/Diagnostics")

  #add_hpx_unit_test(${test} ${${test}_PARAMETERS})  

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.unit.diagnostics.${test})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.diagnostics
                              benchmarks.unit.diagnostics.${test})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.diagnostics.${test}
                              ${test}_test_exe)
endforeach()

