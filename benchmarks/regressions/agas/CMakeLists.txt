# Copyright (c) 2007-2012 Hartmut Kaiser
# Copyright (c) 2011-2012 Bryce Adelstein-Lelbach
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    pass_by_value_id_type_action
   )

foreach(test ${benchmarks})
  set(sources
      ${test}.cpp)

  source_group("Source Files" FILES ${sources})

  set(${test}_PARAMETERS)
  # add example executable
  add_hpx_executable(${test}_test
                     SOURCES ${sources}
                     ${${test}_FLAGS}
                     FOLDER "Benchmarks/Regressions/AGAS")

#  add_hpx_regression_test(${test} ${${test}_PARAMETERS})  

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.regressions.agas.${test})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.regressions.agas
                              benchmarks.regressions.agas.${test})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.regressions.agas.${test}
                              ${test}_test_exe)
endforeach()

