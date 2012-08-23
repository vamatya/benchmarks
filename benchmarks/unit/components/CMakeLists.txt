#  Copyright (c) 2011 Thomas Heller
#
#  Distributed under the Boost Software License, Version 1.0. (See accompanying
#  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    remote_object
   )

set(remote_object_FLAGS
    DEPENDENCIES iostreams remote_object)
set(remote_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

foreach(test ${benchmarks})
  set(sources
      ${test}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${test}_test
                     SOURCES ${sources}
                     ${${test}_FLAGS}
                     FOLDER "Benchmarks/Unit/Components")

  #add_hpx_unit_test(${test} ${${test}_PARAMETERS})  

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.unit.components.${test})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.components
                              benchmarks.unit.components.${test})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.components.${test}
                              ${test}_test_exe)
endforeach()

