# Copyright (c) 2012 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    hpx_homogeneous_task_spawn)

# this is a hack for now, we will do a proper integration of component linking
# in the future
if(MSVC)
    set(dependencies
        ${dependencies}
        iostreams${HPX_POSTFIX})
else()
    set(dependencies
        ${dependencies}
        hpx_component_iostreams${HPX_POSTFIX})
endif()

foreach(benchmark ${benchmarks})
  set(sources
      ${benchmark}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${benchmark}
    SOURCES ${sources}
    ${${benchmark}_FLAGS}
    FOLDER "Benchmarks/Performance/${benchmark}"
    DEPENDENCIES ${dependencies})

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.performance.${benchmark})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.performance
                              benchmarks.performance.${benchmark})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.performance.${benchmark}
                              ${benchmark}_exe)
endforeach()


