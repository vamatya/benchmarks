# Copyright (c) 2011 Bryce Adelstein-Lelbach
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    set_thread_state
    thread
    thread_id
    thread_launching
    thread_mf
    thread_rescheduling
   )

if(HPX_CXX11_LAMBDAS)
  set(benchmarks ${benchmarks} thread_affinity)
endif()

set(thread_affinity_FLAGS
    DEPENDENCIES iostreams)

set(set_thread_state_PARAMETERS THREADS_PER_LOCALITY 4)

set(thread_PARAMETERS THREADS_PER_LOCALITY 4)

set(thread_affinity_FLAGS
    DEPENDENCIES iostreams)
set(thread_affinity_PARAMETERS THREADS_PER_LOCALITY 4)

set(thread_id_PARAMETERS THREADS_PER_LOCALITY 4)

set(thread_launching_PARAMETERS THREADS_PER_LOCALITY 4)

set(thread_mf_PARAMETERS THREADS_PER_LOCALITY 4)

set(thread_rescheduling_PARAMETERS THREADS_PER_LOCALITY 4)

foreach(test ${benchmarks})
  set(sources
      ${test}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${test}_test
                     SOURCES ${sources}
                     ${${test}_FLAGS}
                     FOLDER "Benchmarks/Unit/Threads")

  #add_hpx_unit_test(${test} ${${test}_PARAMETERS})  

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.unit.threads.${test})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.threads
                              benchmarks.unit.threads.${test})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.threads.${test}
                              ${test}_test_exe)
endforeach()

