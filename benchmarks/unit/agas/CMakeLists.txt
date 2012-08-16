# Copyright (c) 2011 Bryce Adelstein-Lelbach
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

add_subdirectory(components)

set(benchmarks
#    scoped_ref_to_local_object
#    scoped_ref_to_remote_object
#    local_embedded_ref_to_local_object
#    local_embedded_ref_to_remote_object
#    remote_embedded_ref_to_local_object
#    remote_embedded_ref_to_remote_object
#    refcnted_symbol_to_local_object
#    refcnted_symbol_to_remote_object
#    uncounted_symbol_to_local_object
#    uncounted_symbol_to_remote_object
    gid_type
#    credit_exhaustion
#    split_credit
   )

set(scoped_ref_to_local_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(scoped_ref_to_local_object_PARAMETERS
    THREADS_PER_LOCALITY 4)

set(scoped_ref_to_remote_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(scoped_ref_to_remote_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(local_embedded_ref_to_local_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(local_embedded_ref_to_local_object_PARAMETERS
    THREADS_PER_LOCALITY 4)

set(local_embedded_ref_to_remote_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(local_embedded_ref_to_remote_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(remote_embedded_ref_to_local_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(remote_embedded_ref_to_local_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(remote_embedded_ref_to_remote_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(remote_embedded_ref_to_remote_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(refcnted_symbol_to_local_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(refcnted_symbol_to_local_object_PARAMETERS
    THREADS_PER_LOCALITY 4)

set(refcnted_symbol_to_remote_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(refcnted_symbol_to_remote_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(uncounted_symbol_to_local_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(uncounted_symbol_to_local_object_PARAMETERS
    THREADS_PER_LOCALITY 4)

set(uncounted_symbol_to_remote_object_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(uncounted_symbol_to_remote_object_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(credit_exhaustion_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(credit_exhaustion_PARAMETERS
    LOCALITIES 2
    THREADS_PER_LOCALITY 2)

set(split_credit_FLAGS
    COMPONENT_DEPENDENCIES simple_refcnt_checker
                 managed_refcnt_checker)
set(split_credit_PARAMETERS
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
                     FOLDER "Benchmarks/Unit/AGAS")

#  add_hpx_unit_test(${test} ${${test}_PARAMETERS})  

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.unit.agas.${test})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.agas
                              benchmarks.unit.agas.${test})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.unit.agas.${test}
                              ${test}_test_exe)
endforeach()

