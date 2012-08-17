# Copyright (c) 2011 Bryce Adelstein-Lelbach
# Copyright (c) 2012 Daniel Kogler
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

add_hpx_component(sender
    SOURCES "parcelsender/sender.cpp"
    FOLDER "Benchmarks/Statistical/parcel_sender")

add_hpx_component(receiver
    SOURCES "parcelreceiver/receiver.cpp"
    FOLDER "Benchmarks/Statistical/parcel_receiver")

add_hpx_component(empty
    SOURCES "emptycomponent/emptycomponent.cpp"
    FOLDER "Benchmarks/Statistical/empty_component")

add_hpx_component(pingpong
    SOURCES "pingpong/pingpong.cpp"
    FOLDER "Benchmarks/Statistical/pingpong")

set(benchmarks
#    create_comps_analysis
    create_parcels_analysis
#    free_comps_analysis
    general_apply_analysis
    general_apply_dir_analysis
    general_create_cont_analysis
#    general_dataflow_analysis
    general_destroy_futures_analysis
    general_get_futures_analysis
    general_get_immresults_analysis
    general_get_oldresults_analysis
    general_pa_create_analysis
    get_base_gid_analysis
    get_gid_analysis
    intranode_latency_analysis
    join_threads_analysis
    parcel_send_analysis
    spawn_threads_analysis
#    thread_cleanup_analysis
   )

#set(create_comps_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams empty)
set(create_parcels_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
#set(free_comps_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams empty)
set(general_apply_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(general_apply_dir_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(general_create_cont_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
#set(general_dataflow_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams dataflow)
set(general_destroy_futures_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(general_get_futures_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(general_get_immresults_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(general_get_oldresults_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(general_pa_create_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(get_gid_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(get_base_gid_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(intranode_latency_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams pingpong)
set(join_threads_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
set(parcel_send_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams sender receiver)
set(spawn_threads_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)
#set(thread_cleanup_analysis_FLAGS COMPONENT_DEPENDENCIES iostreams)

foreach(benchmark ${benchmarks})
  set(sources
      ${benchmark}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${benchmark}
                     SOURCES ${sources}
                     ${${benchmark}_FLAGS}
                     FOLDER "Benchmarks/Statistical/${benchmark}")

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.statistical.${benchmark})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.statistical
                              benchmarks.statistical.${benchmark})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.statistical.${benchmark}
                              ${benchmark}_exe)
endforeach()

#set_target_properties(general_dataflow_analysis_exe
#    PROPERTIES COMPILE_FLAGS
#    "-DHPX_ACTION_ARGUMENT_LIMIT=10 -DHPX_COMPONENT_CREATE_ARGUMENT_LIMIT=10 -DHPX_FUNCTION_ARGUMENT_LIMIT=13")

