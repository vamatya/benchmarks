# Copyright (c) 2011 Bryce Adelstein-Lelbach
# Copyright (c) 2012 Daniel Kogler
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    uts_ws
    uts_wm)

add_definitions(-DBRG_RNG)

foreach(benchmark ${benchmarks})
  set(sources
      ${benchmark}.cpp rng/brg_sha1.cpp
      )

  message(${sources})
  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${benchmark}
                     SOURCES ${sources} 
                     ${${benchmark}_FLAGS}
                     COMPONENT_DEPENDENCIES iostreams distributing_factory
                     FOLDER "Benchmarks/Uts/${benchmark}")

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.uts.${benchmark})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.uts
                              benchmarks.uts.${benchmark})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.uts.${benchmark}
                              ${benchmark}_exe)
endforeach()

