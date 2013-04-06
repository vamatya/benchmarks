# Copyright (c) 2011 Bryce Adelstein-Lelbach
# Copyright (c) 2012 Daniel Kogler
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

set(benchmarks
    osu_bibw
    osu_bw
    osu_latency)

foreach(benchmark ${benchmarks})
  set(sources
      ${benchmark}.cpp)

  source_group("Source Files" FILES ${sources})

  # add example executable
  add_hpx_executable(${benchmark}
                     SOURCES ${sources}
                     ${${benchmark}_FLAGS}
                     COMPONENT_DEPENDENCIES iostreams
                     FOLDER "Benchmarks/Network/${benchmark}")

  # add a custom target for this example
  add_hpx_pseudo_target(benchmarks.network.${benchmark})

  # make pseudo-targets depend on master pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.network
                              benchmarks.network.${benchmark})

  # add dependencies to pseudo-target
  add_hpx_pseudo_dependencies(benchmarks.network.${benchmark}
                              ${benchmark}_exe)
endforeach()

