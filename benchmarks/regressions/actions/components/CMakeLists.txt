# Copyright (c) 2007-2012 Hartmut Kaiser
#
# Distributed under the Boost Software License, Version 1.0. (See accompanying
# file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

add_hpx_component(action_move_semantics
  SOURCES "action_move_semantics.cpp"
  FOLDER "Benchmarks/Regressions/Actions"
  AUTOGLOB)

add_hpx_pseudo_dependencies(benchmarks.regressions.actions action_move_semantics)

