//  Copyright (c) 2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <hpx/runtime/components/component_factory.hpp>

#include <hpx/util/portable_binary_iarchive.hpp>
#include <hpx/util/portable_binary_oarchive.hpp>

#include <boost/serialization/version.hpp>
#include <boost/serialization/export.hpp>
#include "server/emptycomponent.hpp"

HPX_REGISTER_COMPONENT_MODULE();
HPX_REGISTER_MINIMAL_COMPONENT_FACTORY_EX(
    hpx::components::simple_component<
        hpx::components::server::emptycomponent>, emptycomponent,
        hpx::components::factory_disabled)

HPX_DEFINE_GET_COMPONENT_TYPE(hpx::components::server::emptycomponent)
