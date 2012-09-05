//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*The core of the emptycomponent code, there is really not that much here since,
you know, it's an empty component that doesn't really do anything. It has a 
constructor and that's it*/
#include <hpx/hpx.hpp>
#include <hpx/hpx_fwd.hpp>
#include <hpx/runtime/applier/applier.hpp>
#include <hpx/runtime/actions/component_action.hpp> 
#include <hpx/runtime/components/component_type.hpp>
#include <hpx/runtime/components/server/simple_component_base.hpp>

#include "../../statstd.hpp"

namespace hpx { namespace components { namespace server
{

//declare the component so that it is known by hpx
class HPX_COMPONENT_EXPORT emptycomponent : 
    public simple_component_base<emptycomponent>
{
public:
    emptycomponent(){}
};

}}}


