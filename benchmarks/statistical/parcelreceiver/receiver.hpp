//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This is the core of the parcelreceiver component source code, however without
the proper code base (server/stubs/main .hpp files) it cannot be properly treated
as a component.  Fortunately its functionality is sufficient for the purposes of
this benchmark*/
#include <hpx/hpx.hpp>
#include <hpx/hpx_fwd.hpp>
#include <hpx/runtime/applier/applier.hpp>
#include <hpx/runtime/actions/component_action.hpp> 
#include <hpx/runtime/components/component_type.hpp>
#include <hpx/runtime/components/server/simple_component_base.hpp>

#include <vector>
#include <string>
#include "../statstd.hpp"

namespace hpx { namespace components { namespace server
{

using hpx::applier::get_applier;
using hpx::parcelset::parcel;

//declare the component
class HPX_COMPONENT_EXPORT parcelreceiver : 
    public simple_component_base<parcelreceiver>
{
public:
    parcelreceiver(){}
    parcelreceiver(uint64_t num, double ot_):number(num), ot(ot_){}

    //This function simply receives all of the parcels which were sent by the
    //parcelsender component's function send_all()
    bool receive_all(){
        uint64_t i = 0;
        parcel p;
        hpx::parcelset::parcelhandler& ph = get_applier().get_parcel_handler();

        for(; i < number; ++i)
            ph.get_parcel(p);
        for(i = 0; i < number; i++){
            ph.get_parcel(p);
        }
        return true;
    }

    typedef hpx::actions::result_action0<parcelreceiver, bool, 0, 
        &parcelreceiver::receive_all> receive_all_action;
private:
    uint64_t number;
    double ot;
};

}}}


