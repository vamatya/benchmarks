//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This is the core of the parcelsender component source code, however without
the proper code base (server/stubs/main .hpp files) it cannot be properly treated
as a component.  Fortunately its functionality is sufficient for the purposes of
this benchmark*/
#include <hpx/hpx.hpp>
#include <hpx/hpx_fwd.hpp>
#include <hpx/runtime/applier/applier.hpp>
#include <hpx/runtime/actions/component_action.hpp> 
#include <hpx/runtime/components/component_type.hpp>
#include <hpx/runtime/components/server/simple_component_base.hpp>
#include <hpx/runtime/components/component_type.hpp>
#include <hpx/runtime/components/server/simple_component_base.hpp>

#include <vector>
#include <string>
#include "../statstd.hpp"

//just an empty function to be passed with the parcel
void void_thread(){
}
typedef hpx::actions::plain_action0<void_thread> void_action;
typedef hpx::actions::extract_action<void_action>::type action_type;

HPX_REGISTER_PLAIN_ACTION(void_action);

namespace hpx { namespace components { namespace server
{

using hpx::parcelset::parcel;
using hpx::applier::get_applier;
using hpx::naming::id_type;

class HPX_COMPONENT_EXPORT parcelsender : 
    public simple_component_base<parcelsender>
{
public:
    parcelsender(){}

    //constructor we use
    parcelsender(uint64_t num, double ot_, id_type s, id_type d) : 
        source(s), destination(d), number(num), ot(ot_){
        //create our first batch of parcels
        create_parcels();
    }
    //destructor
    ~parcelsender(){
        for(unsigned int i = 0; i < number; i++){
            parcels[i]->~parcel();
        }
        free(parcels);
    }

    //this function just sends a bunch of parcels to the destination
    //provided in the constructor
    bool send_all(){
        uint64_t i = 0;
        double mean;
        string message = "Measuring time required to send parcels:";
        vector<double> time;
        time.reserve(number);
        hpx::parcelset::parcelhandler& ph = get_applier().get_parcel_handler();

        //get the mean time of putting the parcels into the sending queue
        high_resolution_timer t;
        for(; i < number; ++i){
            ph.put_parcel(*parcels[i]);
        }
        mean = t.elapsed()/number;

        //create another batch of parcels using the same allocated memory as
        //for the first batch
        hpx::naming::address addr;
        hpx::agas::is_local_address(destination, addr);
        for(i = 0; i < number; i++){
            parcels[i]->~parcel();
            hpx::actions::transfer_action<action_type>* action = 
                new hpx::actions::transfer_action<action_type>(
                hpx::threads::thread_priority_normal);
            parcels[i] = new parcel(destination.get_gid(), addr, action);
        }
        
        //obtain the statistical sampling of the time required to put parcels
        //on the sending queue
        for(i = 0; i < number; i++){
            high_resolution_timer t1;
            ph.put_parcel(*parcels[i]);
            time.push_back(t1.elapsed());
        }
        printout(time, ot, mean, message);
        return true;
    }

    typedef hpx::actions::result_action0<parcelsender, bool, 0, 
        &parcelsender::send_all> send_all_action;
    
private:
    //create the parcels
    void create_parcels(){
        uint64_t i = 0;
        parcels = new parcel*[number];
        hpx::naming::gid_type gid = destination.get_gid();
        hpx::naming::address addr;
        hpx::agas::is_local_address(destination, addr);
        for(; i < number; ++i){
            hpx::actions::transfer_action<action_type>* action = 
                new hpx::actions::transfer_action<action_type>(
                hpx::threads::thread_priority_normal);
            parcels[i] = new parcel(gid, addr, action);
        }
    }

    parcel** parcels;
    id_type source, destination;
    uint64_t number;
    double ot;
};

}}}


