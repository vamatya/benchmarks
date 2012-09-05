//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*The server file for the pingpong component, this contains the bulk of the 
pingpong component's code*/
#ifndef _PINGPONG_SERVER
#define _PINGPONG_SERVER
#include <hpx/hpx.hpp>
#include <hpx/hpx_fwd.hpp>
#include <boost/thread/mutex.hpp>
#include <hpx/runtime/applier/applier.hpp>
#include <hpx/runtime/actions/component_action.hpp> 
#include <hpx/runtime/components/component_type.hpp>
#include <hpx/runtime/components/server/simple_component_base.hpp>

#include <vector>
#include <string>
#include "../../statstd.hpp"


namespace hpx { namespace components { namespace server
{

using hpx::parcelset::parcel;
using hpx::applier::get_applier;
using hpx::naming::id_type;
using boost::mutex;
using boost::timed_mutex;

//export the component to hpx
class HPX_COMPONENT_EXPORT pingpong : 
    public simple_component_base<pingpong>
{
public:
    pingpong(){}
        
    //initialize the component.  We don't do this in the constructor because
    //it's easier than dealing with multiple constructor arguments when calling
    //the create() function (which is used for creating components)
    int init(uint64_t num, double ot_, id_type des){
        number = num;
        ot = ot_;
        destiny = des;
        return 0;
    }

    typedef hpx::actions::result_action3<pingpong, int, 0, uint64_t, double, 
        id_type, &pingpong::init> init_action;

    //get the elapsed time since t and divide it by 2 since we must send parcels
    //twice in order to ensure clock consistency (once from source to destination
    //and again from destination to source)
    double set_time(double t){
        double tim = (timer.elapsed()-t)*.5;
        return tim;
    }

    typedef hpx::actions::result_action1<pingpong, double, 1, double, 
        &pingpong::set_time> set_action;

    //this sends a parcel from the destination back to the source so that we use
    //the same timer that started timing to get the total latency.  when this
    //function returns, we will have sent two parcels for certain and perhaps three,
    //but only the first two latencies are measured so we do not need to worry
    //about accounting for any additional overhead
    double pong(double t){
        hpx::lcos::packaged_action<server::pingpong::set_action> future(destiny, t);
        return future.get_future().get();
    }

    typedef hpx::actions::result_action1<pingpong, double, 2, double, 
        &pingpong::pong> pong_action;

    //send the parcels from the sender to the receiver
    int ping(){
        uint64_t i = 0;
        double mean;
        string message = "Measuring intra-nodal latency by sending parcels:\n"
            "NOTE - time creating and applying packages is included, please\n"
            "compare with general_pa_analysis.";
        time.reserve(number);

        //obtain the statistical sampling of the timings
        for(i = 0; i < number; i++){
            hpx::lcos::packaged_action<server::pingpong::pong_action> 
                future(destiny,timer.elapsed());
            time.push_back(future.get_future().get());
        }

        //unlike most of the statistical benchmarks, we cannot easily obtain
        //a highly accurate mean time.  the best we can do is simply take the 
        //mean as determined by the statistical sampling
        double sum = 0;
        for(i=0;i<number;i++){
            sum+=time[i];
        }
        mean = sum/number;

        printout(time, ot, mean, message);
        return 0;
    }

    typedef hpx::actions::result_action0<pingpong, int, 3, &pingpong::ping> ping_action;

private:

    id_type destiny;
    uint64_t number;
    double ot;
    vector<double> time;
    high_resolution_timer timer;
};

}}}

HPX_REGISTER_ACTION_DECLARATION_EX(
    hpx::components::server::pingpong::wrapped_type::init_action, hpx_init_action)
HPX_REGISTER_ACTION_DECLARATION_EX(
    hpx::components::server::pingpong::wrapped_type::set_action, hpx_set_action)
HPX_REGISTER_ACTION_DECLARATION_EX(
    hpx::components::server::pingpong::wrapped_type::pong_action, hpx_pong_action)
HPX_REGISTER_ACTION_DECLARATION_EX(
    hpx::components::server::pingpong::wrapped_type::ping_action, hpx_ping_action)

#endif
