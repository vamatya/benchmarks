//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "statstd.hpp"

using hpx::parcelset::parcel;

void void_thread(){
}
typedef hpx::actions::plain_action0<void_thread> void_action;
typedef hpx::lcos::packaged_action<void_action> Package;
typedef typename hpx::actions::extract_action<void_action>::type action_type;

HPX_REGISTER_PLAIN_ACTION(void_action);

void create_parcels(uint64_t num, double ot){
    uint64_t i = 0;
    parcel** parcels = new parcel*[num];
    hpx::naming::gid_type gid = 
        hpx::applier::get_applier().get_runtime_support_gid().get_gid();
    hpx::actions::transfer_action<action_type>* action =
        new hpx::actions::transfer_action<action_type>(
        hpx::threads::thread_priority_normal);
    hpx::naming::address addr;
    hpx::agas::is_local_address(gid, addr);
    vector<double> time;
    double mean;
    string message = "Measuring time required to create parcels:";

    high_resolution_timer t;
    for(; i < num; ++i){
        parcels[i] = new parcel(gid, addr, action);
    }
    mean = t.elapsed()/num;

    time.reserve(num);

    for(i = 0; i < num; ++i){
        high_resolution_timer t1;
        parcels[i] = new parcel(gid, addr, action);
        time.push_back(t1.elapsed());
    }

    free(parcels);
    printout(time, ot, mean, message);
}



///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-created"].as<uint64_t>();
    csv = (vm.count("csv") ? true : false);

    double ot = timer_overhead(num);

    create_parcels(num, ot);

    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]){
    // Configure application-specific options.
    options_description
        desc_commandline("usage: " HPX_APPLICATION_STRING " [options]");

    desc_commandline.add_options()
        ("number-created,N",
            boost::program_options::value<uint64_t>()
                ->default_value(500000),
            "number of parcels created")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv);
}

///////////////////////////////////////////////////////////////////////////////

