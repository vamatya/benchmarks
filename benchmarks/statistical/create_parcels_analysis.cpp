//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/* This benchmark is used to measure the overhead associated with simply
creating parcels.*/

#include "statstd.hpp"

using hpx::parcelset::parcel;

//This is the function which we will use in our action, which in turn we will
//give to the generated parcels. The function itself does nothing.
void void_thread(){
}

//defining the action
typedef hpx::actions::plain_action0<void_thread> void_action;
typedef typename hpx::actions::extract_action<void_action>::type action_type;

//registering the action with HPX
HPX_REGISTER_PLAIN_ACTION(void_action);

/*this function actually performs the benchmark*/
void create_parcels(uint64_t num, double ot){
    uint64_t i = 0;
    parcel** parcels = new parcel*[num];

    //here we obtain a global id which specifies the parcel's destination
    hpx::naming::gid_type gid = 
        hpx::applier::get_applier().get_runtime_support_gid().get_gid();

    //this creates a pointer to the actual action which will be passed with the
    //parcel
    hpx::actions::transfer_action<action_type>* action =
        new hpx::actions::transfer_action<action_type>(
        hpx::threads::thread_priority_normal);

    //addr also must be given to the parcel to specify its destination
    hpx::naming::address addr;
    hpx::agas::is_local_address(gid, addr);
    vector<double> time;
    double mean;
    string message = "Measuring time required to create parcels:";

    //this first loop is used to measure a close approximation to the mean time
    //required to create a parcel
    high_resolution_timer t;
    for(; i < num; ++i){
        parcels[i] = new parcel(gid, addr, action);
    }
    mean = t.elapsed()/num;

    time.reserve(num);

    //this second loop obtains measurements of the approximate time required
    //to create a parcel, by measuring the creation time of a single parcel
    //many times over.
    for(i = 0; i < num; ++i){
        high_resolution_timer t1;
        parcels[i] = new parcel(gid, addr, action);
        time.push_back(t1.elapsed());
    }

    free(parcels);

    //processes the timing samples to generate statistical output
    printout(time, ot, mean, message);
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//hpx_main, required for running hpx
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-created"].as<uint64_t>();
    csv = (vm.count("csv") ? true : false);

    //measure the average overhead of using high_resolution_timers, in order to
    //remove this time from measurements and obtain higher accuracy
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

