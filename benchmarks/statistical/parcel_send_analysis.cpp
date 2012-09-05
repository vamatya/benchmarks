//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This benchmark attempts to measure the amount of time required to send a
parcel from one component to another.  This does not measure the latency between
sending the parcel and the arrival of the parcel, it only measures the overhead
of actually sending the parcel for the thread sending the parcel.*/
#include "parcelsender/sender.hpp"
#include "parcelreceiver/receiver.hpp"

using hpx::components::server::parcelsender;
using hpx::components::server::parcelreceiver;

typedef hpx::lcos::future<bool> bf;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//required to run hpx
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-sent"].as<uint64_t>();
    csv = (vm.count("csv") ? true : false);

    double ot = timer_overhead(num);
    hpx::naming::id_type sendid, receiveid;

    sendid = (hpx::applier::get_applier().get_runtime_support_gid());
    receiveid = hpx::applier::get_applier().get_runtime_support_gid();

    //declare a sending and receiving component
    parcelsender sender(num, ot, sendid, receiveid);
    parcelreceiver receiver(num, ot);

    //asynchronously begin sending all parcels
    bf sent = hpx::async<
        hpx::components::server::parcelsender::send_all_action>(sender.get_gid());
    
    //if the "simultaneous" parameter is set, make sure all parcels are sent
    //before any are received by calling sent.get()
    if(!vm.count("simultaneous")) sent.get();
    
    //asynchronously begin receiving all parcels
    bf received = hpx::async<
        hpx::components::server::parcelreceiver::receive_all_action>(
        receiver.get_gid());

    //wait until all parcels have been sent. This is only necessary because this
    //also waits for the results of the sending measurements to be calculated
    //and printed.  Otherwise simply waiting for all parcels to be received
    //(which is done below) would be sufficient for program safety
    if(vm.count("simultaneous")) sent.get();

    //wait for all parcels to be received
    received.get();

    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]){
    // Configure application-specific options.
    options_description
        desc_commandline("usage: " HPX_APPLICATION_STRING " [options]");

    desc_commandline.add_options()
        ("number-sent,N",
            boost::program_options::value<uint64_t>()
                ->default_value(500000),
            "number of parcels sent")
        ("simultaneous,S",
            "send an receive simultaneously instead of sending"
            "all parcels then receiving all")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    vector<string> cfg;
    cfg.push_back("hpx.components.parcelsender.enabled = 1");
    cfg.push_back("hpx.components.parcelreceiver.enabled = 1");
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv, cfg);
}

///////////////////////////////////////////////////////////////////////////////

