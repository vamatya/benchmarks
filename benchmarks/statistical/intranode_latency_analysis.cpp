//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "pingpong/pingpong.hpp"

using hpx::components::pingpong;

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-sent"].as<uint64_t>();
    csv = (vm.count("csv") ? true : false);

    double ot = timer_overhead(num);

    hpx::naming::id_type g1, g2;
    g1 = hpx::applier::get_applier().get_runtime_support_gid();
    g2 = hpx::applier::get_applier().get_runtime_support_gid();
    pingpong sender;
    pingpong receiver;

    sender.create(g1);
    receiver.create(g2);

    sender.init(num, ot, &receiver);
    receiver.init(num, ot, &sender);

    sender.ping();
    
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
                ->default_value(500),
            "number of parcels sent")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    vector<string> cfg;
    cfg.push_back("hpx.components.pingpong.enabled = 1");
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv, cfg);
}

///////////////////////////////////////////////////////////////////////////////

