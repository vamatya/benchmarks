//  Copyright (c) 2012 Daniel Kogler
//  Copyright (c) 2007-2012 Hartmut Kaiser
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// This benchmark is used to measure the overhead associated with simply
// creating parcels. 

#include "statstd.hpp"

#include <boost/ptr_container/ptr_vector.hpp>

//This is the function which we will use in our action, which in turn we will
//give to the generated parcels. The function itself does nothing.
void void_thread()
{
}

// Registering the action with HPX.
HPX_PLAIN_ACTION(void_thread, void_thread_action_type);

// this function actually performs the benchmark
void create_parcels(uint64_t num, double ot)
{
    using hpx::parcelset::parcel;

    // Here we obtain a global id which specifies the parcel's destination.
    hpx::naming::gid_type gid = hpx::find_here().get_gid();

    // This creates the actual action which will be passed with the parcel.
    typedef hpx::actions::transfer_action<void_thread_action_type> action_type;

    // addr also must be given to the parcel to specify its destination
    hpx::naming::address addr;
    hpx::agas::is_local_address(gid, addr);

    // This first loop is used to measure a close approximation to the mean 
    // time required to create a parcel.
    double mean = 0;
    {
        boost::ptr_vector<parcel> parcels(num);
        hpx::util::high_resolution_timer t;
        for(boost::uint64_t i = 0; i < num; ++i){
            parcels.push_back(new parcel(gid, addr, new action_type));
        }
        mean = t.elapsed()/num;
    }

    // This second loop obtains measurements of the approximate time required
    // to create a parcel, by measuring the creation time of a single parcel
    // many times over.
    std::vector<double> times;
    times.reserve(num);
    {
        boost::ptr_vector<parcel> parcels(num);
        for(boost::uint64_t i = 0; i < num; ++i){
            hpx::util::high_resolution_timer t1;
            parcels.push_back(new parcel(gid, addr, new action_type));
            times.push_back(t1.elapsed());
        }
    }

    // Processes the timing samples to generate statistical output.
    printout(times, ot, mean, "Measuring time required to create parcels:");
}

///////////////////////////////////////////////////////////////////////////////
// hpx_main, required for running HPX
int hpx_main(boost::program_options::variables_map& vm)
{
    boost::uint64_t num = vm["number-created"].as<boost::uint64_t>();
    csv = (vm.count("csv") ? true : false);

    // Measure the average overhead of using high_resolution_timers, in order 
    // to remove this time from measurements and obtain higher accuracy.
    double ot = timer_overhead(num);

    create_parcels(num, ot);

    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[])
{
    // Configure application-specific options.
    namespace po = boost::program_options;
    po::options_description
        desc_commandline("usage: " HPX_APPLICATION_STRING " [options]");

    desc_commandline.add_options()
        ("number-created,N",
            po::value<boost::uint64_t>()->default_value(500000),
            "number of parcels created")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv);
}


