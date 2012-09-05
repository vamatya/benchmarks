//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This benchmark measures the amount of time to join two threads together*/
#include "statstd.hpp"
#include <hpx/include/threadmanager.hpp>
#include <hpx/util/lightweight_test.hpp>
#include <boost/assign/std/vector.hpp>
#include <boost/lexical_cast.hpp>

//the function to be assigned to the thread.  It does nothing and therefore
//minimal extra time will be spent waiting for the thread to complete its 
//workload before attempting to join to the main thread
void void_thread(){
}

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

//this runs a series of tests for a packaged_action.apply()
void run_tests(uint64_t);

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
//necessary to run hpx
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-spawned"].as<uint64_t>();
    csv = (vm.count("csv") ? true : false);
    run_tests(num);
    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]){
    // Configure application-specific options.
    options_description
        desc_commandline("usage: " HPX_APPLICATION_STRING " [options]");

    desc_commandline.add_options()
        ("number-spawned,N",
            boost::program_options::value<uint64_t>()
                ->default_value(50000),
            "number of created and joined")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv);
}

///////////////////////////////////////////////////////////////////////////////

//measure how long it takes to spawn threads with a simple argumentless function 
void run_tests(uint64_t num){
    uint64_t i = 0;
    double ot = timer_overhead(num);
    double mean2;
    vector<double> time;
    vector<hpx::thread> threads;
    threads.reserve(2*num);

    //first create a large number of threads
    //then measure how long it takes to join them back together
    for(; i < num; ++i)
        threads.push_back(hpx::thread(&void_thread));
    for(i = 0; i < num; i++){
        threads.push_back(hpx::thread(&void_thread));
    }

    //this is here simply to ensure that all other threads have completed the
    //assigned empty function; the scheduler should have had each of the other
    //threads run before this one
    hpx::thread temp(&void_thread);
    temp.join();

    //obtain the average join time
    high_resolution_timer t;
    for(i = 0; i < num; i++){
        threads[i].join();
    }
    mean2 = t.elapsed()/num;

    //obtain a statistical sampling of the join time
    time.reserve(num);
    string message = "Measuring how long it takes to join threads:";
    for(i = num; i < num+num; ++i){
        high_resolution_timer t1;
        threads[i].join();
        time.push_back(t1.elapsed());
    }
    printout(time, ot, mean2, message);
}

