//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This benchmark attempts to measure the overhead involved in context switching
between hpx threads.*/
#include <hpx/include/threadmanager.hpp>
#include <hpx/util/lightweight_test.hpp>
#include <boost/lexical_cast.hpp>

#include <vector>
#include <string>
#include <hpx/hpx_init.hpp>
#include <hpx/include/lcos.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/iostreams.hpp>
#include <hpx/include/components.hpp>
#include <hpx/util/high_resolution_timer.hpp>

using boost::program_options::variables_map;
using boost::program_options::options_description;
using boost::uint64_t;
using hpx::util::high_resolution_timer;

//simply a function which will run for a long time
void loop_function(uint64_t iters){
    volatile double bigcount = iters;
    for(uint64_t i = 0; i < iters; i++){
        bigcount*=2;
        bigcount*=.5;
        if(i % 10000 == 0)
            bigcount += 3;
    }
}

//this function runs for a long time but suspends itself regularly, which
//forces a context switch
void loop_function2(uint64_t iters){
    volatile double bigcount = iters;
    for(uint64_t i = 0; i < iters; i++){
        bigcount*=2;
        bigcount*=.5;
        if(i % 10000 == 0)
            hpx::this_thread::suspend();
    }
}
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

//this runs a series of tests for a packaged_action.apply()
void run_tests(uint64_t, int, int);

///////////////////////////////////////////////////////////////////////////////

///////////////////////////////////////////////////////////////////////////////
//required to run hpx
int hpx_main(variables_map& vm){
    uint64_t num = vm["iterations"].as<uint64_t>();
    int threads = vm["hpx-threads"].as<int>();
    int os;
    if(!vm.count("hpx:threads")) os = 1;
    else os = atoi(vm["hpx:threads"].as<std::string>().c_str());
    run_tests(num, threads, os);
    return hpx::finalize();
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]){
    // Configure application-specific options.
    options_description
        desc_commandline("usage: " HPX_APPLICATION_STRING " [options]");

    desc_commandline.add_options()
        ("iterations,N",
            boost::program_options::value<uint64_t>()
                ->default_value(500000000),
            "number of iterations the loop function will iterate over")
        ("hpx-threads,H",
            boost::program_options::value<int>()->default_value(2),
            "number of simultaneous hpx threads running");

    // Initialize and run HPX
    return hpx::init(desc_commandline, argc, argv);
}

///////////////////////////////////////////////////////////////////////////////

//measure how long it takes to spawn threads with a simple argumentless function 
void run_tests(uint64_t num, int threads, int os){
    double time1, time2;
    int i;
    std::vector<hpx::thread> funcs;

    printf("\nNOTE: for now, this benchmark is only intended for the case where \n"
           "        the number of OS threads is <= the number of cores available.\n\n");

    //measure the amount of time loop_function runs for without context switching
    high_resolution_timer t;
    loop_function(num);
    time1 = t.elapsed();

    printf("Executing the function once sequentially yields a total time of "
        "%f s\n", time1);

    t.restart();

    //create multiple threads running loop_function2, with context switching
    for(i = 0; i < threads; ++i)
        funcs.push_back(hpx::thread(&loop_function2, num));

    //wait for these threads to complete
    for(i = 0; i < threads; ++i)
        funcs[i].join();

    //obtain the effective time of loop_function2, which should be the
    //time of loop_function + the overhead of context switching
    time2 = t.elapsed()*std::min(threads,os)/threads;

    printf("Executing the function %d times simultaneously on %d cores yields "
        "an average time of %f s\n\n", threads, os, time2);

    //we know the approximate number of context switches because we force them
    //to occur manually at every 10000 iterations of the loop
    printf("Total number of context switches per thread is approximately %ld\n",
        num/10000);
    printf("Estimated time spent context switching is %f s\n", time2-time1);
    printf("Estimated percentage of time spent context switching is %f %%\n\n",
        (1-time1/(time2))*100);

    printf("Calculated mean time per context switch is %f ns\n\n",
        (time2-time1)*1e9/num*10000);
}

