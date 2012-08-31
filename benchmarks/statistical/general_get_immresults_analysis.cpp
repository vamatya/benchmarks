//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This benchmark measures how long it takes to obtain results from a future
when requesting the result for the first time and shortly after creating the
future. As such some of the time measured may actually be time spent waiting
for the future to finish. Also, this benchmark does not support actions with
arguments, as we are measuring the time required to return the value and
passing arguments should have no effect on this time*/
#include "general_declarations.hpp"
#include "statstd.hpp"

///////////////////////////////////////////////////////////////////////////////
//still creates packaged actions
template <typename Vector, typename Package>
void create_packages(Vector& packages, uint64_t num){
    uint64_t i = 0;
    packages.reserve(num);

    for(; i < num; ++i)
        packages.push_back(new Package());
}
///////////////////////////////////////////////////////////////////////////////

//this collects the generated futures
template <typename Vector1, typename Vector2, typename Package>
void get_futures(Vector1& packages, Vector2& futures, uint64_t num, double ot){
    uint64_t i = 0;

    futures.reserve(num);
    for(i = 0; i < num; ++i){
        futures.push_back(packages[i]->get_future());
    }
}

//this test gets the average time required to get results from futures
template <typename Vector1, typename Vector2, typename Package>
double get_results1(Vector1 packages, Vector2 futures, uint64_t num, double ot){
    uint64_t i = 0;

    high_resolution_timer t;
    for(; i < num; ++i)
        futures[i].get();
    
    return t.elapsed()/num;
}

//this test takes the statistical sampling of the timing
template <typename Vector1, typename Vector2, typename Package>
void get_results2(Vector1 packages, Vector2 futures, uint64_t num, double ot, double mean){
    uint64_t i = 0;
    string message = "Measuring time required to get results from futures:";
    vector<double> time;
    time.reserve(num);

    for(i = 0; i < num; ++i){
        high_resolution_timer t1;
        futures[i].get();
        time.push_back(t1.elapsed());
    }
    printout(time, ot, mean, message);
}

///////////////////////////////////////////////////////////////////////////////

//this runs a series of tests for a plain_result_action
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t);

///////////////////////////////////////////////////////////////////////////////

//decide which action type to use
void decide_action_type(uint64_t num, int type);

//parse the argument types
void parse_arg(string atype, uint64_t num){
    int type;
    if(atype.compare("int") == 0) type = 0;
    else if(atype.compare("long") == 0) type = 1;
    else if(atype.compare("float") == 0) type = 2;
    else if(atype.compare("double") == 0) type = 3;
    else{
        std::cerr<<"Error parsing input, see help entry for a list of ";
        std::cerr<<"available types\n";
        return;
    }
    decide_action_type(num, type);
}

///////////////////////////////////////////////////////////////////////////////
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-spawned"].as<uint64_t>();
    string atype = vm["arg-type"].as<string>();
    csv = (vm.count("csv") ? true : false);
    parse_arg(atype, num);
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
                ->default_value(500000),
            "number of packaged_actions created and run")
        ("arg-type,A",
            boost::program_options::value<string>()
                ->default_value("int"),
            "specifies the argument type of the action, as well as the result "
            "type if a plain_result_action is used, to be either int, long, "
            "float, or double. This argument has no effect if the number of "
            "arguments specified is 0")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv);
}

///////////////////////////////////////////////////////////////////////////////
//decide which action type to use
void decide_action_type(uint64_t num, int type){
    if(type == 0){
        run_empty<vector<empty_packagei0*>, empty_packagei0, 
            empty_actioni0, int> (num);
    }
    else if(type == 1){
        run_empty<vector<empty_packagel0*>, empty_packagel0, 
            empty_actionl0, long> (num);
    }
    else if(type == 2){
        run_empty<vector<empty_packagef0*>, empty_packagef0, 
            empty_actionf0, float> (num);
    }
    else{
        run_empty<vector<empty_packaged0*>, empty_packaged0, 
            empty_actiond0, double> (num);
    }
}

//running the benchmark, but this is mostly overhead
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num){
    uint64_t i;
    double ot = timer_overhead(num);
    Vector packages;
    hpx::naming::id_type lid = hpx::find_here();
    high_resolution_timer t;

    //this bit down to the declaration of mean is all overhead for finding 
    //the mean time required to get results from futures
    create_packages<Vector, Package>(packages, num);
    for(i = 0; i < num; ++i){
        packages[i]->apply(lid);
    }
    vector<hpx::lcos::future<T> > futures;
    get_futures<Vector, vector<hpx::lcos::future<T> >, Package>
        (packages, futures, num, ot); 
    packages.clear();
    double mean = get_results1<Vector, vector<hpx::lcos::future<T> >, Package>
        (packages, futures, num, ot);

    //the rest is overhead for finding the statistical sampling of the timings
    create_packages<Vector, Package>(packages, num);
    for(i = 0; i < num; ++i){
        packages[i]->apply(lid);
    }
    futures.clear();
    get_futures<Vector, vector<hpx::lcos::future<T> >, Package>
        (packages, futures, num, ot); 
    packages.clear();
    get_results2<Vector, vector<hpx::lcos::future<T> >, Package>
        (packages, futures, num, ot, mean);
}
