//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This benchmark also obtains results from futures, but these results have been
obtained previously and the time required is less than for obtaining the results
for the first time. This may be purely because we no longer need to wait for the
future to complete before obtaining the result, but whether that is the case or
not this benchmark is good for knowing how efficient the reuse of a future's 
result is.*/
#include "general_declarations.hpp"
#include "statstd.hpp"

///////////////////////////////////////////////////////////////////////////////
//creating packaged_actions
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

//The actual benchmark is here
template <typename Vector1, typename Vector2, typename Package>
void get_results(Vector1 packages, Vector2 futures, uint64_t num, double ot){
    uint64_t i = 0;
    double mean;
    string message = "Measuring time required to get results from futures:";
    vector<double> time;
    time.reserve(num);

    //first we ensure that we have already obtained the results from the futures
    for(i = 0; i < num; ++i) futures[i].get();

    //now we measure the approximate mean time for obtaining the results again
    high_resolution_timer t;
    for(i = 0; i < num; ++i)
        futures[i].get();
    mean = t.elapsed()/num;

    //here we take a statistical sampling of that timing
    for(i = 0; i < num; ++i){
        high_resolution_timer t1;
        futures[i].get();
        time.push_back(t1.elapsed());
    }
    printout(time, ot, mean, message);
}

///////////////////////////////////////////////////////////////////////////////

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
//required to run hpx
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

//this is all overhead preparing for running the benchmark
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num){
    uint64_t i;
    double ot = timer_overhead(num);
    Vector packages;
    hpx::naming::id_type lid = hpx::find_here();
    high_resolution_timer t;
    create_packages<Vector, Package>(packages, num);
    for(i = 0; i < num; ++i){
        packages[i]->apply(lid);
    }
    vector<hpx::lcos::future<T> > futures;
    get_futures<Vector, vector<hpx::lcos::future<T> >, Package>
        (packages, futures, num, ot); 
    packages.clear();
    get_results<Vector, vector<hpx::lcos::future<T> >, Package>
        (packages, futures, num, ot);
}
