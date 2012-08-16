//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "general_declarations.hpp"
#include "statstd.hpp"

///////////////////////////////////////////////////////////////////////////////
template <typename Vector, typename Package>
void create_packages(Vector& packages, uint64_t num){
    uint64_t i = 0;
    packages.reserve(num);

    for(; i < num; ++i)
        packages.push_back(new Package());
}
///////////////////////////////////////////////////////////////////////////////

//the third test destroys the generated futures
template <typename Vector1, typename Vector2, typename Package>
void destroy_futures(uint64_t num, double ot){
    uint64_t i = 0;
    double mean;
    string message = "Measuring time required to destroy futures:";
    vector<double> time;
    time.reserve(num);
    Vector1 packages, packages2;
    Vector2 futures, futures2;
    create_packages<Vector1, Package>(packages2, num);

    for(; i < num; ++i)
        futures2.push_back(packages2[i]->get_future());
    high_resolution_timer t;
    for(i = 0; i < num; ++i)
        futures2.pop_back();
    mean = t.elapsed()/num;
    packages2.clear();
    create_packages<Vector1, Package>(packages, num);

    for(i = 0; i < num; ++i)
        futures.push_back(packages[i]->get_future());
    for(i = 0; i < num; ++i){
        high_resolution_timer t1;
        futures.pop_back();
        time.push_back(t1.elapsed());
    }
    printout(time, ot, mean, message);
}
///////////////////////////////////////////////////////////////////////////////

//this runs a series of tests for a plain_result_action
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t);
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t, T);
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t, T, T);
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t, T, T, T);
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t, T, T, T, T);

///////////////////////////////////////////////////////////////////////////////

//decide which action type to use
void decide_action_type(bool rtype, uint64_t num, int type, int argc);

//parse the argument types
void parse_arg(string atype, bool rtype, uint64_t num, int argc){
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
    decide_action_type(rtype, num, type, argc);
}

///////////////////////////////////////////////////////////////////////////////
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-spawned"].as<uint64_t>();
    string atype = vm["arg-type"].as<string>();
    int c = vm["argc"].as<int>();
    csv = (vm.count("csv") ? true : false);
    parse_arg(atype, true, num, c);
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
        ("argc,C",
            boost::program_options::value<int>()
                ->default_value(1),
            "the number of arguments each action takes")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv);
}

///////////////////////////////////////////////////////////////////////////////
//Below is purely tedium

//decide which action type to use
void decide_action_type(bool rtype, uint64_t num, int type, int argc){
    if(type == 0){
        switch(argc){
            case 0: run_empty<vector<empty_packagei0*>, empty_packagei0, 
                empty_actioni0, int> (num); break;
            case 1: run_empty<vector<empty_packagei1*>, empty_packagei1, 
                empty_actioni1, int> (num, ivar); break;
            case 2: run_empty<vector<empty_packagei2*>, empty_packagei2, 
                empty_actioni2, int> (num, ivar, ivar); break;
            case 3: run_empty<vector<empty_packagei3*>, empty_packagei3, 
                empty_actioni3, int> (num, ivar, ivar, ivar); break;
            default: run_empty<vector<empty_packagei4*>, empty_packagei4, 
                empty_actioni4, int> (num, ivar, ivar, ivar, ivar); break;
        }
    }
    else if(type == 1){
        switch(argc){
            case 0: run_empty<vector<empty_packagel0*>, empty_packagel0, 
                empty_actionl0, long> (num); break;
            case 1: run_empty<vector<empty_packagel1*>, empty_packagel1, 
                empty_actionl1, long> (num, lvar); break;
            case 2: run_empty<vector<empty_packagel2*>, empty_packagel2, 
                empty_actionl2, long> (num, lvar, lvar); break;
            case 3: run_empty<vector<empty_packagel3*>, empty_packagel3, 
                empty_actionl3, long> (num, lvar, lvar, lvar); break;
            default: run_empty<vector<empty_packagel4*>, empty_packagel4, 
                empty_actionl4, long> (num, lvar, lvar, lvar, lvar); break;
        }
    }
    else if(type == 2){
        switch(argc){
            case 0: run_empty<vector<empty_packagef0*>, empty_packagef0, 
                empty_actionf0, float> (num); break;
            case 1: run_empty<vector<empty_packagef1*>, empty_packagef1, 
                empty_actionf1, float> (num, fvar); break;
            case 2: run_empty<vector<empty_packagef2*>, empty_packagef2, 
                empty_actionf2, float> (num, fvar, fvar); break;
            case 3: run_empty<vector<empty_packagef3*>, empty_packagef3, 
                empty_actionf3, float> (num, fvar, fvar, fvar); break;
            default: run_empty<vector<empty_packagef4*>, empty_packagef4, 
                empty_actionf4, float> (num, fvar, fvar, fvar, fvar); break;
        }
    }
    else{
        switch(argc){
            case 0: run_empty<vector<empty_packaged0*>, empty_packaged0, 
                empty_actiond0, double> (num); break;
            case 1: run_empty<vector<empty_packaged1*>, empty_packaged1, 
                empty_actiond1, double> (num, dvar); break;
            case 2: run_empty<vector<empty_packaged2*>, empty_packaged2, 
                empty_actiond2, double> (num, dvar, dvar); break;
            case 3: run_empty<vector<empty_packaged3*>, empty_packaged3, 
                empty_actiond3, double> (num, dvar, dvar, dvar); break;
            default: run_empty<vector<empty_packaged4*>, empty_packaged4, 
                empty_actiond4, double> (num, dvar, dvar, dvar, dvar); break;
        }
    }
}

//this runs a series of tests for a plain_result_action
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num){
    double ot = timer_overhead(num);
    destroy_futures<Vector, vector<hpx::lcos::future<T> >, Package>(num, ot); 
}
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num, T a1){
    double ot = timer_overhead(num);
    destroy_futures<Vector, vector<hpx::lcos::future<T> >, Package>(num, ot); 
}
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num, T a1, T a2){
    double ot = timer_overhead(num);
    destroy_futures<Vector, vector<hpx::lcos::future<T> >, Package>(num, ot); 
}
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num, T a1, T a2, T a3){
    double ot = timer_overhead(num);
    destroy_futures<Vector, vector<hpx::lcos::future<T> >, Package>(num, ot); 
}
template <typename Vector, typename Package, typename Action, typename T>
void run_empty(uint64_t num, T a1, T a2, T a3, T a4){
    double ot = timer_overhead(num);
    destroy_futures<Vector, vector<hpx::lcos::future<T> >, Package>(num, ot); 
}
