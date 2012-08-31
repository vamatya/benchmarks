//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*This benchmark measures the amount of time to create a continuation from an
action. As with all "general" benchmarks in this directory, multiple types of 
actions can be used based on command line arguments in order to see if different
action types have different overheads.*/
#include "general_declarations.hpp"
#include "statstd.hpp"

///////////////////////////////////////////////////////////////////////////////
//create the packaged_actions
template <typename Vector, typename Package>
void create_packages(Vector& packages, uint64_t num){
    uint64_t i = 0;
    packages.reserve(num);

    for(; i < num; ++i)
        packages.push_back(new Package());
}

///////////////////////////////////////////////////////////////////////////////

//this runs a series of tests for a packaged_action.apply()
template <typename Vector, typename Package, typename Action>
void run_tests(bool, uint64_t);

///////////////////////////////////////////////////////////////////////////////
//THIS IS THE ACTUAL BENCHMARK, the measurements of the amount of time required
//to create a continuation.
template<typename VectorP, typename VectorC, typename Result>
void apply_create_continuations(VectorP packages, VectorC& continuations, 
                                uint64_t num, double ot){

    //rc_type simply refers to a continuation
    typedef hpx::actions::base_lco_continuation<Result> rc_type;
    uint64_t i = 0;
    double mean;
    string message = "Measuring time required to create continuations:";
    vector<double> time;
    vector<hpx::naming::id_type> cgids;
    rc_type* temp;
    
    //first obtain the gids of the created packages
    for(; i < num; i++)
        cgids.push_back(packages[i]->get_gid());

    //measure the average time required to create a new continuation
    high_resolution_timer t;
    for(i = 0; i < num; i++)
        temp = new rc_type(cgids[i]);
    mean = t.elapsed()/num;

    cgids.clear();
    time.reserve(num);
    continuations.reserve(num);
    
    //get a statistical sampling of the creation time of continuations
    for(i = 0; i < num; ++i){
        const hpx::naming::id_type cgid = packages[i]->get_gid();
        high_resolution_timer t1;
        continuations.push_back(new rc_type(cgid));
        time.push_back(t1.elapsed());
    }
    printout(time, ot, mean, message);
}


///////////////////////////////////////////////////////////////////////////////
//decide which action type to use
void decide_action_type(uint64_t num, int type, int argc);

//parse the argument types
void parse_arg(string atype, uint64_t num, int argc){
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
    decide_action_type(num, type, argc);
}

///////////////////////////////////////////////////////////////////////////////
//required to run hpx
int hpx_main(variables_map& vm){
    uint64_t num = vm["number-spawned"].as<uint64_t>();
    string atype = vm["arg-type"].as<string>();
    int c = vm["argc"].as<int>();
    csv = (vm.count("csv") ? true : false);
    parse_arg(atype, num, c);
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
            "float, or double")
        ("argc,C",
            boost::program_options::value<int>()
                ->default_value(2),
            "the number of arguments each action takes")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    outname = argv[0];
    return hpx::init(desc_commandline, argc, argv);
}

///////////////////////////////////////////////////////////////////////////////

//decide which action type to use
void decide_action_type(uint64_t num, int type, int argc){
    if(type == 0){
        switch(argc){
            case 0: run_tests<vector<empty_packagei0*>, empty_packagei0, 
                empty_actioni0> (true, num); break;
            case 1: run_tests<vector<empty_packagei1*>, empty_packagei1, 
                empty_actioni1> (true, num); break;
            case 2: run_tests<vector<empty_packagei2*>, empty_packagei2, 
                empty_actioni2> (true, num); break;
            case 3: run_tests<vector<empty_packagei3*>, empty_packagei3, 
                empty_actioni3> (true, num); break;
            default: run_tests<vector<empty_packagei4*>, empty_packagei4, 
                empty_actioni4> (true, num); 
        }
    }
    else if(type == 1){
        switch(argc){
            case 0: run_tests<vector<empty_packagel0*>, empty_packagel0, 
                empty_actionl0> (true, num); break;
            case 1: run_tests<vector<empty_packagel1*>, empty_packagel1, 
                empty_actionl1> (true, num); break;
            case 2: run_tests<vector<empty_packagel2*>, empty_packagel2, 
                empty_actionl2> (true, num); break;
            case 3: run_tests<vector<empty_packagel3*>, empty_packagel3, 
                empty_actionl3> (true, num); break;
            default: run_tests<vector<empty_packagel4*>, empty_packagel4, 
                empty_actionl4> (true, num);
        }
    }
    else if(type == 2){
        switch(argc){
            case 0: run_tests<vector<empty_packagef0*>, empty_packagef0, 
                empty_actionf0> (true, num); break;
            case 1: run_tests<vector<empty_packagef1*>, empty_packagef1, 
                empty_actionf1> (true, num); break;
            case 2: run_tests<vector<empty_packagef2*>, empty_packagef2, 
                empty_actionf2> (true, num); break;
            case 3: run_tests<vector<empty_packagef3*>, empty_packagef3, 
                empty_actionf3> (true, num); break;
            default: run_tests<vector<empty_packagef4*>, empty_packagef4, 
                empty_actionf4> (true, num); 
        }
    }
    else{
        switch(argc){
            case 0: run_tests<vector<empty_packaged0*>, empty_packaged0, 
                empty_actiond0> (true, num); break;
            case 1: run_tests<vector<empty_packaged1*>, empty_packaged1, 
                empty_actiond1> (true, num); break;
            case 2: run_tests<vector<empty_packaged2*>, empty_packaged2, 
                empty_actiond2> (true, num); break;
            case 3: run_tests<vector<empty_packaged3*>, empty_packaged3, 
                empty_actiond3> (true, num); break;
            default: run_tests<vector<empty_packaged4*>, empty_packaged4, 
                empty_actiond4> (true, num);
        }
    }
}

//just some overhead before calling the actual benchmark
template <typename Vector, typename Package, typename Action>
void run_tests(bool empty, uint64_t num){
    double ot = timer_overhead(num);
    vector<double> time;
    Vector packages;
    create_packages<Vector, Package>(packages, num);
    hpx::naming::id_type lid = hpx::find_here();
    typedef typename hpx::actions::extract_action<Action>::type action_type;
    typedef 
        typename hpx::actions::extract_action<action_type>::result_type
        result_type;
    typedef hpx::actions::base_lco_continuation<result_type> rc_type;
    vector<rc_type*> continuations;

    //measures creation time of continuations
    apply_create_continuations<Vector, vector<rc_type*>, result_type>
        (packages, continuations, num, ot);
}
