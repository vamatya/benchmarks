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

//this runs a series of tests for a packaged_action.apply()
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool, uint64_t);
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool, uint64_t, T);
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool, uint64_t, T, T);
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool, uint64_t, T, T, T);
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool, uint64_t, T, T, T, T);

///////////////////////////////////////////////////////////////////////////////
//all of the measured tests are declared in this section

//base case, just call apply.  used as control to validate other results

//here measures the time it takes to create continuations
template<typename VectorP, typename VectorC, typename Result>
void apply_create_continuations(VectorP packages, VectorC& continuations, 
                                uint64_t num, double ot){
    typedef hpx::actions::base_lco_continuation<Result> rc_type;
    uint64_t i = 0;
    double mean;
    string message = "Measuring time required to create continuations:";
    vector<double> time;
    vector<hpx::naming::id_type> cgids;
    rc_type* temp;
    for(; i < num; i++)
        cgids.push_back(packages[i]->get_gid());
    high_resolution_timer t;
    for(i = 0; i < num; i++)
        temp = new rc_type(cgids[i]);
    mean = t.elapsed()/num;
    cgids.clear();
    time.reserve(num);
    continuations.reserve(num);
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
void decide_action_type(bool rtype, uint64_t num, int type, int argc){
    if(type == 0){
        switch(argc){
            case 0: run_tests<vector<empty_packagei0*>, empty_packagei0, 
                empty_actioni0, int> (true, num); break;
            case 1: run_tests<vector<empty_packagei1*>, empty_packagei1, 
                empty_actioni1, int> (true, num, ivar); break;
            case 2: run_tests<vector<empty_packagei2*>, empty_packagei2, 
                empty_actioni2, int> (true, num, ivar, ivar); break;
            case 3: run_tests<vector<empty_packagei3*>, empty_packagei3, 
                empty_actioni3, int> (true, num, ivar, ivar, ivar); break;
            default: run_tests<vector<empty_packagei4*>, empty_packagei4, 
                empty_actioni4, int> (true, num, ivar, ivar, ivar, ivar); 
        }
    }
    else if(type == 1){
        switch(argc){
            case 0: run_tests<vector<empty_packagel0*>, empty_packagel0, 
                empty_actionl0, long> (true, num); break;
            case 1: run_tests<vector<empty_packagel1*>, empty_packagel1, 
                empty_actionl1, long> (true, num, lvar); break;
            case 2: run_tests<vector<empty_packagel2*>, empty_packagel2, 
                empty_actionl2, long> (true, num, lvar, lvar); break;
            case 3: run_tests<vector<empty_packagel3*>, empty_packagel3, 
                empty_actionl3, long> (true, num, lvar, lvar, lvar); break;
            default: run_tests<vector<empty_packagel4*>, empty_packagel4, 
                empty_actionl4, long> (true, num, lvar, lvar, lvar, lvar);
        }
    }
    else if(type == 2){
        switch(argc){
            case 0: run_tests<vector<empty_packagef0*>, empty_packagef0, 
                empty_actionf0, float> (true, num); break;
            case 1: run_tests<vector<empty_packagef1*>, empty_packagef1, 
                empty_actionf1, float> (true, num, fvar); break;
            case 2: run_tests<vector<empty_packagef2*>, empty_packagef2, 
                empty_actionf2, float> (true, num, fvar, fvar); break;
            case 3: run_tests<vector<empty_packagef3*>, empty_packagef3, 
                empty_actionf3, float> (true, num, fvar, fvar, fvar); break;
            default: run_tests<vector<empty_packagef4*>, empty_packagef4, 
                empty_actionf4, float> (true, num, fvar, fvar, fvar, fvar); 
        }
    }
    else{
        switch(argc){
            case 0: run_tests<vector<empty_packaged0*>, empty_packaged0, 
                empty_actiond0, double> (true, num); break;
            case 1: run_tests<vector<empty_packaged1*>, empty_packaged1, 
                empty_actiond1, double> (true, num, dvar); break;
            case 2: run_tests<vector<empty_packaged2*>, empty_packaged2, 
                empty_actiond2, double> (true, num, dvar, dvar); break;
            case 3: run_tests<vector<empty_packaged3*>, empty_packaged3, 
                empty_actiond3, double> (true, num, dvar, dvar, dvar); break;
            default: run_tests<vector<empty_packaged4*>, empty_packaged4, 
                empty_actiond4, double> (true, num, dvar, dvar, dvar, dvar);
        }
    }
}

//this runs a series of tests for packaged_action.apply()
template <typename Vector, typename Package, typename Action, typename T>
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
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool empty, uint64_t num, T a1){
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
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool empty, uint64_t num, T a1, T a2){
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
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool empty, uint64_t num, T a1, T a2, T a3){
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
template <typename Vector, typename Package, typename Action, typename T>
void run_tests(bool empty, uint64_t num, T a1, T a2, T a3, T a4){
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
