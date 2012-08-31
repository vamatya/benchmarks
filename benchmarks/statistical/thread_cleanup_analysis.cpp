//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include "statstd.hpp"

void void_thread(){
}

typedef hpx::actions::plain_action0<void_thread> void_action0;
typedef hpx::lcos::packaged_action<void_action0> void_package0;
HPX_REGISTER_PLAIN_ACTION(void_action0);

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
template <typename Vector, typename Package>
void create_packages(Vector& packages, uint64_t num){
    uint64_t i = 0;
    packages.reserve(num);

    for(; i < num; ++i)
        packages.push_back(new Package());
}

///////////////////////////////////////////////////////////////////////////////

//this runs a test of checking if a gid is local
template <typename Vector, typename Package, typename Action>
void run_tests(uint64_t);

namespace hpx { namespace threads {
    std::ptrdiff_t get_default_stack_size(){
        return get_runtime().get_config().get_default_stack_size();
    }
}}

///////////////////////////////////////////////////////////////////////////////
//all of the measured tests are declared in this section

//measure how long it takes to register work with the thread_manager
template <typename Vector, typename Action>
void register_work(Vector packages, uint64_t num, double ot){
    using namespace hpx;
    uint64_t i = 0;
    double mean;
    string message = "Measuring time required to cleanup terminated threads:";
    vector<double> time;
    time.reserve(num);

    naming::address addr;
    agas::is_local_address(find_here(), addr);
    naming::address::address_type lva = addr.address_;
    util::tuple0<> env;
    BOOST_RV_REF(HPX_STD_FUNCTION<
        threads::thread_function_type>) func = 
        Action::construct_thread_function(
        lva, boost::forward<util::tuple0<> >(env));
    applier::applier* app = applier::get_applier_ptr();
    threads::thread_priority priority = actions::action_priority<Action>();
    threads::thread_init_data data(boost::move(func), "<unknown>", 
        lva, priority, std::size_t(-1));
    threads::threadmanager_base& base = app->get_thread_manager();
    threads::thread_state_enum state = threads::terminated;
    error_code ec = hpx::throws;

    high_resolution_timer t;
//    for(; i < num; ++i) base.register_thread(data, state, true, ec);
    mean = t.elapsed()/num;
    for(i = 0; i < num; i++){
        high_resolution_timer t1;
        printf("registering %ld\n",i);
        base.register_thread(data, state, true, ec);
        time.push_back(t1.elapsed());
    }
    
    state = threads::suspended;
    t.restart();
    for(i = 0; i < num; ++i) base.register_thread(data, state, false, ec);
    double regmean = t.elapsed()/num;

    if(!csv)
        printf("Average time to register threads is %f ns\n\n", regmean*1e9);
    for(i = 0; i < num; i++) time[i] -= regmean;
    mean -= regmean;

    printout(time, ot, mean, message);
}

///////////////////////////////////////////////////////////////////////////////
int hpx_main(variables_map& vm){
    uint64_t num = vm["number"].as<uint64_t>();
    csv = (vm.count("csv") ? true : false);
    run_tests<vector<void_package0*>, void_package0, void_action0>(num);
    hpx::finalize();
    if(!csv) printf("The following abort is intentional:\n");
    hpx::terminate();
    return 0;
}

///////////////////////////////////////////////////////////////////////////////
int main(int argc, char* argv[]){
    // Configure application-specific options.
    options_description
        desc_commandline("usage: " HPX_APPLICATION_STRING " [options]");

    desc_commandline.add_options()
        ("number,N",
            boost::program_options::value<uint64_t>()
                ->default_value(500000),
            "number of packages to create which will register work")
        ("csv",
            "output results as csv "
            "(format:count,mean,accurate mean,variance,min,max)");

    // Initialize and run HPX
    hpx::init(desc_commandline, argc, argv);
    outname = argv[0];
    return 0;
}

///////////////////////////////////////////////////////////////////////////////


//this tests how long is required to check if an address is local
template <typename Vector, typename Package, typename Action>
void run_tests(uint64_t num){
    double ot = timer_overhead(num);
    string message;
    vector<double> time;
    Vector packages;
    create_packages<Vector, Package>(packages, num);

    register_work<Vector, Action>(packages, num, ot);
}
