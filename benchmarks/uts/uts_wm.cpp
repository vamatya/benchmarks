
//  Copyright (c) 2013 Thomas Heller
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*******************************************************************************
 *
 *
 *
 ******************************************************************************/

#include <benchmarks/uts/params.hpp>
#include <benchmarks/uts/wm_stealstack.hpp>

HPX_REGISTER_MINIMAL_COMPONENT_FACTORY(
    hpx::components::managed_component< ::components::wm_stealstack>
  , wm_stealstack_component);

int hpx_main(boost::program_options::variables_map & vm)
{
    std::vector<hpx::id_type> stealstacks =
        create_stealstacks<components::wm_stealstack>(vm, "worksharing");

    hpx::util::high_resolution_timer t;

    std::vector<hpx::future<void> > tree_search_futures;
    tree_search_futures.reserve(stealstacks.size());
    BOOST_FOREACH(hpx::id_type const & id, stealstacks)
    {
        tree_search_futures.push_back(
            hpx::async<components::wm_stealstack::tree_search_action>(
                id
            )
        );
    }

    hpx::wait(tree_search_futures);

    double elapsed = t.elapsed();

    std::vector<hpx::future<components::wm_stealstack::stats> > stats_futures;
    stats_futures.reserve(stealstacks.size());
    BOOST_FOREACH(hpx::id_type const & id, stealstacks)
    {
        stats_futures.push_back(
            hpx::async<components::wm_stealstack::get_stats_action>(
                id
            )
        );
    }

    std::vector<components::wm_stealstack::stats> stats;
    stats.reserve(stealstacks.size());
    hpx::wait(stats_futures, stats);
    show_stats(elapsed, stats, vm["verbose"].as<int>(), vm["chunk-size"].as<std::size_t>(), vm["overcommit-factor"].as<float>());

    return hpx::finalize();
}

int main(int argc, char* argv[])
{
    boost::program_options::options_description desc = uts_params_desc();
    return hpx::init(desc, argc, argv);
}
