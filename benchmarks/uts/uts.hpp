
//  Copyright (c) 2013 Thomas Heller
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef BENCHMARKS_UTS_UTS_HPP
#define BENCHMARKS_UTS_UTS_HPP

#include <benchmarks/uts/rng/rng.h>

#include <hpx/hpx_init.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/iostreams.hpp>
#include <hpx/components/distributing_factory/distributing_factory.hpp>

#include <cmath>

#define MAX_NUM_CHILDREN    100  // cap on children (BIN root is exempt)
#define MAX_SPAWN_GRANULARITY 500  // cap max sequential work

// Interpret 32 bit positive integer as value on [0,1)
inline double rng_toProb(int n) 
{
  if (n < 0) {
      hpx::cout << "*** toProb: rand n = " << n << " out of range\n" << hpx::flush;
  }
  return ((n<0)? 0.0 : ((double) n)/2147483648.0);
}

struct node
{
    enum tree_type {
        BIN = 0,
        GEO,
        HYBRID,
        BALANCED
    };

    static const char * tree_type_str(tree_type type)
    {
        switch (type)
        {
            case BIN:
                return "Binomial";
            case GEO:
                return "Geometric";
            case HYBRID:
                return "Hybrid";
            case BALANCED:
                return "Balanced";
            default:
                return "Unkown";
        }
    }

    enum geoshape {
        LINEAR = 0,
        EXPDEC,
        CYCLIC,
        FIXED
    };

    static std::string geoshape_str(geoshape type)
    {
        switch (type)
        {
            case LINEAR:
                return "Linear decrease";
            case EXPDEC:
                return "Exponential decrease";
            case CYCLIC:
                return "Cyclic";
            case FIXED:
                return "Fixed branching factor";
            default:
                return "Unkown";
        }
    }

    int type;
    std::size_t height;
    int num_children;

    state_t state;

    template <typename Archive>
    void serialize(Archive & ar, unsigned)
    {
        ar & type;
        ar & height;
        ar & num_children;
        ar & state.state;
    }

    template <typename Params>
    void init_root(Params const & p)
    {
        type = p.type;
        height = 0;
        num_children = -1;
        rng_init(state.state, p.root_id);

        if(p.debug & 1)
        {
            hpx::cout << "root node of type " << p.type 
                << " at " << std::hex << this << std::dec << "\n" << hpx::flush;
        }
    }

    template <typename Params>
    int child_type(Params const & p)
    {
        switch (p.type)
        {
            case BIN:
              return BIN;
            case GEO:
              return GEO;
            case HYBRID:
              if (height < p.shift_depth * p.gen_mx)
                return GEO;
              else
                return BIN;
            case BALANCED:
              return BALANCED;
            default:
              throw std::logic_error("node::child_type(): Unknown tree type");
              return -1;
        }
    }

    template <typename Params>
    int get_num_children_bin(Params const & p)
    {
        int v = rng_rand(state.state);
        double d = rng_toProb(v);

        return (d < p.non_leaf_prob) ? p.non_leaf_bf : 0;
    }

    template <typename Params>
    int get_num_children_geo(Params const & p)
    {
        double b_i = p.b_0;
        std::size_t depth = height;

        // use shape function to compute target b_i
        if(depth != 0)
        {
            switch(p.shape_fn)
            {
                // expected size polynomial in depth
                case EXPDEC:
                    b_i = p.b_0 * std::pow(
                        (double)depth, -std::log(p.b_0)/std::log((double)p.gen_mx));
                    break;
                // cyclic tree size
                case CYCLIC:
                    if(depth > 5 * p.gen_mx)
                    {
                        b_i = 0.0;
                        break;
                    }
                    b_i = std::pow(p.b_0,
                        std::sin(2.0 * 3.141592653589793*(double)depth / (double)p.gen_mx));
                    break;
                case FIXED:
                    b_i = (depth < p.gen_mx) ? p.b_0 : 0;
                    break;
                case LINEAR:
                default:
                    b_i = p.b_0 * (1.0 - (double)depth / (double)p.gen_mx);
                    break;
            }
        }

        // given target b_i, find prob p so expected value of
        // geometric distribution is b_i.
        double prob = 1.0 / (1.0 + b_i);

        // get uniform random number on [0,1)
        int h = rng_rand(state.state);
        double u = rng_toProb(h);

        // max number of children at this cumulative probability
        // (from inverse geometric cumulative density function)
        return (int) std::floor(std::log(1.0 - u) / std::log(1.0 - prob));
    }

    template <typename Params>
    int get_num_children(Params const & p)
    {
        int num = 0;
        switch (p.type)
        {
            case BIN:
                if(height == 0)
                {
                    num = static_cast<int>(std::floor(p.b_0));
                }
                else
                {
                    num = get_num_children_bin(p);
                }
                break;
            case GEO:
                num = get_num_children_geo(p);
                break;
            case HYBRID:
                if(height < p.shift_depth * p.gen_mx)
                {
                    num = get_num_children_geo(p);
                }
                else
                {
                    num = get_num_children_bin(p);
                }
                break;
            case BALANCED:
                if(height < p.gen_mx)
                {
                    num = (int)p.b_0;
                }
                break;
            default:
                throw std::logic_error("node::get_num_children(): Unknown tree type");
        }

        // limit number of children
        // only a BIN root can have more than MAX_NUM_CHILDREN
        if(height == 0 && type == BIN)
        {
            int root_BF = (int)std::ceil(p.b_0);
            if(num > root_BF)
            {
                hpx::cout << "*** Number of children of root truncated from "
                    << num << " to " << root_BF << "\n" << hpx::flush;
                num = root_BF;
            }
        }
        else if (p.type != BALANCED)
        {
            if (num > MAX_NUM_CHILDREN)
            {
                hpx::cout << "*** Number of children truncated from "
                    << num << " to " << MAX_NUM_CHILDREN << "\n" << hpx::flush;
                num = MAX_NUM_CHILDREN;
            }
        }
        return num;
    }
};

typedef boost::shared_ptr<node> node_ptr;

inline std::ostream & operator<<(std::ostream & os, node::tree_type type)
{
    os << node::tree_type_str(type);
    return os;
}

inline std::istream & operator>>(std::istream & is, node::tree_type & type)
{
    int i;
    is >> i;
    switch (i)
    {
        case node::BIN:
            type = node::BIN;
            break;
        case node::GEO:
            type = node::GEO;
            break;
        case node::HYBRID:
            type = node::HYBRID;
            break;
        case node::BALANCED:
            type = node::BALANCED;
            break;
        default:
            type = node::BIN;
            break;
    }
    return is;
}

inline std::ostream & operator<<(std::ostream & os, node::geoshape type)
{
    os << node::geoshape_str(type);
    return os;
}


inline std::istream & operator>>(std::istream & is, node::geoshape & type)
{
    int i;
    is >> i;
    switch (i)
    {
        case node::LINEAR:
            type = node::LINEAR;
            break;
        case node::EXPDEC:
            type = node::EXPDEC;
            break;
        case node::CYCLIC:
            type = node::CYCLIC;
            break;
        case node::FIXED:
            type = node::FIXED;
            break;
        default:
            type = node::LINEAR;
            break;
    }
    return is;
}

struct stealstack_node
{
    stealstack_node()
    {}

    stealstack_node(std::size_t size)
    {
        work.reserve(size);
    }

    template <typename Archive>
    void serialize(Archive & ar, unsigned)
    {
        ar & work;
    }

    void swap(stealstack_node& rhs)
    {
        std::swap(work, rhs.work);
    }

    std::vector<node> work;
};

template <typename Stats>
void show_stats(double walltime, Stats const & stats, int verbose, int header, std::size_t chunk_size, float overcommit_factor)
{
    std::size_t tnodes = 0, tleaves = 0, trel = 0, tacq = 0, tsteal = 0, tfail= 0;
    std::size_t mdepth = 0, mheight = 0;
    double twork = 0.0, tsearch = 0.0, tidle = 0.0, tovh = 0.0;
    double max_times[4];
    double min_times[4];
//    double elapsedSecs;

    for (int i = 0; i < 4; i++) {
        max_times[i] = 0.0;
        min_times[i] = stats[0].time[i];
    }

    // combine measurements from all threads
    BOOST_FOREACH(typename Stats::value_type const & stat, stats)
    {
        tnodes  += stat.n_nodes;
        tleaves += stat.n_leaves;
        trel    += stat.n_release;
        tacq    += stat.n_acquire;
        tsteal  += stat.n_steal;
        tfail   += stat.n_fail;
        twork   += stat.time[0];
        tsearch += stat.time[1];
        tidle   += stat.time[2];
        tovh    += stat.time[3];
        mdepth   = (std::max)(mdepth, stat.max_stack_depth);
        mheight  = (std::max)(mheight, stat.max_tree_depth);

        for (std::size_t j = 0; j < 4; j++) {
            if (max_times[j] < stat.time[j])
                max_times[j] = stat.time[j];
            if (min_times[j] > stat.time[j])
                min_times[j] = stat.time[j];
        }
    }

    if (trel != tacq + tsteal) {
        hpx::cout << "*** error! total released != total acquired + total stolen\n" << hpx::flush;
    }

    //uts_showStats(ss_get_num_threads(), chunkSize, walltime, tnodes, tleaves, mheight);

    // summarize execution info for machine consumption
    if (verbose == 0) {
        if (header == 1){
            hpx::cout << "OS_Threads,Num_localities,Execution_Time,"
                "Nodes_trans,Ntr_per_sec,Ntrps_per_thread,Chunk_size,Overcommit_factor\n"
            << hpx::flush;
        }
        std::size_t num_threads = hpx::get_num_worker_threads();
        hpx::future<boost::uint32_t> locs = hpx::get_num_localities();
        hpx::cout
            << num_threads << ","
            << locs.get() << ","
            << walltime << ","
            << tnodes << ","
<<<<<<< Updated upstream
            << static_cast<long long>(tnodes/walltime) << ","
            << static_cast<long long>((tnodes/walltime)/num_threads) << ","
=======
            //<< static_cast<long long>(tnodes/walltime) << ","
          //  << static_cast<long long>((tnodes/walltime)/num_threads) << ","
>>>>>>> Stashed changes
            << chunk_size << ","
            << overcommit_factor << "\n"
            << hpx::flush;
        /*
        printf("%4d %7.3f %9llu %7.0llu %7.0llu %d %d %.2f %d %d %1d %f %3d\n",
            nPes, walltime, nNodes, (long long)(nNodes/walltime), (long long)((nNodes/walltime)/nPes), chunkSize,
            type, b_0, rootId, gen_mx, shape_fn, nonLeafProb, nonLeafBF);
        */
    }

    // summarize execution info for human consumption
    else {
        hpx::cout
            << "Tree size = " << tnodes << ", "
            << "tree depth = " << mheight << ", "
            << "num leaves = " << tleaves
                << " (" << tleaves/static_cast<float>(tnodes)*100.0f << "%)"
            << "\n" << hpx::flush;

        hpx::cout
            << "Wallclock time = " << walltime << " sec\n"
            << "Performance = " << (tnodes / walltime) << " nodes/sec "
            << "(" << (tnodes / walltime / hpx::get_num_worker_threads()) << " nodes/sec per PE)\n"
            << "\n" << hpx::flush;
    }

    /*
    if (verbose > 1) {
        printf("Total chunks released = %d, of which %d reacquired and %d stolen\n",
            trel, tacq, tsteal);
        printf("Failed steals = %d, Max queue size = %d\n", tfail, mdepth);
        printf("Avg time per thread: Work = %.6f, Overhead = %6f, Search = %.6f, Idle = %.6f.\n", (twork / ss_get_num_threads()),
            (tovh / ss_get_num_threads()), (tsearch / ss_get_num_threads()), (tidle / ss_get_num_threads()));
        printf("Min time per thread: Work = %.6f, Overhead = %6f, Search = %.6f, Idle = %.6f.\n", min_times[SS_WORK], min_times[SS_OVH],
            min_times[SS_SEARCH], min_times[SS_IDLE]);
        printf("Max time per thread: Work = %.6f, Overhead = %6f, Search = %.6f, Idle = %.6f.\n\n", max_times[SS_WORK], max_times[SS_OVH],
            max_times[SS_SEARCH], max_times[SS_IDLE]);
    }

    // per thread execution info
    if (verbose > 2) {
        for (i = 0; i < num_workers; i++) {
            printf("** Thread %d\n", i);
            printf("  # nodes explored    = %d\n", stealStack[i].nNodes);
            printf("  # chunks released   = %d\n", stealStack[i].nRelease);
            printf("  # chunks reacquired = %d\n", stealStack[i].nAcquire);
            printf("  # chunks stolen     = %d\n", stealStack[i].nSteal);
            printf("  # failed steals     = %d\n", stealStack[i].nFail);
            printf("  maximum stack depth = %d\n", stealStack[i].maxStackDepth);
            printf("  work time           = %.6f secs (%d sessions)\n",
                    stealStack[i].time[SS_WORK], stealStack[i].entries[SS_WORK]);
            printf("  overhead time       = %.6f secs (%d sessions)\n",
                    stealStack[i].time[SS_OVH], stealStack[i].entries[SS_OVH]);
            printf("  search time         = %.6f secs (%d sessions)\n",
                    stealStack[i].time[SS_SEARCH], stealStack[i].entries[SS_SEARCH]);
            printf("  idle time           = %.6f secs (%d sessions)\n",
                    stealStack[i].time[SS_IDLE], stealStack[i].entries[SS_IDLE]);
            printf("\n");
        }
    }
    */
}

#endif
