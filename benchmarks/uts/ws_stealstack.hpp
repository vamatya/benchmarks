
//  Copyright (c) 2013 Thomas Heller
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef BENCHMARKS_UTS_WS_STEALSTACK_HPP
#define BENCHMARKS_UTS_WS_STEALSTACK_HPP

#include <benchmarks/uts/params.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/components.hpp>

namespace components
{
    struct ws_stealstack
      : hpx::components::simple_component_base<ws_stealstack>
    {
        enum states
        {
            WORK    = 0,
            SEARCH  = 1,
            IDLE    = 2,
            OVH     = 3,
            NSTATES = 4
        };

        struct stats
        {
            stats()
              : n_nodes(0)
              , n_leaves(0)
              , n_release(0)
              , n_acquire(0)
              , n_steal(0)
              , n_fail(0)
              , max_stack_depth(0)
              , max_tree_depth(0)
            {
                time[WORK] = 0.0;
                time[SEARCH] = 0.0;
                time[IDLE] = 0.0;
                time[OVH] = 0.0;
            }

            template <typename Archive>
            void serialize(Archive & ar, unsigned)
            {
                ar & n_nodes;
                ar & n_leaves;
                ar & n_acquire;
                ar & n_release;
                ar & n_steal;
                ar & n_fail;

                ar & max_stack_depth;
                ar & max_tree_depth;

                ar & time;
            }

            std::size_t n_nodes;
            std::size_t n_leaves;
            std::size_t n_release;
            std::size_t n_acquire;
            std::size_t n_steal;
            std::size_t n_fail;

            std::size_t max_stack_depth;
            std::size_t max_tree_depth;

            double time[NSTATES];
        };

        ws_stealstack()
          : local_work(0)
          , work_shared(0)
          , walltime(0)
          , work_time(0)
          , search_time(0)
          , idle_time(0)
          , idle_sessions(0)
          , time_last(0)
          , curState(0)
          , start_time(0)
          , chunks_recvd(0)
          , chunks_sent(0)
          , ctrl_recvd(0)
          , ctrl_sent(0)
          , pollint_adaptive(false)
        {
        }

        typedef hpx::lcos::local::spinlock mutex_type;
        mutex_type local_queue_mtx;

        void init(params p, std::size_t r, std::size_t s)
        {
            rank = r;
            size = s;
            param = p;

            last_steal = rank;
            last_share = rank;

            if(p.polling_interval == 0)
            {
                pollint_adaptive = true;
                p.polling_interval = 1;
            }

            if(rank == 0)
            {
                node n;
                n.init_root(param);
                put_work(n);
            }
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, init);

        void resolve_names(std::vector<hpx::id_type> const & idss)
        {
            ids = idss;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, resolve_names);

        void put_work(node const & n)
        {
            {
                mutex_type::scoped_lock lk(local_queue_mtx);
                /* If the stack is empty, push an empty stealstack_node. */
                if(local_queue.empty())
                {
                    stealstack_node node(param.chunk_size);
                    local_queue.push_front(node);
                }


                /* If the current stealstack_node is full, push a new one. */
                if(local_queue.front().work.size() == param.chunk_size)
                {
                    stealstack_node node(param.chunk_size);
                    local_queue.push_front(node);
                }
                else if (local_queue.front().work.size() > param.chunk_size)
                {
                    throw std::logic_error("ss_put_work(): Block has overflowed!");
                }

                stealstack_node & node = local_queue.front();

                node.work.push_back(n);
            }
            local_work++;
            std::size_t local_work_tmp = local_work;
            stat.max_stack_depth = (std::max)(local_work_tmp, stat.max_stack_depth);
        }

        void gen_children(node & parent)
        {
            std::size_t parent_height = parent.height;

            stat.max_tree_depth = (std::max)(stat.max_tree_depth, parent_height);

            int num_children = parent.get_num_children(param);
            int child_type = parent.child_type(param);

            parent.num_children = num_children;

            if(num_children > 0)
            {

                for(int i = 0; i < num_children; ++i)
                {
                    node child;
                    child.type = child_type;
                    child.height = parent_height + 1;
                    for(int j = 0; j < param.compute_granularity; ++j)
                    {
                        rng_spawn(parent.state.state, child.state.state, i);
                    }

                    put_work(child);
                }
                /*
                if(size > 1)
                {
                    if(local_work > param.chunk_size * param.chunk_size)
                    {
                        mutex_type::scoped_lock lk(local_queue_mtx);
                        last_share = (last_share + 1) % size;
                        if(last_share == rank) last_share = (last_share + 1) % size;

                        std::size_t steal_num = local_queue.size()/2;
                        std::vector<stealstack_node> nodes;
                        nodes.resize(steal_num);
                        for(std::size_t i = 0; i < steal_num; ++i)
                        {
                            std::swap(nodes[i], local_queue.back());
                            local_queue.pop_back();

                            if(local_work < nodes[i].work.size())
                            {
                                throw std::logic_error(
                                    "gen_children(): local_work count is less than 0!");
                            }
                            local_work -= nodes[i].work.size();
                            work_shared += nodes[i].work.size();
                        }
                        hpx::apply<share_work_action>(ids[last_share], rank, nodes);
                    }
                }
                */
            }
            else
            {
                ++stat.n_leaves;
            }
        }

        std::pair<bool, std::vector<stealstack_node> > steal_work()
        {
            std::pair<bool, std::vector<stealstack_node> > res = 
                std::make_pair(false, std::vector<stealstack_node>());

            if(local_work > param.chunk_size * param.chunk_size)
            {
                mutex_type::scoped_lock lk(local_queue_mtx);
                std::size_t steal_num = local_queue.size()/2;
                res.second.resize(steal_num);
                for(std::size_t i = 0; i < steal_num; ++i)
                {
                    std::swap(res.second[i], local_queue.back());
                    local_queue.pop_back();

                    if(local_work < res.second[i].work.size())
                    {
                        throw std::logic_error(
                            "ensure_local_work(): local_work count is less than 0!");
                    }
                    local_work -= res.second[i].work.size();
                }
            }

            if(local_work > 0 || work_shared > 0)
            {
                res.first = true;
            }

            return res;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, steal_work);

        void share_work(std::size_t src, std::vector<stealstack_node> const & work)
        {
            if(local_work > 2 * param.chunk_size)
            {
                mutex_type::scoped_lock lk(local_queue_mtx);
                last_share = (last_share + 1) % size;
                if(last_share == rank) last_share = (last_share + 1) % size;
                {
                    hpx::util::unlock_the_lock<mutex_type::scoped_lock> ull(lk);
                    hpx::apply<share_work_action>(ids[last_share], src, work);
                }
            }
            else
            {
                std::size_t count = 0;
                BOOST_FOREACH(stealstack_node const & ss_node, work)
                {
                    if(ss_node.work.size() > 0)
                    {
                        mutex_type::scoped_lock lk(local_queue_mtx);

                        local_queue.push_back(ss_node);
                        local_work += ss_node.work.size();
                        count += ss_node.work.size();
                    }
                }
                hpx::apply<ack_share_action>(ids[src], count);
            }
        }
        
        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, share_work);

        void ack_share(std::size_t work)
        {
            work_shared -= work;
        }
        
        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, ack_share);

        bool ensure_local_work()
        {
            while(local_work == 0)
            {
                bool terminate = true;
                for(std::size_t i = 0; i < size -1; ++i)
                {
                    last_steal = (last_steal + 1) % size;
                    if(last_steal == rank) last_steal = (last_steal + 1) % size;

                    ws_stealstack::steal_work_action act;
                    std::pair<bool, std::vector<stealstack_node> > node_pair(boost::move(act(ids[last_steal])));

                    bool break_ = false;
                    BOOST_FOREACH(stealstack_node & ss_node, node_pair.second)
                    {
                        if(ss_node.work.size() > 0)
                        {
                            mutex_type::scoped_lock lk(local_queue_mtx);
                            terminate = false;

                            local_queue.push_back(ss_node);
                            local_work += ss_node.work.size();
                            break_ = true;
                        }
                    }
                    if(break_ || local_work > 0) break;

                    if(node_pair.first)
                    {
                        terminate = false;
                    }
                }

                if(terminate) return false;
            }

            return true;
        }

        bool get_work(std::vector<node> & work)
        {
            if(!ensure_local_work())
            {
                return false;
            }

            {
                mutex_type::scoped_lock lk(local_queue_mtx);
                std::swap(work, local_queue.front().work);
                local_queue.pop_front();
            }

            stat.n_nodes += work.size();
            if (local_work < work.size())
            {
                throw std::logic_error(
                    "ensure_local_work(): local_work count is less than 0!");
            }
            local_work -= work.size();

            if(work.size() == 0)
            {
                hpx::cout << "get_work(): called with work.size() = 0, "
                    << "local_work=" << local_work
                    << " or " << (local_work % param.chunk_size)
                    << " (mod " << param.chunk_size << ")\n" << hpx::flush;
                throw std::logic_error("get_work(): Underflow!");
            }

            return true;
        }

        void tree_search()
        {
            std::vector<node> parents;
            while(get_work(parents))
            {
                BOOST_FOREACH(node & parent, parents)
                {
                    gen_children(parent);
                }
                parents.clear();
            }
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, tree_search);

        stats get_stats()
        {
            return stat;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, get_stats);

    private:
        std::vector<hpx::id_type> ids;
        boost::atomic<std::size_t> local_work;
        boost::atomic<std::size_t> work_shared;

        stats stat;

        double walltime;
        double work_time;
        double search_time;
        double idle_time;
        int idle_sessions;
        double time_last;
        int entries[NSTATES];
        int curState;

        double start_time;

        std::deque<stealstack_node> local_queue;
        std::size_t last_steal;
        std::size_t last_share;
        int chunks_recvd;
        int chunks_sent;
        int ctrl_recvd;
        int ctrl_sent;

        params param;
        bool pollint_adaptive;
        std::size_t rank;
        std::size_t size;
    };
}

#endif
