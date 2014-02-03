
//  Copyright (c) 2013 Thomas Heller
//  Copyright (c) 2013-2014 Vinay C Amatya
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef BENCHMARKS_UTS_WS_STEALSTACK_HPP
#define BENCHMARKS_UTS_WS_STEALSTACK_HPP

#include <benchmarks/uts/params.hpp>
#include <hpx/include/async.hpp>
#include <hpx/include/components.hpp>
#include <hpx/lcos/local/condition_variable.hpp>

#include <boost/ref.hpp>

namespace components
{
    struct ws_stealstack
      : hpx::components::managed_component_base<ws_stealstack>
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
              //, max_shared_stack_depth(0)
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
                //ar & max_shared_stack_depth;
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
            //std::size_t max_shared_stack_depth;
            std::size_t max_tree_depth;

            double time[NSTATES];
        };

        ws_stealstack()
          : local_work(0)
          , work_shared(0)
          , sharedq_work(0)
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
          , local_q_(NULL)
          , shared_q_(NULL)
        {
        }

        typedef hpx::lcos::local::spinlock mutex_type;
        mutex_type localq_mtx;
        mutex_type sharedq_mtx;
        mutex_type check_work_mtx;

        void init(params p, std::size_t r, std::size_t s, hpx::naming::id_type id)
            //, std::vector<hpx::id_type> sharedq_ids)
            //, hpx::id_type shared_id)
        {
            rank = r;
            size = s;
            param = p;
            my_id = id;
            
            /*BOOST_FOREACH(hpx::id_type s_id, sharedq_ids)
            {
                // One shared queue per locality.
                if(s_id.get_msb() == id.get_msb())
                    sharedq_id = s_id;
            }*/
            
            last_steal = rank;
            last_share = rank;

            max_localq_size = param.chunk_size * param.chunk_size;

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

        void put_work_sharedq(std::vector<stealstack_node> work_shared)
        {
            std::size_t count = 0;
            {
                mutex_type::scoped_lock lk(sharedq_mtx);
                BOOST_FOREACH(stealstack_node ss_n, work_shared)
                {
                    shared_q_.push_front(ss_n);
                    count += ss_n.work.size();
                }
            }
            sharedq_work += count;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, put_work_sharedq);

        void put_work(node const & n)
        {   
            if(local_work < max_localq_size) //param.chunk_size * param.chunk_size
            {
                {   
                    /* If the stack is empty, push an empty stealstack_node. */
                    if(local_q_.empty())
                    {
                        stealstack_node ss_node(param.chunk_size);
                        local_q_.push_front(ss_node);
                    }

                    /* If the current stealstack_node is full, push a new one. */
                    if(local_q_.front().work.size() == param.chunk_size)
                    {
                        stealstack_node ss_node(param.chunk_size);
                        local_q_.push_front(ss_node);
                    }
                    else if (local_q_.front().work.size() > param.chunk_size)
                    {
                        throw std::logic_error("ss_put_work(): Block has overflowed!");
                    }

                        stealstack_node & ss_node = local_q_.front();

                        ss_node.work.push_back(n);
                }
                ++local_work;
                std::size_t local_work_tmp = local_work;
                stat.max_stack_depth = (std::max)(local_work_tmp, stat.max_stack_depth);
            }
            else
            {
                std::vector<stealstack_node> ss_nshare;
                BOOST_ASSERT(!local_q_.empty());
                std::size_t transfer_size = local_q_.size()/2;
                ss_nshare.resize(transfer_size);

                for(std::size_t i = 0; i< transfer_size; ++i)
                {
                    std::swap(ss_nshare[i], local_q_.back());
                    local_q_.pop_back();
                    local_work -= ss_nshare[i].work.size();
                }
                
                //typedef components::ws_stealstack::put_work_sharedq_action action_type;
                //hpx::unique_future<void> fut = hpx::async<action_type>(my_id, ss_nshare);
                //action_type act;
                //act(my_id, ss_nshare);
                put_work_sharedq(ss_nshare);

                {
                    if(local_q_.front().work.size() == param.chunk_size)
                    {
                        stealstack_node ss_node(param.chunk_size);
                        local_q_.push_front(ss_node);
                    }
                    else if (local_q_.front().work.size() > param.chunk_size)
                    {
                        throw std::logic_error("ss_put_work(): Block has overflowed!");
                    }

                    stealstack_node & ss_node = local_q_.front();

                    ss_node.work.push_back(n);
                }
                ++local_work;
                std::size_t local_work_tmp = local_work;
                stat.max_stack_depth = (std::max)(local_work_tmp, stat.max_stack_depth);
            }
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
            }
            else
            {
                ++stat.n_leaves;
            }
        }

        //steal work from the shared stack of same thread
        std::pair<bool, std::vector<stealstack_node> > steal_work_local()
        {
            std::pair<bool, std::vector<stealstack_node> > res = 
                std::make_pair(false, std::vector<stealstack_node>());

            ///\a max_localq_size = param.chunk_size * param.chunk_size
            if(sharedq_work > max_localq_size/2)
            {    
                std::size_t steal_num = param.chunk_size/2; // steal half from the local shared_q
                res.second.resize(steal_num);
                mutex_type::scoped_lock lk(sharedq_mtx);
                for(std::size_t i = 0; i < steal_num; ++i)
                {
                    std::swap(res.second[i], shared_q_.back());
                    shared_q_.pop_back();
                    sharedq_work -= res.second[i].work.size();
                }
            }
            else if(sharedq_work > 0)
            {
                std::size_t steal_num = shared_q_.size();
                res.second.resize(steal_num);
                mutex_type::scoped_lock lk(sharedq_mtx);
                {   
                    for(std::size_t i = 0; i < steal_num; ++i)
                    {   std::swap(res.second[i], shared_q_.back());
                        shared_q_.pop_back();
                        sharedq_work -= res.second[i].work.size();
                    }
                }
            }

            if(res.second.size() != 0)
            {
                res.first = true;
            }

            return res;
        }

        /// steal only chunk size
        std::pair<bool, std::vector<stealstack_node> > steal_work()
        {
            std::pair<bool, std::vector<stealstack_node> > res =
                std::make_pair(false, std::vector<stealstack_node>());

            /// For within node work stealing, steal chunk size work only
            //if(sharedq_work >= param.chunk_size && shared_q_.size() != 0)
            if(sharedq_work > 0)    // && shared_q_.size() != 0)
            {
                std::size_t steal_num = 0;
                if(shared_q_.size() == 1)
                    steal_num = 1;
                else
                    steal_num = shared_q_.size()/2;

                res.second.resize(steal_num);
                
                {
                    std::size_t count = 0;
                    mutex_type::scoped_lock lk(sharedq_mtx);
                    for(std::size_t i = 0; i < steal_num; ++i)
                    {      
                        std::swap(res.second[i], shared_q_.back());
                        shared_q_.pop_back();
                        count += res.second[i].work.size();
                        //sharedq_work -= res.second[i].work.size();
                    }
                    sharedq_work -= count;
                }

                if(sharedq_work < res.second[0].work.size())
                {
                    throw std::logic_error(
                    "ensure_sharedq_work(): sharedq_work count is less than chunksize!");
                }
            }

            //if(local_work > 0 || work_shared > 0)
            if(res.second.size() !=0 )
            {
                res.first = true;
            }

            return res;

        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, steal_work);

        bool check_work()
        {
            //mutex_type::scoped_lock lk(check_work_mtx);	//##################
            if(sharedq_work.load(boost::memory_order::memory_order_relaxed) > 0 || local_work > 0)
                return true;
            else 
                return false;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, check_work);

        bool ensure_local_work()
        {   
            while(local_work == 0)
            {   
                bool terminate = true; 

                // steal work from own shared q first 
                std::pair<bool, std::vector<stealstack_node> > nd_pair(boost::move(steal_work_local()));
                BOOST_FOREACH(stealstack_node & ss_n, nd_pair.second)
                {
                    if(ss_n.work.size() > 0)
                    {
                        terminate = false;
                        local_q_.push_back(ss_n);
                        local_work += ss_n.work.size();
                        //break_ = true;
                    }
                }

                /// check if local work is now present
                if(!terminate || local_work > 0)
                    return true;                    

                BOOST_ASSERT(local_work == 0 && sharedq_work == 0);

                /// No work in local shared queue, look for work in other shared queue
                for(std::size_t i = 0; i < size -1; ++i)
                {
                    std::size_t last_steal_temp = last_steal;
                    if(last_steal == rank) last_steal = (last_steal + 1) % size;
                    //last_steal = (last_steal + 1) % size;
                    
                    ws_stealstack::steal_work_action act;
                    std::pair<bool, std::vector<stealstack_node> > node_pair(boost::move(act(ids[last_steal])));

                    bool break_ = false;
                    if(node_pair.second.size() != 0)
                    {
                        if(node_pair.second.size() > param.chunk_size/2)
                        {
                            std::size_t lcl_num = param.chunk_size/2;
                            //local_q_.resize(lcl_num);
                            // put a certain no. on local_q
                            for(std::size_t i = 0; i < lcl_num; ++i)
                            {
                                local_q_.push_front(node_pair.second.back());
                                node_pair.second.pop_back();
                                local_work += local_q_.front().work.size();
                            }

                            terminate = false;
                            break_ = true;
                            //put remaining on shared_q
                            {
                                //std::size_t count = 0;
                                mutex_type::scoped_lock lk(sharedq_mtx);
                                BOOST_FOREACH(stealstack_node& ss_n, node_pair.second)
                                {
                                    shared_q_.push_front(ss_n);
                                    sharedq_work += shared_q_.front().work.size();
                                }
                            }
                            node_pair.second.clear();
                        }
                        else
                        {
                            BOOST_FOREACH(stealstack_node & ss_n, node_pair.second)
                            {
                                local_q_.push_front(ss_n);
                                local_work += local_q_.front().work.size();
                            }
                            terminate = false;
                            break_ = true;

                            node_pair.second.clear();
                        }
                    }
                    
                    if(break_ || local_work > 0) 
                    {
                        last_steal = last_steal_temp;
                        break;
                    }

                    /*if(node_pair.first)
                    {
                        terminate = false;
                    }*/
                }

                // Finally Check if there is any work remaining
                std::vector<hpx::unique_future<bool> > check_work_futures;
                typedef components::ws_stealstack::check_work_action action_type;
                BOOST_FOREACH(hpx::id_type id, ids)
                {
                    if(id != my_id)
                    {
                        check_work_futures.push_back(hpx::async<action_type>(id));
                    }
                }
                //hpx::wait_any(check_work_futures);
                BOOST_FOREACH(hpx::unique_future<bool> & fut, check_work_futures)
                {
                    if(fut.get())
                        terminate =  false;
                    break;
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
                //mutex_type::scoped_lock lk(localq_mtx); //##################
                std::swap(work, local_q_.front().work);
                local_q_.pop_front();
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
            //std::vector<hpx::unique_future<void> > gen_children_futures;
            //gen_children_futures.reserve(param.chunk_size);
            while(get_work(parents))
            {
                BOOST_FOREACH(node & parent, parents)
                {
                    /*gen_children_futures.push_back(
                        hpx::async(&ws_stealstack::gen_children, this
                            , boost::ref(parent))
                    );*/
                    // Test the serial version.
                    gen_children(boost::ref(parent));
                }
                parents.clear();
                //hpx::wait(gen_children_futures);
                //gen_children_futures.clear();
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
        //boost::atomic<std::size_t> local_work;
        std::size_t local_work;
        boost::atomic<std::size_t> work_shared;
        boost::atomic<std::size_t> sharedq_work;
        hpx::id_type my_id;
        //hpx::id_type sharedq_id;

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

        std::size_t max_localq_size;

        std::deque<stealstack_node> local_q_;
        std::deque<stealstack_node> shared_q_;
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
