
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

#include <boost/ref.hpp>

namespace components
{
    struct shared_queue
      : hpx::components::managed_component_base<shared_queue>
    {
        typedef hpx::lcos::local::spinlock mutex_type;
        typedef boost::atomic<std::size_t> atomic_type;
        
        mutex_type sharedq_mtx;

        shared_queue(): shared_q_(NULL), sharedq_work_(0)
        {}

        shared_queue(params p, hpx::naming::id_type id)
            : param_(p), shared_q_(NULL), sharedq_work_(0), my_id_(id)
        {}

        void init(params p, hpx::naming::id_type id)
        {
            param_ = p;
            my_id_ = id;
        }

        HPX_DEFINE_COMPONENT_ACTION(shared_queue, init);

        // Shared Queue work amount. 
        std::size_t sharedq_size()
        {
            return sharedq_work_.load();
        }

        HPX_DEFINE_COMPONENT_ACTION(shared_queue, sharedq_size);

        std::pair<bool, std::vector<stealstack_node> > steal_work()
        {
            bool work_stolen = false;
            std::size_t num_stealstacks = 0;
            std::pair<bool, std::vector<stealstack_node> > ss_result =
                std::make_pair(false, std::vector<stealstack_node>());

            mutex_type::scoped_lock lk(sharedq_mtx);
            if(sharedq_work_ > 0)
            {
                if(sharedq_work_ < param_.chunk_size)
                {
                    throw std::logic_error(
                        "Steal Stack Node for Shared Q has less than Chunk Size Count");
                }
                else
                {
                    if(sharedq_work_ > param_.chunk_size * param_.chunk_size)
                    {
                        work_stolen = true;
                        num_stealstacks = param_.chunk_size;
                        ss_result.second.resize(num_stealstacks);

                        BOOST_ASSERT(shared_q_.size() != 0);
                        for(std::size_t i = 0; i< num_stealstacks; ++i)
                        {
                            std::swap(ss_result.second[i], shared_q_.back());
                            shared_q_.pop_back();
                            sharedq_work_ -= param_.chunk_size;
                        }
                        ss_result.first = work_stolen;
                    }
                    else if(sharedq_work_ > param_.chunk_size)
                    {
                        work_stolen = true;
                        num_stealstacks = shared_q_.size()/2;
                        ss_result.second.resize(num_stealstacks);

                        for(std::size_t i = 0; i < num_stealstacks; ++i)
                        {
                            std::swap(ss_result.second[i], shared_q_.back());
                            shared_q_.pop_back();
                            sharedq_work_ -= param_.chunk_size;
                        }
                        ss_result.first = work_stolen;
                    }
                    else
                    {
                        work_stolen = true;
                        num_stealstacks = 1;
                        ss_result.second.push_back(shared_q_.front());
                        shared_q_.pop_front();
                        sharedq_work_ -= param_.chunk_size;
                        ss_result.first = work_stolen;
                    }
                }
            }

            return ss_result;
        }

        HPX_DEFINE_COMPONENT_ACTION(shared_queue, steal_work);

        // put chunk size steal stack node(work nodes)
        void put_work(std::vector<stealstack_node> ssn_v)
        {
            mutex_type::scoped_lock lk(sharedq_mtx);
            BOOST_FOREACH(stealstack_node ssn, ssn_v)
            {
                sharedq_work_ += ssn.work.size();
                shared_q_.push_front(ssn);
            }
        }

        HPX_DEFINE_COMPONENT_ACTION(shared_queue, put_work);

    private:
        atomic_type sharedq_work_;

        std::deque<stealstack_node> shared_q_;
        hpx::naming::id_type my_id_;

        params param_;
    };

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

        void init_sharedq_id(hpx::id_type s_id)
        {
            sharedq_id = s_id;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, init_sharedq_id);

        void resolve_names(std::vector<hpx::id_type> const & idss)
        {
            ids = idss;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, resolve_names);

        void put_work(node const & n)
        {
            if(2 * param.chunk_size * param.chunk_size > local_work)//local_q_.size())
            {
                {
                    mutex_type::scoped_lock lk(localq_mtx);	//###############
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
                std::vector<stealstack_node> ss_shared_vec;
                //std::size_t num_elem_shared = local_work/2;
                std::size_t num_stealstack_nodes = local_work/(2 * param.chunk_size);
                //std::size_t num_stealstack_nodes = local_q_.size()/2;
                mutex_type::scoped_lock lk(localq_mtx);
                {                  
                    BOOST_ASSERT(num_stealstack_nodes <= local_q_.size());
                    if(num_stealstack_nodes > 0)
                    {                    
                        std::size_t ss_size = ss_shared_vec.size();
                        for(std::size_t i = 0; i < num_stealstack_nodes; ++i)
                        {   
                            ss_shared_vec.push_back(local_q_.back());
                            local_work -= param.chunk_size;
                            local_q_.pop_back();
                        }
                        hpx::async<typename ::components::shared_queue::put_work_action>
                            (sharedq_id, ss_shared_vec);
                    }
                    else
                    {
                        throw std::logic_error("Steal Stack nodes shared is zero!!");
                    }

                    BOOST_ASSERT(!local_q_.empty());

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

        std::pair<bool, std::vector<stealstack_node> > steal_work(hpx::id_type requestor_id)
        {
            std::size_t work_stolen = 0;

            std::pair<bool, std::vector<stealstack_node> > res = 
                std::make_pair(false, std::vector<stealstack_node>());

            if(work_stolen > 0)
            {
                res.first = true;
            }
            
            hpx::unique_future<std::pair<bool, std::vector<stealstack_node> > > res_future 
                = hpx::async<typename ::components::shared_queue::steal_work_action>(sharedq_id);
            res = boost::move(res_future.get());
            return res;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, steal_work);

        bool work_present()
        {
            //mutex_type::scoped_lock lk(check_work_mtx);	//##################
            if(local_work.load(boost::memory_order::memory_order_relaxed) > 0)
                return true;
            else 
                return false;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, work_present);

        bool ensure_local_work()
        {   
            while(local_work == 0)
            { 
                bool terminate = true;
                {   
                    std::pair<bool, std::vector<stealstack_node> > node_pair(boost::move(steal_work(my_id)));

                    mutex_type::scoped_lock lk(localq_mtx);
                    BOOST_FOREACH(stealstack_node ss_node, node_pair.second)
                    {
                        if(ss_node.work.size() > 0)
                        {
                            local_q_.push_back(ss_node);
                            local_work += ss_node.work.size();
                        }
                    }
                }
                if(local_work > 0) 
                {
                    break;                  
                }
                else
                {
                        std::vector<hpx::unique_future<bool> > cw_futures;

                        typedef ws_stealstack::work_present_action action_type;

                        BOOST_FOREACH(hpx::id_type id, ids)
                        {
                            if(id != my_id)
                                cw_futures.push_back(hpx::async<action_type>(id));    
                        }

                        //hpx::wait(cw_futures);

                        bool result = false;
                        BOOST_FOREACH(hpx::lcos::unique_future<bool> &f, cw_futures)
                        {   
                            if(f.get() == true)
                                terminate = false;
                        }
                        if(terminate)
                            return false;
                }
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
                mutex_type::scoped_lock lk(localq_mtx); //##################
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
            std::vector<hpx::unique_future<void> > gen_children_futures;
            gen_children_futures.reserve(param.chunk_size);
            while(get_work(parents))
            {
                BOOST_FOREACH(node & parent, parents)
                {
                    gen_children_futures.push_back(
                        hpx::async(&ws_stealstack::gen_children, this
                            , boost::ref(parent))
                    );
                }
                parents.clear();
                hpx::wait(gen_children_futures);
                gen_children_futures.clear();
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
        boost::atomic<std::size_t> sharedq_work;
        hpx::id_type my_id;
        hpx::id_type sharedq_id;

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
