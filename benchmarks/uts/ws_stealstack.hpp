
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

    //struct empty_queue: std::exception
    //{
    //    const char* what() const throw();
    //};

    template<typename T>
    class threadsafe_deque : boost::noncopyable
    {
        typedef hpx::lcos::local::spinlock mutex_type;
    private:
        std::deque<T> data;
        std::size_t queue_size;
        mutex_type mtx;
        
    public:
        threadsafe_deque(){}
        threadsafe_deque(const threadsafe_deque& other)
        {
            mutex_type::scoped_lock lk(other.mtx);
            data = other.data;
        }

        // disable assignment operator 
        //threadsafe_deque& operator=(const threadsafe_deque&) = delete;
        void push_front(std::vector<T>& in_vec)
        {
            mutex_type::scoped_lock lk(mtx);
            BOOST_FOREACH(T& node, in_vec)
            {
                data.push_front(node);
            }
        }

        void push_back(std::vector<T>& in_vec)
        {
            mutex_type::scoped_lock lk(mtx);
            BOOST_FOREACH(T& node, in_vec)
            {
                data.push_back(node);
            }
        }
        void push_front(T in_val)
        {
            mutex_type::scoped_lock lk(mtx);
            data.push_front(in_val);
        }

        void push_back(T in_val)
        {
            mutex_type::scoped_lock lk(mtx);
            data.push_back(in_val);
        }

        void pop_front(std::vector<T>& nodes)
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw std::logic_error(
                "Trying to pop from empty queue");

            if(data.size() < nodes.size())
            {
                nodes.resize(0);
            }
            else
            {
                BOOST_FOREACH(T& nd, nodes)
                {
                    nd = data.front();
                    data.pop_front();
                }
            }
            
        }

        void pop_back(std::vector<T>& nodes)
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw std::logic_error(
                "Trying to pop from empty queue");

            if(data.size() < nodes.size())
            {
                nodes.resize(0);
            }
            else
            {
                BOOST_FOREACH(T& nd, nodes)
                {
                    nd = data.back();
                    data.pop_back();
                }
            }
            
        }

        void pop_front(T& value)
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw std::logic_error(
                "Trying to pop from empty queue");//empty_queue();
            value = data.front();
            data.pop_front();
        }

        void pop_back(T& value)
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw std::logic_error(
                "Trying to pop from empty queue");//empty_queue();
            value = data.back();
            data.pop_back();
        }

        std::shared_ptr<T> pop_front()
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw empty_queue();
            std::shared_ptr<T> const res(std::make_shared<T>(data.front()));
            data.pop_front();
            return res;
        }

        std::shared_ptr<T> pop_back()
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw empty_queue();
            std::shared_ptr<T> const res(std::make_shared<T>(data.back()));
            data.pop_back();
            return res;
        }

        bool empty() //const
        {
            mutex_type::scoped_lock lk(mtx);
            return data.empty();
        }

        std::size_t size() //const
        {
            mutex_type::scoped_lock lk(mtx);
            return data.size();
        }
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
          , local_q_()
          , shared_q_()
          , my_state(WORK)
        {
        }

        typedef hpx::lcos::local::spinlock mutex_type;
        
        void init(params p, std::size_t r, std::size_t s, hpx::naming::id_type id)
        {
            rank = r;
            size = s;
            param = p;
            my_id = id;
            
            last_steal = rank;

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

        //void put_work_sharedq(std::vector<stealstack_node>& work_shared)
        void put_work_sharedq(std::vector<node>& work_shared)
        {
            std::size_t temp_size = work_shared.size();
            shared_q_.push_front(work_shared);//, temp_size);
            sharedq_work += temp_size;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, put_work_sharedq);

        void put_work(node const & n)
        {   
            if(local_work < max_localq_size)
            {   
                local_q_.push_front(n);
                ++local_work;
                std::size_t local_work_tmp = local_work;
                stat.max_stack_depth = (std::max)(local_work_tmp, stat.max_stack_depth);
            }
            else
            {
                //std::vector<stealstack_node> ss_nshare;
                std::vector<node> nshare;
                std::size_t temp_count = 0;
                    
                BOOST_ASSERT(!local_q_.empty());
                std::size_t transfer_size = local_work/2; //local_q_.size()/2;
                nshare.resize(transfer_size);
                local_q_.pop_back(nshare);

               local_work -= nshare.size();//transfer_size;
               put_work_sharedq(nshare);
                local_q_.push_front(n);
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

        /// steal only chunk size
        //std::pair<bool, std::vector<stealstack_node> > steal_work()
        std::pair<bool, std::vector<node> > steal_work()
        {   
            std::pair<bool, std::vector<node> > res =
                std::make_pair(false, std::vector<node>());

            /// For within node work stealing, steal chunk size work only
            if(sharedq_work > param.chunk_size)
            {
                std::size_t steal_num = param.chunk_size;
                res.second.resize(steal_num);
                shared_q_.pop_back(res.second);
                sharedq_work -= res.second.size();//steal_num;
            }
            else if(sharedq_work > 0)
            {
                std::size_t steal_num = sharedq_work;
                res.second.resize(steal_num);
                shared_q_.pop_back(res.second);
                sharedq_work -= res.second.size();//steal_num;
            }

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
            if(sharedq_work > 0 || local_work > 0)
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
                typedef std::pair<bool, std::vector<node> > node_pair_type;
                node_pair_type node_pair(steal_work());

                std::size_t temp_count = 0;
                if(node_pair.first)
                {   
                    //mutex_type::scoped_lock lk(localq_mtx);
                    if(node_pair.second.size() != 0)
                    {
                        terminate = false;
                    }

                    local_q_.push_front(node_pair.second);
                    local_work += node_pair.second.size();
                }
                

                /// check if local stealing was successful, else steal from others
                if(terminate && local_work < 1)
                {
                    BOOST_ASSERT(local_work == 0 && sharedq_work == 0);

                    /// No work in local shared queue, look for work in other shared queue
                    for(std::size_t i = 0; i < size -1; ++i)
                    {   
                    
                        if(last_steal == rank) last_steal = (last_steal + 1) % size;
                    
                        std::size_t last_steal_temp = last_steal;
                        typedef ws_stealstack::steal_work_action action_type;
                        action_type act;
                        node_pair = boost::move(act(ids[last_steal]));

                        bool break_ = false;
                        if(node_pair.first)
                        {                                                       
                            if(node_pair.second.size() != 0)
                            {
                                terminate = false;
                                break_ = true;
                            }
                            else
                            {
                                throw std::logic_error(
                                    "Stolen work did not received. Remote queue probably popped.");
                            }

                            local_q_.push_front(node_pair.second);
                            local_work += node_pair.second.size();
                        }
                    
                        if(break_ || local_work > 0) 
                        {
                            last_steal = last_steal_temp;
                            break;
                        }

                        last_steal = (last_steal + 1) % size;
                    }
                }

                //Check if there is any work remaining throughout, if stealing unsuccessful. 
                if(terminate && local_work < 1)
                {
                    std::vector<hpx::future<bool> > check_work_futures;
                    typedef components::ws_stealstack::check_work_action action_type;

                    BOOST_FOREACH(hpx::id_type id, ids)
                    {
                        if(id != my_id)
                        {
                            check_work_futures.push_back(hpx::async<action_type>(id));
                        }
                    }
                                
                    BOOST_FOREACH(hpx::future<bool> & fut, check_work_futures)
                    {
                        if(fut.get())
                        {
                            terminate =  false;
                            break;
                        }
                        
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
            
            if(local_work > param.chunk_size)
            {
                work.resize(param.chunk_size);
                local_q_.pop_front(work);
            }
            else if(local_work > 0)
            {
                work.resize(local_work);
                local_q_.pop_front(work);
            }                                        
            stat.n_nodes += work.size();
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
            std::vector<hpx::future<void> > gen_children_futures;
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
                hpx::wait_all(gen_children_futures);
                //hpx::when_all(gen_children_futures).wait();
                /*BOOST_FOREACH(hpx::future<void>& fut_ch, gen_children_futures)
                {
                    fut_ch.get();
                }*/
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
        std::vector<hpx::id_type> local_cmp_ids;
        std::vector<hpx::id_type> remote_cmp_ids;

        boost::atomic<std::size_t> local_work;
        //std::size_t local_work;
        boost::atomic<std::size_t> sharedq_work;
        hpx::id_type my_id;

        stats stat;

        double walltime;
        double work_time;
        double search_time;
        double idle_time;
        int idle_sessions;
        double time_last;
        int entries[NSTATES];
        int curState;

        states my_state;
        double start_time;

        std::size_t max_localq_size;

        threadsafe_deque<node> local_q_;
        threadsafe_deque<node> shared_q_;

        std::size_t last_steal;
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
