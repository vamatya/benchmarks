
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

#include <exception>
#include <boost/ref.hpp>
#include <boost/iterator/counting_iterator.hpp>
#include <boost/range/counting_range.hpp>
#include <boost/range/algorithm.hpp>
#include <tuple>

namespace components
{

    struct empty_queue: std::exception
    {
        const char* what() const throw();
    };

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

        void pop_back(std::vector<T>& nodes, std::size_t steal_num)
        {
            mutex_type::scoped_lock lk(mtx);
            if(data.empty()) throw std::logic_error(
                "Trying to pop from empty queue");
            
            std::size_t shared_q_size = data.size();

            if(shared_q_size < steal_num)//nodes.size())
            {
                nodes.resize(shared_q_size);
            }
            else
            {
                nodes.resize(steal_num);
            }
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

        template<typename InputIterator>
        void insert_end(InputIterator in_start_, InputIterator in_end_)
        {
            std::deque<T>::iterator itr_e = data.end();
            mutex_type::scoped_lock lk(mtx);
            data.insert(itr_e, in_start_, in_end_);
        }

        template<typename InputIterator>
        void insert_begin(InputIterator in_start_, InputIterator in_end_)
        {
            std::deque<T>::iterator itr_b = data.begin();
            mutex_type::scoped_lock lk(mtx);
            data.insert(itr_b, in_start_, in_end_);
        }
    };

    template<typename R, typename NODE, typename P, typename WS_SS>
    struct spawn_work
    {
    public: 
        typedef void result_type;

        template <typename FN>
        struct result;

        template <typename FN, typename RN, typename ND, typename GR, typename SSN>
        struct result<FN(RN, ND, GR, SSN)>
        {
            typedef void type;
        };

        //typename result<spawn_work(R,NODE,P, WS_SS)>::type
        result_type
            operator()(R const& range, NODE& parent, P& param_, WS_SS this_ss);
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
          , local_stk_ids()
          , remote_stk_ids()
        {
        }

        typedef hpx::lcos::local::spinlock mutex_type;
        mutex_type local_work_mtx;
        mutex_type shared_work_mtx;
        
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
                std::vector<node> n_vec(1,n);
                //n_vec.push_back(n);
                //put_work(n);
                put_work(n_vec);
            }
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, init);

        void resolve_names(std::vector<hpx::id_type> const & idss)
        {
            ids = idss;
            BOOST_FOREACH(hpx::id_type t_id, idss)
            {
                if(my_id != t_id)
                {
                    if(my_id.get_msb() == t_id.get_msb())
                    {
                        local_stk_ids.push_back(t_id);
                    }
                    else
                    {
                        remote_stk_ids.push_back(t_id);
                    }
                }
            }
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, resolve_names);

        //void put_work_sharedq(std::vector<stealstack_node>& work_shared)
        void put_work_sharedq(std::vector<node>& work_shared)
        {
            std::size_t temp_size = work_shared.size();
            shared_q_.push_back(work_shared);
            sharedq_work += temp_size;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, put_work_sharedq);

        void put_work(std::vector<node> const & n)
        {   
            mutex_type::scoped_lock lk(local_work_mtx);
            {            
                std::vector<node>::const_iterator itr_b = n.begin();
                std::vector<node>::const_iterator itr_e = n.end();
                //if(local_q_.size() < max_localq_size)
                if(local_work < max_localq_size)
                {  
                    //local_q_.push_back(n);                    
                    local_q_.insert_end(itr_b, itr_e);
                    local_work += n.size();
                                
                    stat.max_stack_depth = (std::max)(local_work + sharedq_work, stat.max_stack_depth);
                }
                else
                {
                    //std::vector<stealstack_node> ss_nshare;
                    std::vector<node> nshare;
                    //std::size_t temp_count = 0;
                    //TO DO: 
                    BOOST_ASSERT(!local_q_.empty());
                    //std::size_t transfer_size = local_q_.size()/2;
                    std::size_t transfer_size = local_work - (max_localq_size/2);//local_work/2;
                    nshare.resize(transfer_size);
                    local_q_.pop_back(nshare);

                    local_work -= nshare.size();//transfer_size;
                    put_work_sharedq(nshare);
                    //local_q_.push_back(n);
                    local_q_.insert_end(itr_b, itr_e);
                    local_work += n.size();

                    stat.max_stack_depth = (std::max)(local_work + sharedq_work, stat.max_stack_depth);
                }
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
                if(num_children < MAX_SPAWN_GRANULARITY)
                {
                    //accumulate all generated childen nodes
                    std::vector<node> child_nodes;
                    for(int i = 0; i < num_children; ++i)
                    {
                        node child;
                        child.type = child_type;
                        child.height = parent_height + 1;
                        for(int j = 0; j < param.compute_granularity; ++j)
                        {
                            rng_spawn(parent.state.state, child.state.state, i);
                        }
                        //put_work(child);
                        child_nodes.push_back(child);
                    }
                    put_work(child_nodes);
                    child_nodes.clear();
                }
                else
                {
                    typedef boost::iterator_range<boost::counting_iterator<std::size_t> >  count_itr_type;
                    std::vector<count_itr_type> ranges;

                    std::size_t chunk_commit = 0;
                    // std::size_t remainder = num_children;
                    while(chunk_commit != num_children)
                    {   
                        if((num_children - chunk_commit) <  2 * MAX_SPAWN_GRANULARITY)
                        {
                            std::size_t temp_var = (num_children - chunk_commit)/2;
                            ranges.push_back(boost::counting_range(chunk_commit, chunk_commit + temp_var));
                            chunk_commit += temp_var;
                            ranges.push_back(boost::counting_range(chunk_commit, (std::size_t)num_children));
                            chunk_commit += num_children - chunk_commit;
                        }
                        else
                        {
                            ranges.push_back(boost::counting_range(chunk_commit, chunk_commit + MAX_SPAWN_GRANULARITY));
                            chunk_commit += MAX_SPAWN_GRANULARITY;
                        }
                    }

                    typedef node node_type;
                    typedef components::ws_stealstack ws_stealstack_type;
                    typedef params param_type;

                    //spawn_work<count_itr_type, node_type, std::size_t, ss_node_type>  
                    std::vector<hpx::future<void> > fun_obj_futures; 
                    BOOST_FOREACH(count_itr_type rng, ranges)
                    {
                        spawn_work<count_itr_type, node_type, param_type, ws_stealstack_type*> sp_work;
                        fun_obj_futures.push_back(
                            hpx::async(
                                //HPX_STD_BIND(
                                std::bind(
                                    sp_work
                                    , rng
                                    , boost::ref(parent)
                                    , boost::ref(param)
                                    , this
                            )));
                    }
                    hpx::wait_all(fun_obj_futures);
                    fun_obj_futures.clear();
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
                //res.second.resize(steal_num);
                shared_q_.pop_back(res.second, steal_num);
                sharedq_work -= res.second.size();//steal_num;
            }
            else if(sharedq_work > 0)
            {
                std::size_t steal_num = sharedq_work;
                //res.second.resize(steal_num);
                shared_q_.pop_back(res.second, steal_num);
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
            if(shared_q_.size() > 0 || local_q_.size() > 0)
                return true;
            else 
                return false;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, check_work);

        hpx::util::tuple<bool,hpx::id_type> lcl_share_work()
        {
            if(shared_q_.size() > param.chunk_size)
                return boost::move(hpx::util::make_tuple(true,my_id));
            else
                return boost::move(hpx::util::make_tuple(false,my_id));
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, lcl_share_work);

        hpx::util::tuple<bool,hpx::id_type> remote_share_work()
        {
            if(shared_q_.size() > max_localq_size)
                return boost::move(hpx::util::make_tuple(true,my_id));
            else
                return boost::move(hpx::util::make_tuple(false,my_id));
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, remote_share_work);

        bool ensure_local_work()
        {   
            //while(local_q_.size() == 0)
            while(local_work == 0)
            {   
                bool terminate = true; 

                // steal work from own shared q first 
                typedef std::pair<bool, std::vector<node> > node_pair_type;
                node_pair_type node_pair(steal_work());

                // std::size_t temp_count = 0;
                if(node_pair.first)
                {   
                    BOOST_ASSERT(node_pair.second.size() != 0);
                    terminate = false;
                    
                    // depth first search
                    local_q_.push_back(node_pair.second);
                    local_work += node_pair.second.size();
                }
                

                /// check if local stealing was successful, else steal from others
                // Steal from other SS within the locality first
                //if(terminate && local_q_.size() < 1)  //local_work < 1)
                if(terminate && local_work < 1)
                {
                    //BOOST_ASSERT(local_work <= 0);
                    //BOOST_ASSERT(local_q_.size() == 0 && shared_q_.size() == 0);//sharedq_work == 0);
                    BOOST_ASSERT(local_work == 0 && sharedq_work == 0);

                    /// No work in local shared queue, look for work in other shared queue within same loc

                    {                
                        //typedef hpx::future<std::tuple<bool,hpx::id_type> > future_type;
                        typedef hpx::future<hpx::util::tuple<bool,hpx::id_type> > future_type;
                        std::vector<future_type> check_work_futvec, cw_futvec_temp;
                        
                        hpx::future<std::vector<future_type> > fut_temp;
                

                        BOOST_FOREACH(hpx::id_type t_id, local_stk_ids)
                        {
                            check_work_futvec.push_back(hpx::async<lcl_share_work_action>(t_id));
                        }

                        std::size_t fv_count = check_work_futvec.size();

                        while(fv_count != 0)
                        {
                            bool break_ = false;
                            
                            fut_temp = hpx::when_any(check_work_futvec);
                            cw_futvec_temp = boost::move(fut_temp.get());
                            hpx::util::tuple<bool, hpx::id_type> check_data = cw_futvec_temp.back().get();
                            //if(std::get<0>(check_data))
                            if(hpx::util::get<0>(check_data))
                            {
                                //last steal
                                hpx::id_type l_steal_temp = hpx::util::get<1>(check_data);
                                typedef ws_stealstack::steal_work_action action_type;
                                action_type act;
                                node_pair = boost::move(act(l_steal_temp));//ids[last_steal]));

                                
                                if(node_pair.first)
                                {                 
                                    BOOST_ASSERT(node_pair.second.size() != 0);
                                    terminate = false;
                                    break_ = true;

                                    //Depth First Search
                                    local_q_.push_back(node_pair.second);
                                    local_work += node_pair.second.size();
                                }
                            }
                            if(break_ || local_work > 0)
                            {
                                //l_steal = l_steal_temp;
                                break;
                            }
                            else
                            {
                                cw_futvec_temp.pop_back();
                                check_work_futvec = boost::move(cw_futvec_temp);
                                --fv_count;
                                //cw_futvec_temp = hpx::when_any(check_work_futvec);
                            }
                        }
                    }

                    /*for(std::size_t i = 0; i < size -1; ++i)
                    {   
                        if(last_steal == rank) last_steal = (last_steal + 1) % size;
                    
                        std::size_t last_steal_temp = last_steal;
                        typedef ws_stealstack::steal_work_action action_type;
                        action_type act;
                        node_pair = boost::move(act(ids[last_steal]));

                        bool break_ = false;
                        if(node_pair.first)
                        {                 
                            BOOST_ASSERT(node_pair.second.size() != 0);
                            terminate = false;
                            break_ = true;

                            //Depth First Search
                            local_q_.push_back(node_pair.second);
                            local_work += node_pair.second.size();
                        }
                    
                        //if(break_ || local_q_.size() > 0) 
                        if(break_ || local_work > 0) 
                        {
                            last_steal = last_steal_temp;
                            break;
                        }

                        last_steal = (last_steal + 1) % size;
                    }*/
                }

                //Steal Work from other nodes
                if(terminate && local_work < 1)
                {
                    
                }
                
                //Termination Detection Phase
                //Check if there is any work remaining throughout, if stealing unsuccessful. 
                //Check if work is present within other SS within the node
                //if(terminate && local_q_.size() < 1)
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

            local_q_.pop_back(work, param.chunk_size);

            std::size_t tmp_work_size = work.size();

            if(tmp_work_size == 0)
            {
                hpx::cout << "TreeSearch: get_work(): called with work.size() = 0, "
                    << "local_work=" << local_work
                    << " or " << (local_work % param.chunk_size)
                    << " (mod " << param.chunk_size << ")\n" << hpx::flush;
                throw std::logic_error("get_work(): Underflow!");
            }
            stat.n_nodes += tmp_work_size;
            local_work -= tmp_work_size;         

            return true;
        }

        void tree_search()
        {
            std::vector<node> parents;
            //std::vector<hpx::future<void> > gen_children_futures;
            //gen_children_futures.reserve(param.chunk_size);
            while(get_work(parents))
            {   
                BOOST_FOREACH(node & parent, parents)
                {
                    /*gen_children_futures.push_back(
                        hpx::async(&ws_stealstack::gen_children, this
                            , boost::ref(parent))
                    );*/
                    ws_stealstack::gen_children(boost::ref(parent));
                }
                parents.clear();
                //hpx::wait_all(gen_children_futures);
                /*BOOST_FOREACH(hpx::future<void>& fut_ch, gen_children_futures)
                {
                    fut_ch.get();
                }*/
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
        std::vector<hpx::id_type> local_stk_ids;
        std::vector<hpx::id_type> remote_stk_ids;

        boost::atomic<std::size_t> local_work;
        //std::size_t local_work;
        boost::atomic<std::size_t> sharedq_work;
        //std::size_t sharedq_work;

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

        std::tuple<bool,hpx::id_type> tup_obj;
    };

    template<typename R, typename NODE, typename P, typename WS_SS>
    typename spawn_work<R, NODE, P, WS_SS>::result_type
        spawn_work<R,NODE, P, WS_SS>::operator ()(R const& range, NODE& parent, P& param_, WS_SS this_ss)
    {
        int child_type = parent.child_type(param_);
        std::size_t parent_height = parent.height;
        std::vector<node> child_vec;
        BOOST_FOREACH(std::size_t i, range)
        {
            node child;
            child.type= child_type;
            child.height = parent_height + 1;

            for(int j = 0; j < param_.compute_granularity; ++j)
            {
                rng_spawn(parent.state.state, child.state.state,(int)i);
            }
            //std::vector<node> child_vec;
            child_vec.push_back(child);
        }
        this_ss->put_work(child_vec);
        
    }
}

#endif
