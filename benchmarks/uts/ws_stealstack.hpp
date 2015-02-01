
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
            typename std::deque<T>::iterator itr_e = data.end();
            mutex_type::scoped_lock lk(mtx);
            data.insert(itr_e, in_start_, in_end_);
        }

        template<typename InputIterator>
        void insert_begin(InputIterator in_start_, InputIterator in_end_)
        {
            typename std::deque<T>::iterator itr_b = data.begin();
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
          , num_loc_(0)
          , local_stk_ids()
        {
        }

        typedef hpx::lcos::local::spinlock mutex_type;
        mutex_type local_work_mtx;
        mutex_type shared_work_mtx;
        
        void init(params p, std::size_t r, std::size_t s, hpx::naming::id_type id, std::size_t num_loc)
        {
            rank = r;
            size = s;
            param = p;
            my_id = id;
            num_loc_ = num_loc;
            
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
            //boost::uint32_t my_loc_id = hpx::naming::get_locality_from_id(my_id);
            hpx::id_type my_loc_id = hpx::naming::get_locality_from_id(my_id);
            std::size_t remote_count = num_loc_ - 1;
            //bool inserted = false;
            BOOST_FOREACH(hpx::id_type t_id, idss)
            {
                bool inserted = false;
                               
                hpx::id_type temp_loc_id = hpx::naming::get_locality_from_id(t_id);
                if(my_loc_id ==  temp_loc_id)
                {
                    //hpx::id_type here = hpx::find_here();
                    //BOOST_ASSERT(hpx::naming::get_locality_from_id(here) == hpx::naming::get_locality_from_id(my_id));
                    //BOOST_ASSERT(here.get_lsb() != my_id.get_lsb());
                    if(my_id != t_id)
                    {
                        local_stk_ids.push_back(t_id);
                        inserted = true;
                    }
                }
                else
                {
                    if(remote_stks_vec.size() == 0)
                    {
                        remote_stks_type temp_node_stacks(1,t_id);
                        remote_stks_vec.push_back(temp_node_stacks);
                        --remote_count;
                        inserted = true;
                    }

                    BOOST_FOREACH(remote_stks_type &rnode_stks, remote_stks_vec)
                    {
                        if(hpx::naming::get_locality_from_id(rnode_stks[0]) 
                            == temp_loc_id)
                        {
                            rnode_stks.push_back(t_id);
                            inserted = true;
                        }
                    }

                    if(!inserted)
                    {
                        remote_stks_type temp_node_stacks(1,t_id);
                        remote_stks_vec.push_back(temp_node_stacks);
                        --remote_count;
                        inserted = true;
                    }
                }
            }
            BOOST_ASSERT(remote_count == 0);
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

        

        //For Termination Detection. 
        bool check_work()
        {
            //mutex_type::scoped_lock lk(check_work_mtx);	//##################
            if(shared_q_.size() > 0 || local_q_.size() > 0)
                return true;
            else 
                return false;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, check_work);

        /////////////////////////////////////////////////////////////////////
        bool terminate_remote_check_work()
        {
            bool work_present = false;
            if(shared_q_.size() > 0 || local_q_.size() > 0)
            {
                return true;
            }
            else  // check other threads on the same node
            {
                typedef std::vector<hpx::future<bool> > vec_fut_type;
                vec_fut_type check_work_futvec, chk_wrk_fvec_temp;
                typedef components::ws_stealstack::check_work_action action_type;
                BOOST_FOREACH(hpx::id_type id, local_stk_ids)
                {
                    check_work_futvec.push_back(hpx::async<action_type>(id));
                }
                
                while(check_work_futvec.size() != 0)
                {
                    hpx::future<hpx::when_any_result<vec_fut_type> > temp_result_fut;
                    temp_result_fut = hpx::when_any(check_work_futvec);

                    hpx::when_any_result<vec_fut_type> temp_result
                        = boost::move(temp_result_fut.get());
                    //hpx::future<vec_fut_type> temp_fut = hpx::when_any(
                    //    check_work_futvec);

                    //chk_wrk_fvec_temp = boost::move(temp_result.futures);//temp_fut.get());
                    //bool tmp = chk_wrk_fvec_temp.back().get();
                    bool tmp = (temp_result.futures.at(temp_result.index)).get();

                    if(tmp)
                    {
                        work_present = true;
                        break;
                    }
                    
                    //vec_fut_type::iterator itr = temp_result.futures.begin() + temp_result.index;
                    temp_result.futures.erase(temp_result.futures.begin() + temp_result.index);
                    //chk_wrk_fvec_temp.pop_back();
                    //check_work_futvec = boost::move(chk_wrk_fvec_temp);
                    check_work_futvec = boost::move(temp_result.futures);
                }
                if(work_present)
                    return true;
            }
            return false;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, terminate_remote_check_work);
        /////////////////////////////////////////////////////////////////////

        //For Local Work Check
        hpx::util::tuple<bool,hpx::id_type> lcl_check_work()
        {
            if(shared_q_.size() > 0)//param.chunk_size)
                return boost::move(hpx::util::make_tuple(true,my_id));
            else
                return boost::move(hpx::util::make_tuple(false,my_id));
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, lcl_check_work);

        // Report Total work available for stealing
        // For Remote Stealing
        hpx::util::tuple<std::size_t,hpx::id_type> shared_que_size()
        {
            return boost::move(hpx::util::make_tuple(shared_q_.size(), my_id));
        }
        
        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, shared_que_size);
        
        /// steal only chunk size
        //std::pair<bool, std::vector<stealstack_node> > steal_work()
        //std::pair<bool, std::vector<node> > ssteal_work(id)
        std::pair<bool, std::vector<node> > lcl_steal_work()
        {   
            std::pair<bool, std::vector<node> > res =
                std::make_pair(false, std::vector<node>());
            std::size_t steal_num = 0;
            /// For within node work stealing, steal chunk size work only
            if(sharedq_work >= param.chunk_size)
            {
               if(sharedq_work >= max_localq_size)
               {
                   steal_num = max_localq_size/2;
               }
               else if(sharedq_work >= 2 * param.chunk_size)
               {
                   bool found_cvalue = false;
                   std::size_t temp = max_localq_size;
                   while(!found_cvalue)
                   {
                       std::size_t temp1 = temp/2;
                       if(sharedq_work > temp1)
                       {
                           steal_num = temp1;
                           found_cvalue = true;
                       }
                       temp = temp1;
                   }
               }
               else
               {
                   steal_num = param.chunk_size;
               }
            }
            else
            {
                steal_num = sharedq_work;
                //res.second.resize(steal_num);
            }

            if(steal_num > 0)
            {            
                shared_q_.pop_back(res.second, steal_num);
                sharedq_work -= res.second.size();//steal_num;
            }

            if(res.second.size() !=0 )
            {
                res.first = true;
            }

            return res;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, lcl_steal_work);

        //remote check work, on each node
        hpx::util::tuple<bool, hpx::id_type> remote_check_work()
        {
            typedef hpx::util::tuple<bool, hpx::id_type> tup_type;
            tup_type res = hpx::util::make_tuple(false, my_id);
            //check shared_q work of the first destination 
            if(shared_q_.size() >= max_localq_size)
            {
                hpx::util::get<0>(res) = true;
                return res;
            }
            else
            {
                std::size_t cumulative_work = shared_q_.size();
                
                //check work in queues of same loc
                typedef hpx::util::tuple<std::size_t,hpx::id_type> data_type;
                typedef hpx::future<data_type> future_type;
                std::vector<future_type> chk_work_fut, chk_work_fut_temp;
                BOOST_FOREACH(hpx::id_type id, local_stk_ids)
                {
                    chk_work_fut.push_back(hpx::async<shared_que_size_action>(id));
                }

                while(chk_work_fut.size() !=0)
                {
                    typedef hpx::when_any_result<std::vector<future_type> > when_any_result_type;
                    hpx::future<when_any_result_type> temp_result_fut
                        = hpx::when_any(chk_work_fut);
                    //hpx::when_any_result<std::vector<future_type> > result_temp;
                    when_any_result_type temp_result 
                        = boost::move(temp_result_fut.get());
                    //chk_work_fut_temp = boost::move(fut_temp.get());

                    //chk_work_fut_temp = boost::move(temp_result.futures);

                    data_type chk_wrk = (temp_result.futures.at(
                                            temp_result.index)).get();
                        //chk_work_fut_temp.back().get();

                    if(hpx::util::get<0>(chk_wrk) >= max_localq_size)
                    {
                        hpx::util::get<0>(res) = true;
                        hpx::util::get<1>(res) = hpx::util::get<1>(chk_wrk);
                        break;
                    }
                    else
                    {
                        cumulative_work = hpx::util::get<0>(chk_wrk);
                    }

                    //Cumulative work on node is good to be stolen
                    if(cumulative_work > 2 * max_localq_size)
                    {
                        hpx::util::get<0>(res) = true;
                        break;
                    }
                    
                    //chk_work_fut_temp.pop_back();
                    temp_result.futures.erase(temp_result.futures.begin() + temp_result.index);
                    //chk_work_fut = boost::move(chk_work_fut_temp);
                    chk_work_fut = boost::move(temp_result.futures);
                }
                return res;
            }
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, remote_check_work);

        //////////////////////////////////////////////////////////////////////
        // steal up to max_local_q size from a remote stack. 
        // invoked within the remote node.
        hpx::util::tuple<bool, std::vector<node> > remote_steal_work()
        {
            hpx::util::tuple<bool, std::vector<node> > res = 
                hpx::util::make_tuple(false, std::vector<node>());

            if(sharedq_work > param.chunk_size) 
            {
                std::size_t steal_num = 0;
                if(sharedq_work > max_localq_size)
                {
                    steal_num = sharedq_work/2; //max_localq_size
                }
                else if(sharedq_work >= 2 * param.chunk_size)
                {   
                    // find chunk_value for remote stealing 
                    bool found_cvalue = false;

                    std::size_t temp = max_localq_size;
                    // find the right chunk-sizes to steal
                    while(!found_cvalue)
                    {       
                        // FIX ME: ?
                        std::size_t temp1 = temp/2;
                        if(sharedq_work > temp1 )
                        {
                            steal_num = temp1;
                            found_cvalue = true;
                        }
                        temp = temp1;
                    }
                }
                else
                {
                    steal_num = param.chunk_size;
                }

                shared_q_.pop_back(hpx::util::get<1>(res),steal_num);//max_localq_size);
                sharedq_work -= hpx::util::get<1>(res).size(); // max_localq_size
                
                if(hpx::util::get<1>(res).size() != 0)
                {
                    hpx::util::get<0>(res) = true;
                }
            }
            return res;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, remote_steal_work);

        /////////////////////////////////////////////////////////////////////////
        // Aggregate work steal from a node 
        hpx::util::tuple<bool, std::vector<node> > remote_aggregate_steal_work()
        {
            typedef  std::vector<node> node_vec_type;
            node_vec_type collect_work;
            typedef hpx::util::tuple<bool, std::vector<node> > result_type;
            result_type res = 
                hpx::util::make_tuple(false, std::vector<node>());

            result_type lcl_res = remote_steal_work();

            if(hpx::util::get<0>(lcl_res))
            {
                //hpx::util::get<1>(res) = hpx::util::get<1>(lcl_res);
                collect_work = hpx::util::get<1>(lcl_res);
            }

            if(collect_work.size() > max_localq_size)
            {
                hpx::util::get<0>(res) = true;
                hpx::util::get<1>(res) = collect_work;
                return res;
            }

            BOOST_FOREACH(hpx::id_type id, local_stk_ids)
            {
                hpx::future<result_type> temp_fut = hpx::async<remote_steal_work_action>(id);
                result_type temp_res = boost::move(temp_fut.get());
                
                if(hpx::util::get<0>(temp_res))
                {
                    node_vec_type temp_vec = hpx::util::get<1>(temp_res);
                    node_vec_type::iterator it_b = temp_vec.begin();
                    node_vec_type::iterator it_e = temp_vec.end();
                    
                    node_vec_type::iterator it_agg = collect_work.end();

                    collect_work.insert(it_agg, it_b, it_e);
                }
                if(collect_work.size() > 2 * max_localq_size)
                {
                    break;
                }
            }

            if(collect_work.size() != 0)
            {
                hpx::util::get<0>(res) = true;
                hpx::util::get<1>(res) = collect_work;
            }

            return res;
        }
        
        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, remote_aggregate_steal_work);

        //////////////////////////////////////////////////////////////////////
        /*hpx::util::tuple<bool,std::vector<node> > remote_node_share_work()
        {
            typedef hpx::util::tuple<bool, std::vector<node> > res_type;
            res_type res = 
                hpx::util::make_tuple(false, std::vector<node>());

            std::vector<hpx::future<res_type> > res_fut_vec;
            //res_fut_vec.push_back(hpx::async<remote_node_share_work>(my_id));

            return res;
        }

        HPX_DEFINE_COMPONENT_ACTION(ws_stealstack, remote_node_share_work);*/
        //////////////////////////////////////////////////////////////////////

        bool ensure_local_work()
        {   
            while(local_q_.size() == 0)
            //while(local_work == 0)
            {   
                bool terminate = true; 

                // steal work from own shared q first 
                typedef std::pair<bool, std::vector<node> > node_pair_type;
                node_pair_type node_pair(lcl_steal_work());

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
                            check_work_futvec.push_back(hpx::async<lcl_check_work_action>(t_id));
                        }

                        std::size_t fv_count = check_work_futvec.size();

                        while(fv_count != 0)
                        {
                            bool break_ = false;
                            
                            typedef hpx::when_any_result<std::vector<future_type> > when_any_result_type;
                            hpx::future<when_any_result_type> temp_result_fut
                                = hpx::when_any(check_work_futvec);

                            when_any_result_type temp_result 
                                = boost::move(temp_result_fut.get());

                            
                            //fut_temp = hpx::when_any(check_work_futvec);
                            //cw_futvec_temp = boost::move(temp_result.futures);//fut_temp.get());
                            hpx::util::tuple<bool, hpx::id_type> check_data =
                                (temp_result.futures.at(temp_result.index)).get();
                                //cw_futvec_temp.back().get();

                            //if(std::get<0>(check_data))
                            if(hpx::util::get<0>(check_data))
                            {
                                //last steal
                                hpx::id_type l_steal_temp = hpx::util::get<1>(check_data);
                                typedef ws_stealstack::lcl_steal_work_action action_type;
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
                                //cw_futvec_temp.pop_back();
                                temp_result.futures.erase(
                                    temp_result.futures.begin() + temp_result.index);
                                check_work_futvec = boost::move(temp_result.futures);
                                    //boost::move(cw_futvec_temp);
                                
                                --fv_count;
                                //cw_futvec_temp = hpx::when_any(check_work_futvec);
                            }
                        }
                    }                   
                }

                BOOST_ASSERT(local_work == local_q_.size());

                //Distributed Work Stealing? (Latency might cause inability to work stealing. 
                //Get Work from other nodes
                if(num_loc_ > 1)
                {
                    if(terminate && local_work < 1)
                    {
                        BOOST_ASSERT(local_work == 0 && sharedq_work == 0);

                        typedef hpx::util::tuple<bool, std::vector<node> > node_vec_result_type;
                        typedef hpx::util::tuple<bool, hpx::id_type> data_type;
                        typedef hpx::future<data_type> future_type;
                        std::vector<future_type> chk_rwork_fvec, chk_rwork_fvec_temp;

                        hpx::future<std::vector<future_type> > fut_temp;
                        data_type temp;

                        BOOST_FOREACH(std::vector<hpx::id_type> rmt_node, remote_stks_vec)
                        {
                            chk_rwork_fvec.push_back(hpx::async<
                                remote_check_work_action>(rmt_node.front()));                         
                        }

                        bool break_loop = false;
                        while(chk_rwork_fvec.size() != 0)
                        {
                            typedef hpx::when_any_result<std::vector<future_type> > when_any_result_type;
                            hpx::future<when_any_result_type> temp_result_fut
                                = hpx::when_any(chk_rwork_fvec);
                            when_any_result_type temp_result
                                = boost::move(temp_result_fut.get());

                            //fut_temp = hpx::when_any(chk_rwork_fvec);
                            //chk_rwork_fvec_temp = boost::move(temp_result.futures);//fut_temp.get());
                            temp = (temp_result.futures.at(temp_result.index)).get();
                                //chk_rwork_fvec_temp.back().get();

                            //if work present, try to steal from that remote id/node collective
                            if(hpx::util::get<0>(temp))
                            {
                                hpx::future<node_vec_result_type> fut_res;
                                node_vec_result_type res_work_vec;

                                fut_res = hpx::async<
                                    remote_aggregate_steal_work_action>(
                                        hpx::util::get<1>(temp));
                                res_work_vec = boost::move(fut_res.get());
                                if(hpx::util::get<0>(res_work_vec))
                                {
                                    break_loop = true;
                                    local_q_.push_back(hpx::util::get<1>(res_work_vec));
                                    local_work += local_q_.size();
                                }
                            }
                            if(local_work > 1)
                            {
                                terminate = false;
                            }
                            if(break_loop)
                            {
                                break;
                            }

                            //chk_rwork_fvec_temp.pop_back();
                            temp_result.futures.erase(
                                temp_result.futures.begin() + temp_result.index);
                            chk_rwork_fvec = boost::move(temp_result.futures);
                                //boost::move(chk_rwork_fvec_temp);
                        }
                    }

                }
                
                
                //Termination Detection Phase
                //Check if there is any work remaining throughout, if stealing unsuccessful. 
                //Check if work is present within other SS within the node
                //if(terminate && local_q_.size() < 1)
                if(terminate && local_work < 1)
                {
                    std::vector<hpx::future<bool> > check_work_futures;
                    check_work_futures.reserve(local_stk_ids.size());
                    //std::cout << "Local Stacks Ids:" << local_stk_ids.size();
                    typedef components::ws_stealstack::check_work_action action_type;

                    /*BOOST_FOREACH(hpx::id_type id, ids)
                    {
                        if(id != my_id)
                        {
                            check_work_futures.push_back(hpx::async<action_type>(id));
                        }
                    }*/
                    BOOST_FOREACH(hpx::id_type t_id, local_stk_ids)
                    {
                        check_work_futures.push_back(hpx::async<action_type>(t_id));
                    }
                                
                    std::size_t fv_count = check_work_futures.size();
                    hpx::future<std::vector<hpx::future<bool> > > fut_temp;
                    std::vector<hpx::future<bool> > cw_futures_temp;
                    while(check_work_futures.size() != 0)
                    {
                        bool break_ = false;

                        typedef hpx::when_any_result<std::vector<hpx::future<bool> > > when_any_result_type;
                        hpx::future<when_any_result_type> temp_result_fut
                            = hpx::when_any(check_work_futures);
                        when_any_result_type temp_result
                            = boost::move(temp_result_fut.get());

                        //fut_temp = hpx::when_any(check_work_futures);
                        //cw_futures_temp = boost::move(temp_result.futures);//fut_temp.get());
                        bool work_present = (temp_result.futures.at(temp_result.index)).get();
                            //cw_futures_temp.back().get();
                        
                        if(work_present)
                        {
                            terminate = false;
                            break;
                            //last steal
                        }
                        else
                        {
                            //cw_futures_temp.pop_back();
                            temp_result.futures.erase(
                                temp_result.futures.begin() + temp_result.index);
                            check_work_futures = boost::move(temp_result.futures);
                                //boost::move(cw_futures_temp);

                            --fv_count;
                            //cw_futvec_temp = hpx::when_any(check_work_futvec);
                        }
                    }
                    
                    // Remote Nodes termination Detection
                    BOOST_ASSERT(local_work == 0);
                    BOOST_ASSERT(local_work == local_q_.size());

                    if(num_loc_ > 1)
                    {
                        if(terminate && local_work < 1)
                        {
                            typedef std::vector<hpx::future<bool> > vec_fut_type;
                            vec_fut_type check_work_futures, chk_wrk_fut_temp;
                            typedef components::ws_stealstack::terminate_remote_check_work_action action_type;

                            BOOST_FOREACH(std::vector<hpx::id_type> node, remote_stks_vec)
                            {
                                check_work_futures.push_back(hpx::async<
                                    action_type>(node.front()));
                                
                            }
                            while(check_work_futures.size() != 0)
                            {

                                typedef hpx::when_any_result<vec_fut_type> when_any_result_type;
                                hpx::future <when_any_result_type> temp_result_fut
                                    = hpx::when_any(check_work_futures);
                                when_any_result_type temp_result
                                    = temp_result_fut.get();


                                //hpx::future<vec_fut_type> tmp = 
                                    //hpx::when_any(check_work_futures);
                                
                                //chk_wrk_fut_temp = boost::move(temp_result.futures);//tmp.get());
                                bool work_present = (temp_result.futures.at(
                                                        temp_result.index)).get();
                                    //chk_wrk_fut_temp.back().get();

                                if(work_present)
                                {
                                    terminate = false;
                                    break;
                                }
                                //chk_wrk_fut_temp.pop_back();
                                temp_result.futures.erase(
                                    temp_result.futures.begin() + temp_result.index);
                                check_work_futures = boost::move(temp_result.futures);
                                    //boost::move(chk_wrk_fut_temp);
                            }
                            
                        }
                    }

                }

                //std::cout << "Local Q size: " << local_q_.size() << ", Shared Q size: " <<shared_q_.size() << ", Rank: " << rank << " , terminate: " << terminate << std::endl;
   
                if(terminate)
                {
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
        typedef std::vector<hpx::id_type> remote_stks_type;
        std::vector<remote_stks_type> remote_stks_vec;

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
        std::size_t num_loc_;

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

///////////////////////////////////////////////////////////////////////////
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
