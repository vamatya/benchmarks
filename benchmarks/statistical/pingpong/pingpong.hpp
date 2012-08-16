//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef _PINGPONG_MAIN_HPP
#define _PINGPONG_MAIN_HPP

#include "stubs/pingpong.hpp"

namespace hpx { namespace components
{

    class pingpong : 
        public client_base<pingpong, stubs::pingpong>
    {
    typedef client_base<pingpong, stubs::pingpong> base_type;
    public:
        pingpong(){}
        pingpong(id_type gid) : base_type(gid){}
        
        int init(uint64_t n, double o, pingpong* other){
            BOOST_ASSERT(gid_);
            return base_type::init(gid_,n,o,other->get_gid_());
        }

        void set_time(double t){
            BOOST_ASSERT(gid_);
            base_type::set_time(gid_, t);
        }

        void pong(double t){
            BOOST_ASSERT(gid_);
            base_type::pong(gid_, t);
        }

        void ping(){
            BOOST_ASSERT(gid_);
            base_type::ping(gid_);
        }
        
        id_type get_gid_(){
            return gid_;
        }
    };
}}

#endif
