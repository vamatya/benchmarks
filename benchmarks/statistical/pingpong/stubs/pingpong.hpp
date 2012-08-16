//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#ifndef _PINGPONG_STUBS_HPP
#define _PINGPONG_STUBS_HPP

#include "../server/pingpong.hpp"

namespace hpx { namespace components { namespace stubs
{

    struct pingpong : stub_base<server::pingpong>
    {

    static int init(id_type gid, uint64_t n, double o, id_type d){
        return hpx::async<server::pingpong::init_action>(gid,n,o,d).get();
    }

    static void set_time(id_type gid, double t){
        hpx::async<server::pingpong::set_action>(gid,t);
    }

    static void pong(id_type gid, double t){
        hpx::async<server::pingpong::pong_action>(gid,t);
    }

    static int ping(id_type gid){
        return hpx::async<server::pingpong::ping_action>(gid).get();
    }
};

}}}

#endif

