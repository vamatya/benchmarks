
#include <hpx/runtime/components/component_factory.hpp>

#include <hpx/util/portable_binary_iarchive.hpp>
#include <hpx/util/portable_binary_oarchive.hpp>

#include <boost/serialization/version.hpp>
#include <boost/serialization/export.hpp>
#include "server/pingpong.hpp"

HPX_REGISTER_COMPONENT_MODULE();

typedef hpx::components::simple_component<hpx::components::server::pingpong>
    pingpong_type;

HPX_REGISTER_MINIMAL_COMPONENT_FACTORY_EX(
    hpx::components::simple_component<
        hpx::components::server::pingpong>, pingpong,
        hpx::components::factory_disabled)

HPX_REGISTER_ACTION_EX(
    pingpong_type::wrapped_type::init_action, hpx_init_action)
HPX_REGISTER_ACTION_EX(
    pingpong_type::wrapped_type::set_action, hpx_set_action)
HPX_REGISTER_ACTION_EX(
    pingpong_type::wrapped_type::pong_action, hpx_pong_action)
HPX_REGISTER_ACTION_EX(
    pingpong_type::wrapped_type::ping_action, hpx_ping_action)

HPX_DEFINE_GET_COMPONENT_TYPE(pingpong_type::wrapped_type);

