
//  Copyright (c) 2013 Thomas Heller
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

/*******************************************************************************
 *
 *
 *
 ******************************************************************************/

#include <benchmarks/uts/uts.hpp>

int hpx_main(boost::program_options::variables_map & vm)
{
    params p(vm);
    
    p.print("Workmanager");

    return hpx::finalize();
}

int main(int argc, char* argv[])
{
    boost::program_options::options_description desc = uts_params_desc();
    return hpx::init(desc, argc, argv);
}
