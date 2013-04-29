//  Copyright (c) 2013 Thomas Heller
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#define SKIP 200
#define SKIP_LARGE 10
#define LARGE_MESSAGE_SIZE 8192
#define ITERATIONS_LARGE 100

struct params
{
    std::size_t max_msg_size;
    std::size_t iterations;
    std::size_t chunk_size;
};

boost::program_options::options_description params_desc()
{
    boost::program_options::options_description
        desc("Usage: " HPX_APPLICATION_STRING " [options]");

    desc.add_options()
        ("max-msg-size",
         boost::program_options::value<std::size_t>()->default_value(1048576),
         "Set maximum message size in bytes.")
        ("iter",
         boost::program_options::value<std::size_t>()->default_value(1000),
         "Set number of iterations per message size.")
        ("chunk-size",
         boost::program_options::value<std::size_t>()->default_value(2),
         "Set number of iterations per message size.")
        ;

    return desc;
}

params process_args(boost::program_options::variables_map & vm)
{
    params p
        = {
            vm["max-msg-size"].as<std::size_t>()
          , vm["iter"].as<std::size_t>()
          , vm["chunk-size"].as<std::size_t>()
        }; 

    return p;
}

void print_header(std::string const & benchmark)
{
    hpx::cout << "# " << benchmark << hpx::endl
              << "# Size    Latency (microsec)" << hpx::endl
              << hpx::flush;
}

void print_data(double elapsed, std::size_t size, std::size_t iterations)
{
    hpx::cout << std::left << std::setw(10) << size
              << elapsed
              << hpx::endl << hpx::flush;
}
