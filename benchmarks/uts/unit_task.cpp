
#include <benchmarks/uts/uts.hpp>
#include <benchmarks/uts/params.hpp>
//#include <benchmarks/uts/ws_stealstack.hpp>

int hpx_main(boost::program_options::variables_map & vm)
{
    params p(vm);

    int temp = 0;
    int max_children_count = 0;
    node parent;
    parent.init_root(p);
    hpx::util::high_resolution_timer t;

    for(int i = 0; i < 1000000; i++)
    {
        int children_count = parent.get_num_children(p);
        if(children_count > max_children_count)
            max_children_count = children_count;
    }

    double elapsed = t.elapsed();
    std::cout << "get_num_children() total time:" << elapsed << ", max_children_count:"
              << max_children_count 
              << ", unit task_time:" << elapsed/1000000 
              << std::endl;
    
    t.restart();
    
    for(int i = 0; i < 1000000; i++)
    {
        node child;
        rng_spawn(parent.state.state, child.state.state, i);
        parent = child;
    }

    elapsed = t.elapsed();

    std::cout << "rng_spawn() total time:" << elapsed << ", unit task_time:" 
            << elapsed/1000000
            << std::endl;

    return hpx::finalize();   
}

int main(int argc, char* argv[])
{
    boost::program_options::options_description desc = uts_params_desc();
    return hpx::init(desc, argc, argv);
}
