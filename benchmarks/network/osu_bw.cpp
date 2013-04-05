//  Copyright (c) 2013 Hartmut Kaiser
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

#include <hpx/hpx_main.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/iostreams.hpp>
#include <hpx/util/serialize_buffer.hpp>

#include <boost/assert.hpp>
#include <boost/shared_ptr.hpp>

///////////////////////////////////////////////////////////////////////////////
void isend(hpx::util::serialize_buffer<char> const& )
{
}
HPX_PLAIN_ACTION(isend);

///////////////////////////////////////////////////////////////////////////////
void print_header ()
{
    hpx::cout << "# OSU HPX Bandwidth Test\n"
              << "# Size    Bandwidth (MB/s)\n"
              << hpx::flush;
}

///////////////////////////////////////////////////////////////////////////////
char* align_buffer (char* ptr, unsigned long align_size)
{
    return (char*)(((std::size_t)ptr + (align_size - 1)) / align_size * align_size);
}

#if defined(BOOST_MSVC)
unsigned long getpagesize()
{
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwPageSize;
}
#endif

///////////////////////////////////////////////////////////////////////////////
#define LOOP_LARGE  20
#define WINDOW_SIZE_LARGE  64
#define SKIP_LARGE  2
#define LARGE_MESSAGE_SIZE  8192
#define MAX_MSG_SIZE (1<<22)
#define MAX_ALIGNMENT 65536
#define MYBUFSIZE (MAX_MSG_SIZE + MAX_ALIGNMENT)

char send_buffer[MYBUFSIZE];
char receive_buffer[MYBUFSIZE];

void run_benchmark()
{
    // use the first remote locality to bounce messages, if possible
    hpx::id_type dest = hpx::find_here();
    std::vector<hpx::id_type> localities = hpx::find_remote_localities();
    if (!localities.empty())
        dest = localities[0];

    unsigned long align_size = getpagesize();
    int loop = 100;
    int window_size = 64;
    int skip = 10;

    char* aligned_send_buffer = align_buffer(send_buffer, align_size);
    char* aligned_receive_buffer = align_buffer(receive_buffer, align_size);

    BOOST_ASSERT(align_size <= MAX_ALIGNMENT);
    for(std::size_t size = 1; size <= MAX_MSG_SIZE; size *= 2)
    {
        std::memset(aligned_send_buffer, 'a', size);
        std::memset(aligned_receive_buffer, 'b', size);

        if(size > LARGE_MESSAGE_SIZE) {
            loop = LOOP_LARGE;
            skip = SKIP_LARGE;
            window_size = WINDOW_SIZE_LARGE;
        }

        hpx::util::high_resolution_timer t;
        for (int i = 0; i != loop + skip; ++i) {
            if (i == skip)
                t.restart();

            std::vector<hpx::future<void> > lazy_results;
            lazy_results.reserve(window_size);

            isend_action send;
            for (int j = 0; j < window_size; ++j)
            {
                lazy_results.push_back(hpx::async(send, dest,
                    hpx::util::serialize_buffer<char>(aligned_send_buffer, size)));
            }

            hpx::wait_all(lazy_results);
        }

        double elapsed = t.elapsed();
        double bw = (size / 1e6 * loop * window_size) / elapsed;
        hpx::cout << std::left << std::setw(10) << size << bw << hpx::endl << hpx::flush;
    }
}

int main(int argc, char* argv[])
{
    print_header();
    run_benchmark();
    return 0;
}
