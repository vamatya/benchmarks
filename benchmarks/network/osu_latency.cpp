//  Copyright (c) 2013 Hartmut Kaiser
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// Bidirectional network bandwidth test

#include <hpx/hpx_main.hpp>
#include <hpx/hpx.hpp>
#include <hpx/include/iostreams.hpp>
#include <hpx/util/serialize_buffer.hpp>

#include <boost/assert.hpp>
#include <boost/shared_ptr.hpp>

///////////////////////////////////////////////////////////////////////////////
#define LOOP_SMALL  10000
#define SKIP_SMALL  1000

#define LOOP_LARGE  100
#define SKIP_LARGE  10

#define LARGE_MESSAGE_SIZE  8192

#define MAX_MSG_SIZE (1<<22)
#define MAX_ALIGNMENT 65536
#define SEND_BUFSIZE (MAX_MSG_SIZE + MAX_ALIGNMENT)

char send_buffer[SEND_BUFSIZE];

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
hpx::util::serialize_buffer<char>
isend(hpx::util::serialize_buffer<char> const& receive_buffer)
{
    return receive_buffer;
}
HPX_PLAIN_ACTION(isend);

///////////////////////////////////////////////////////////////////////////////
double ireceive(hpx::naming::id_type dest, std::size_t size)
{
    int loop = LOOP_SMALL;
    int skip = SKIP_SMALL;

    if (size > LARGE_MESSAGE_SIZE) {
        loop = LOOP_LARGE;
        skip = SKIP_LARGE;
    }

    // align used buffers on page boundaries
    unsigned long align_size = getpagesize();
    BOOST_ASSERT(align_size <= MAX_ALIGNMENT);

    char* aligned_send_buffer = align_buffer(send_buffer, align_size);
    std::memset(aligned_send_buffer, 'a', size);

    hpx::util::high_resolution_timer t;

    isend_action send;
    for (int i = 0; i != loop + skip; ++i) {
        // do not measure warm up phase
        if (i == skip)
            t.restart();

        typedef hpx::util::serialize_buffer<char> buffer_type;
        send(dest, buffer_type(aligned_send_buffer, size,
            buffer_type::reference));
    }

    double elapsed = t.elapsed();
    return (elapsed * 1e6) / (2 * loop);
}
HPX_PLAIN_ACTION(ireceive);

///////////////////////////////////////////////////////////////////////////////
void print_header ()
{
    hpx::cout << "# OSU HPX Latency Test\n"
              << "# Size    Bandwidth (microsec)\n"
              << hpx::flush;
}

///////////////////////////////////////////////////////////////////////////////
void run_benchmark()
{
    // use the first remote locality to bounce messages, if possible
    hpx::id_type here = hpx::find_here();

    hpx::id_type there = here;
    std::vector<hpx::id_type> localities = hpx::find_remote_localities();
    if (!localities.empty())
        there = localities[0];

    // perform actual measurements
    ireceive_action receive;
    for (std::size_t size = 1; size <= MAX_MSG_SIZE; size *= 2)
    {
        double latency = receive(there, here, size);
        hpx::cout << std::left << std::setw(10) << size
                  << latency << hpx::endl << hpx::flush;
    }
}

int main(int argc, char* argv[])
{
    print_header();
    run_benchmark();
    return 0;
}
