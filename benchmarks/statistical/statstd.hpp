//  Copyright (c)      2012 Daniel Kogler
//
//  Distributed under the Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)


/*This file includes a number of functions used in all statistical benchmarks,
such as finding the mean, variance, min, and max times among a sample set.
Additionally, the printout command also creates a file with logarithmic
histogram data, for graphing purposes.*/

//TO-DO: allow histogram data generation to be disabled/enabled via command line
#ifndef STANDARD_STATISTICAL_FUNCTIONS
#define STANDARD_STATISTICAL_FUNCTIONS
#include <vector>
#include <string>
#include <hpx/hpx_init.hpp>
#include <hpx/include/lcos.hpp>
#include <hpx/include/actions.hpp>
#include <hpx/include/iostreams.hpp>
#include <hpx/include/components.hpp>
#include <hpx/util/high_resolution_timer.hpp>

using std::vector;
using std::string;
using boost::program_options::variables_map;
using boost::program_options::options_description;
using boost::uint64_t;
using hpx::util::high_resolution_timer;
using hpx::cout;
using hpx::flush;

bool csv;
string outname;

//find approximate overhead of instantiating and obtaining results from
//high_resolution_timer
double timer_overhead(uint64_t iterations){
    vector<double> record;
    double total = 0;
    record.reserve(iterations);
    for(uint64_t i = 0; i < iterations; i++){
        high_resolution_timer in_t;
        record.push_back(in_t.elapsed());
    }
    for(uint64_t i = 0; i < iterations; i++) total += record[i];
    if(!csv){
        cout<<"Using "<<iterations<<" iterations:\n";
        cout<<"\nAverage overhead of taking timed measurements: "<<
            total/iterations*1e9<<" ns\n";
        cout<<"NOTE - this value will be subtracted from all subsequent timings.\n";
        cout<<"       This value is an average taken at the start of the program\n";
        cout<<"       and as such the timings output by the program may be off\n";
        cout<<"       by several nanoseconds. \n\n";
        cout<<flush;
    }
    return total/iterations;
}

//return the average time a particular action takes
inline double sample_mean(vector<double> time, double ot){
    long double sum = 0;
    for(uint64_t i = 0; i < time.size(); i++)
        sum += time[i] - ot;
    return sum/time.size();
}

//return the variance of the collected measurements
inline double sample_variance(vector<double> time, double ot, double mean){
    long double sum = 0;
    for(uint64_t i = 0; i < time.size(); i++)
        sum += pow((time[i] - ot - mean)*1e9, 2);
    return sum/time.size();
}

//below returns the maximum and minimum time required per action
inline double sample_min(vector<double> time, double ot){
    double min = time[0]-ot;
    for(uint64_t i = 1; i < time.size(); i++)
        if(time[i]-ot < min) min = time[i]-ot;
    return min;
}
inline double sample_max(vector<double> time, double ot){
    double max = time[0]-ot;
    for(uint64_t i = 1; i < time.size(); i++)
        if(time[i]-ot > max) max = time[i]-ot;
    return max;
}

//This function generates histogram data and writes it to a file
void make_histogram(vector<double> time, double min, double max);

//print out the statistical results of the benchmark
void printout(vector<double> time, double ot, double mean, string message){
    double avg = sample_mean(time, ot);
    double var = sample_variance(time, ot, avg);
    double min = sample_min(time, ot);
    double max = sample_max(time, ot);
    double num = time.size();

    if(!csv){
        cout<<message<<"\n";
        cout<<"Mean time:       "<<avg * 1e9<<" ns\n";
        cout<<"True mean time:  "<<mean* 1e9<<" ns\n";
        cout<<"Variance:        "<<var      <<" ns^2\n";
        cout<<"Minimum time:    "<<min * 1e9<<" ns\n";
        cout<<"Maximum time:    "<<max * 1e9<<" ns\n\n";
    }
    else{
        cout << (boost::format("%1%,%2%,%3%,%4%,%5%,%6%\n")
                % num % (avg*1e9) % (mean*1e9)
                % var % (min*1e9) % (max*1e9));
    }
    cout << flush;

    make_histogram(time, min, max);
}

void make_histogram(vector<double> time, double min, double max){
    uint64_t i, j;
    unsigned int bottom = 0, top = 0;

    min *= 1e9;
    max *= 1e9;
    while(min > 1){
        bottom++;
        min *= 0.1;
    }
    while(max > 1){
        top++;
        max *= 0.1;
    }

    unsigned int slots = top-bottom+1;
    int* outvals = new int[slots];
    for(i = 0; i < slots; i++) outvals[i] = 0;

    int start = 1;
    for(i = 0; i < bottom; i++) start *= 10;

    int current;
    for(i = 0; i < time.size(); i++){
        current = start;
        time[i] *= 1e9;
        for(j = 0; j < slots; j++){
            if(time[i] < current){
                outvals[j]++;
                break;
            }
            current *= 10;
        }
    }

    std::ofstream outfile;
    outfile.open(outname.append(".dat").c_str());
    for(i = 0; i < slots; i++){
        outfile<<i<<" "<<outvals[i]<<"\n";
    }
    outfile.close();
}
#endif

