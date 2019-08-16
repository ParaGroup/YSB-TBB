/******************************************************************************
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License version 3 as
 *  published by the Free Software Foundation.
 *  
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public
 *  License for more details.
 *  
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software Foundation,
 *  Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
 ******************************************************************************
 */

/*  
 *  Test application of the Yahoo! Streaming Benchmark
 *  (TBB FlowGraph version)
 *  
 *  The application is a pipeline of five stages:
 *  EventSource (generator of events at full speed)
 *  Filter
 *  Join
 *  Window Aggregate
 *  Sink
 */ 

// include
#include <fstream>
#include <iostream>
#include <iterator>
#include "tbb/tbb.h"
#include "tbb/flow_graph.h"
#include <ysb_nodes.hpp>
#include <ysb_common.hpp>
#include <campaign_generator.hpp>

// global variable: starting time of the execution
extern volatile unsigned long start_time_usec;

// global variable: number of generated events
extern atomic<long> sentCounter;

// main
int main(int argc, char *argv[])
{
    int option = 0;
    unsigned long exec_time_sec = 0;
    size_t pardegree1 = 1;
    size_t pardegree2 = 1;
    int TBBThreads = -1;
    // initialize global sentCounter
    sentCounter = 0;
    // arguments from command line
    if (argc < 7) {
	   cout << argv[0] << " -l [execution_seconds] -n [par_degree] -m [par_degree] [-t numTBBThreads]" << endl;
	   exit(EXIT_SUCCESS);
    }
    while ((option = getopt(argc, argv, "l:n:m:t:")) != -1) {
    	switch (option) {
        	case 'l': exec_time_sec = atoi(optarg);
        	    break;
        	case 'n': pardegree1 = atoi(optarg);
        	    break;
        	case 'm': pardegree2 = atoi(optarg);
        	    break;
        	case 't': TBBThreads = atoi(optarg);
        	    break;
        	default: {
        	    cout << argv[0] << " -l [execution_seconds] -n [par_degree] -m [par_degree] [-t numTBBThreads]" << endl;
        	    exit(EXIT_SUCCESS);
        	}
        }
    }
    // initialize TBB environment
    tbb::task_scheduler_init init((TBBThreads > 0) ? TBBThreads : tbb::task_scheduler_init::default_num_threads());
    // create the campaigns
    CampaignGenerator campaign_gen;
    // the application graph
    graph g;
    // create the TBB FlowGraph nodes (left part)
    vector<source_node_t *> sources;
    vector<filter_node_t *> filters;
    vector<map_node_t *> maps;
    vector<window_node_t *> workers;
    vector<sink_node_t *> sinks;
    for(size_t i=0; i<pardegree1; ++i) {
    	// create source
    	auto source = new source_node_t(g, YSBSource(exec_time_sec, campaign_gen.getArrays(), campaign_gen.getAdsCompaign()));
    	assert(source);
    	sources.push_back(source);
    	// create filter
    	auto filter = new filter_node_t(g, unlimited, YSBFilter());
    	assert(filter);
    	filters.push_back(filter);
    	// create the flat-map
    	auto join = new map_node_t(g, unlimited, YSBJoin(workers, campaign_gen.getHashMap(), campaign_gen.getRelationalTable()));
    	assert(join);
    	maps.push_back(join);
    }
    // create the TBB FlowGraph nodes (right part)
    for(size_t i=0; i<pardegree2; ++i) {
    	// create the aggregation
    	auto aggregation = new window_node_t(g, 1, WinAggregate(i, pardegree1));
    	assert(aggregation);
    	workers.push_back(aggregation);
    	// create the sink
    	auto sink = new sink_node_t(g, 1, YSBSink());
    	assert(sink);
    	sinks.push_back(sink);
    }
    // create the connections between nodes
    for(size_t i=0; i<pardegree1; ++i) {
    	make_edge(*sources[i], *filters[i]);
    	make_edge(*filters[i], *maps[i]);
    }
    for(size_t i=0; i<pardegree2; ++i) {
	   make_edge (*workers[i], *sinks[i]);
    }
    // initialize global start_time_usec
    volatile unsigned long start_time_main_us = current_time_usecs();
    start_time_usec = start_time_main_us;
    // starting all sources
    for(size_t i=0; i<pardegree1; ++i)
	   sources[i]->activate();
    // waiting for termination
    g.wait_for_all();
    // final statistics
    volatile unsigned long end_time_main_us = current_time_usecs();
    double elapsed_time_sec = (end_time_main_us - start_time_main_us) / (1000000.0);
    unsigned long rcvResults  = 0;
    for(size_t i=0; i<pardegree2; ++i) {
	   auto body = copy_body<YSBSink, sink_node_t>(*sinks[i]);
	   rcvResults  += body.rcvResults();
    }
    cout << "[Main] Total generated messages are " << sentCounter << endl;
    cout << "[Main] Total received results are " << rcvResults << endl;
    cout << "[Main] Total elapsed time (seconds) " << elapsed_time_sec << endl;
    for(size_t i=0; i<pardegree1; ++i) {
	   delete sources[i];
	   delete filters[i];
	   delete maps[i];
    }
    for(size_t i=0; i<pardegree2; ++i) {
	   delete workers[i];
	   delete sinks[i];
    }
    return 0;
}
