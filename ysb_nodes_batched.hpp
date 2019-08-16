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
 *  Classes of the Yahoo! Streaming Benchmark (TBB FlowGraph version)
 *  
 *  This version of the Yahoo! Streaming Benchmark is the one modified in the
 *  StreamBench project available in GitHub:
 *  https://github.com/lsds/StreamBench
 */ 

#ifndef YSB_NODES
#define YSB_NODES

// include
#include <tuple>
#include <mutex>
#include <atomic>
#include <cassert>
#include <sys/time.h>
#include <functional>
#include <unordered_map>
#include <ysb_common.hpp>
#include <campaign_generator.hpp>

using namespace std;

// global variable: starting time of the execution
volatile unsigned long start_time_usec;

// global variable: number of generated events
std::atomic<long> sentCounter;

// global variable: constant for the EOS
const unsigned long EOS = (unsigned long) -1;

/** 
 *  \brief Function to return the number of microseconds from the epoch
 *  
 *  This function returns the number of microseconds from the epoch using
 *  the clock_gettime() call.
 */ 
static inline unsigned long current_time_usecs() __attribute__((always_inline));
static inline unsigned long current_time_usecs()
{
    struct timespec t;
    clock_gettime(CLOCK_REALTIME, &t);
    return (t.tv_sec)*1000000L + (t.tv_nsec / 1000);
}

// Source functor
class YSBSourceBatched
{
private:
    unsigned long execution_time_sec; // total execution time of the benchmark
    unsigned long **ads_arrays;
    unsigned int adsPerCampaign; // number of ads per campaign
    size_t num_sent;
    volatile unsigned long current_time_us;
    unsigned int value;
    bool eos = false;
    size_t batch_len;

public:
    // constructor
    YSBSourceBatched(unsigned long _time_sec, unsigned long **_ads_arrays, unsigned int _adsPerCampaign, size_t _batch_len):
			  	     execution_time_sec(_time_sec), ads_arrays(_ads_arrays), adsPerCampaign(_adsPerCampaign), num_sent(0), value(0), batch_len(_batch_len) {}

    // source function
    bool operator()(vector<event_t *> &batch_evs)
    {
    	batch_evs.clear(); // <- important!
		if (eos)
			return false; // stopping
		for (size_t i=0; i<batch_len; i++) {
		    event_t *event = new event_t();
		    current_time_us = current_time_usecs();
		    // fill the event's fields
		    event->ts = current_time_usecs() - start_time_usec;
		    event->user_id = 0; // not meaningful
		    event->page_id = 0; // not meaningful
		    event->ad_id = ads_arrays[(value % 100000) % (N_CAMPAIGNS * adsPerCampaign)][1];
		    event->ad_type = (value % 100000) % 5;
		    event->event_type = (value % 100000) % 3;
		    event->ip = 1; // not meaningful
		    value++;
		    num_sent++;
		    batch_evs.push_back(event);
		}
		//volatile long mytime = current_time_usecs();
		//while(current_time_usecs() - mytime <= 10);
	    double elapsed_time_sec = (current_time_us - start_time_usec) / 1000000.0;
	    if (elapsed_time_sec >= execution_time_sec) {
	        //cout << "[EventSource] Generated " << num_sent << " events" << endl;
	        sentCounter.fetch_add(num_sent);
	    	eos = true;
	    	event_t *event = new event_t();
	    	event->ts = EOS;
	    	batch_evs.push_back(event);
		}
		return true;
    }
};

// Filter functor
class YSBFilterBatched
{
private:
    unsigned int event_type; // forward only tuples with event_type

public:
    // constructor
    YSBFilterBatched(unsigned int _event_type=0): event_type(_event_type) {}

    // filter function
    void operator()(vector<event_t *> batch_input, filter_node_batched_t::output_ports_type &op) {
    	vector<event_t *> batch_output;
    	for (size_t i=0; i<batch_input.size(); i++) {
    		event_t *event = batch_input[i];
        	if (event->event_type == event_type || event->ts == EOS) {
        		batch_output.push_back(event);
			}
			else
				delete event;
		}
		if (batch_output.size() > 0) {
			if (!std::get<0>(op).try_put(batch_output)) abort();
		}
    }
};

// Join functor
class YSBJoinBatched
{
private:
    unordered_map<unsigned long, unsigned int> &map; // hashmap
    campaign_record *relational_table; // relational table
    vector<window_node_batched_t *> &workers;

public:
    // constructor
    YSBJoinBatched(vector<window_node_batched_t *> &_workers, unordered_map<unsigned long, unsigned int> &_map, campaign_record *_relational_table):
				   workers(_workers), map(_map), relational_table(_relational_table) {}

	// constructor
    YSBJoinBatched(const YSBJoinBatched &other):
				   workers(other.workers), map(other.map), relational_table(other.relational_table) {}

    // join function
    continue_msg operator()(vector<event_t *> batch_input) {
    	vector<vector<joined_event_t *>> batches(workers.size());
    	for (size_t i=0; i<batch_input.size(); i++) {
    		event_t *event = batch_input[i];
    		if (event->ts == EOS) {
    			for (size_t w=0; w<workers.size(); w++) {
    				joined_event_t *out = new joined_event_t();
    				out->ts = EOS;
    				batches[w].push_back(out);
    			}
    		}
    		else {
	    		// check inside the hashmap
	        	auto it = map.find(event->ad_id);
	    		if (it != map.end()) {
	    			joined_event_t *out = new joined_event_t();
					out->ts = event->ts;
	            	out->ad_id = event->ad_id;
	            	campaign_record record = relational_table[(*it).second];
	            	out->relational_ad_id = record.ad_id;
	            	out->cmp_id = record.cmp_id;
	            	auto key = std::get<0>(out->getControlFields()); // key
	            	size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
	            	// evaluate the routing function
					size_t dest_w = hashcode % workers.size(); // routing_func(hashcode, pardegree);
					batches[dest_w].push_back(out);
				}
			}
			delete event;
    	}
    	for (size_t w=0; w<workers.size(); w++) {
    		if (batches[w].size() > 0) {
    			if (!workers[w]->try_put(batches[w])) abort();
    		}
    	}
		return continue_msg();  // keep going on
    }
};

// Window operator
class WinAggregateBatched
{
private:
    long myid;
    long pardegree1;
    unordered_map<unsigned long, vector<Window> *> hashmap;
    int eos_received = 0;
    size_t received;

public:
	// constructor
    WinAggregateBatched(long _myid, long _pardegree1): myid(_myid), pardegree1(_pardegree1), received(0), avgLatencyUs(0) {}

    // window function
    void operator()(vector<joined_event_t *> batch_input, window_node_batched_t::output_ports_type &op) {
    	for (size_t i=0; i<batch_input.size(); i++) {
    		joined_event_t *event = batch_input[i];
			if (event->ts == EOS) {  // end-of-stream management
			    if (++eos_received == pardegree1) {
					for (auto& it: hashmap) {
						vector<Window> &wins = *(it.second);
						received += wins.size();
					}
				}
			}
		    else {
				unsigned long cmp_id = event->cmp_id;
				unsigned long ts = event->ts;
				unsigned long wid = event->ts / 10000000;
	    		auto it = hashmap.find(cmp_id);
				if (it != hashmap.end()) {
					vector<Window> &wins = *(it->second);
					if (wins.size() <= wid) {
						for (size_t i=wins.size(); i<=wid; i++)
							wins.push_back(Window());
						wins[wid].count = 1;
						wins[wid].initial_ts = ts;
						wins[wid].last_ts = ts;
						wins[wid].last_Update = current_time_usecs();
					}
					else {
						wins[wid].count++;
						if (wins[wid].initial_ts > ts)
							wins[wid].initial_ts = ts;
						if (wins[wid].last_ts < ts)
							wins[wid].last_ts = ts;
						wins[wid].last_Update = current_time_usecs();
					}
				}
				else {
					vector<Window> *wins = new vector<Window>();
					for (size_t i=0; i<=wid; i++)
						wins->push_back(Window());
					(*wins)[wid].count = 1;
					(*wins)[wid].initial_ts = ts;
					(*wins)[wid].last_ts = ts;
					(*wins)[wid].last_Update = current_time_usecs();
					hashmap[cmp_id] = wins;
				}
			}
			delete event;
		}
	}

    // get the number of received results
    size_t rcvResults() { return received; } 
};

#endif
