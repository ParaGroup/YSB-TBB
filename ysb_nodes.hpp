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
class YSBSource
{
private:
    unsigned long execution_time_sec; // total execution time of the benchmark
    unsigned long **ads_arrays;
    unsigned int adsPerCampaign; // number of ads per campaign
    size_t num_sent;
    volatile unsigned long current_time_us;
    unsigned int value;
    bool eos = false;

public:
    // constructor
    YSBSource(unsigned long _time_sec, unsigned long **_ads_arrays, unsigned int _adsPerCampaign):
			  execution_time_sec(_time_sec), ads_arrays(_ads_arrays), adsPerCampaign(_adsPerCampaign), num_sent(0), value(0) {}

    // source function
    bool operator()(event_t *&event)
    {
		if (eos) return false; // stopping
	    event = new event_t();
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
		//volatile long mytime = current_time_usecs();
		//while(current_time_usecs() - mytime <= 10);
	    double elapsed_time_sec = (current_time_us - start_time_usec) / 1000000.0;
	    if (elapsed_time_sec >= execution_time_sec) {
	        //cout << "[EventSource] Generated " << num_sent << " events" << endl;
	        sentCounter.fetch_add(num_sent);
	    	eos = true;
	    	event->ts = EOS;
		}
		return true;
    }
};

// Filter functor
class YSBFilter
{
private:
    unsigned int event_type; // forward only tuples with event_type

public:
    // constructor
    YSBFilter(unsigned int _event_type=0): event_type(_event_type) {}

    // filter function
    void operator()(event_t *event, filter_node_t::output_ports_type &op) {
        if(event->event_type == event_type || event->ts == EOS) {
	    	if (!std::get<0>(op).try_put(event)) abort();
		}
    }
};

// Join functor
class YSBJoin
{
private:
    unordered_map<unsigned long, unsigned int> &map; // hashmap
    campaign_record *relational_table; // relational table
    vector<window_node_t*> &workers;

public:
    // constructor
    YSBJoin(vector<window_node_t*> &_workers, unordered_map<unsigned long, unsigned int> &_map, campaign_record *_relational_table):
			workers(_workers), map(_map), relational_table(_relational_table) {}

	// constructor
    YSBJoin(const YSBJoin &other):
			workers(other.workers), map(other.map), relational_table(other.relational_table) {}

    // join function
    continue_msg operator()(event_t *event) {
		if (event->ts == EOS) {
	    	for(long i=0; i<workers.size(); ++i) {
				joined_event_t *out = new joined_event_t();
				out->ts = EOS;
				if (!workers[i]->try_put(out)) abort();
	    	}
	    	delete event;
	    	return continue_msg();
		}
		// check inside the hashmap
        auto it = map.find(event->ad_id);
        if (it != map.end()) {
            joined_event_t *out = new joined_event_t();
            out->ts = event->ts;
            out->ad_id = event->ad_id;
            campaign_record record = relational_table[(*it).second];
            out->relational_ad_id = record.ad_id;
            out->cmp_id = record.cmp_id;
	    	// parte eseguita dal KF_Emitter (joined_event_t --> joined_event_t)
	    	{
				auto key = std::get<0>(out->getControlFields()); // key
				size_t hashcode = hash<decltype(key)>()(key); // compute the hashcode of the key
				// evaluate the routing function
				size_t dest_w = hashcode % workers.size(); // routing_func(hashcode, pardegree);
				// routing the data on the basis of a key value 
				if (!workers[dest_w]->try_put(out)) abort();
	    	}
	    	// input cleanup
	    	delete event;
		}
		return continue_msg();  // keep going on
    }
};

// Window operator
class WinAggregate
{
private:
    long myid;
    long pardegree1;
    unordered_map<unsigned long, Window*> hashmap;
    int eos_received = 0;

public:
	// constructor
    WinAggregate(long _myid, long _pardegree1): myid(_myid), pardegree1(_pardegree1) {}

    // window function
    void operator()(joined_event_t *in, window_node_t::output_ports_type &op) {
		if (in->ts == EOS) {  // end-of-stream management
		    if (++eos_received == pardegree1) {
				for (auto& it: hashmap) {
				    if (it.second != nullptr) {
						unsigned long cmp_id = it.first;
						Window &win = *(it.second);
						win_result *out = new win_result();
						assert(out);
						out->setControlFields(cmp_id, 0, win.last_ts);
						out->count = win.count;
						out->lastUpdate = win.last_ts;
						if (!std::get<0>(op).try_put(out)) abort();
				    }
				}
				// forward EOS
				win_result *out = new win_result();
				assert(out);
				out->ts = EOS;
				if (!std::get<0>(op).try_put(out)) abort();
			}
		    return;
		}
		unsigned long cmp_id = in->cmp_id;
		unsigned long ts = in->ts;
	    auto it = hashmap.find(cmp_id);
	    if (it != hashmap.end()) {
		    Window &win = *(it->second);
		    if ((ts - win.initial_ts) >= 10000000) {  // <--- 10s
				win_result *out = new win_result();
				assert(out);
				out->setControlFields(cmp_id, 0, win.last_ts);
				out->count = win.count;
				out->lastUpdate = win.last_ts;
				if (!std::get<0>(op).try_put(out)) abort();
				// reset the window
				win.set(1, ts, ts);
		    }
		    else {
				win.count++;
				win.last_ts = ts;
		    }
		}
		else { 
		    Window *w = new Window(1, ts, ts);
		    hashmap[cmp_id] = w;
		}
		delete in;
    }
};

// Sink functor
class YSBSink
{
private:
    size_t received;

public:
    // constructor
    YSBSink(): received(0) {}

    // sink function
    long operator()(win_result *res) {
		if (res->ts == EOS) {
	    	delete res;
	    	return 0;
		}
		received++;
		delete res;
		return 0;
    }

    // get the number of received results
    size_t rcvResults() { return received; }
};

#endif
