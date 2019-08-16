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

#ifndef YAHOO_H
#define YAHOO_H

// include
#include <tuple>
#include <atomic>
#include "tbb/flow_graph.h"

using namespace std;
using namespace tbb::flow;

// event_t struct
struct event_t
{
    uint64_t ts; // timestamp
    unsigned long user_id; // user id
    unsigned long page_id; // page id
    unsigned long ad_id; // advertisement id
    unsigned int ad_type; // advertisement type (0, 1, 2, 3, 4) => ("banner", "modal", "sponsored-search", "mail", "mobile")
    size_t event_type; // event type (0, 1, 2) => ("view", "click", "purchase")
    unsigned int ip; // ip address
    char padding[20]; // padding

    // constructor
    event_t() {}

    // getControlFields method
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>(event_type, 0, ts);
    }

    // setControlFields method
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        event_type = _key;
        ts = _ts;
    }
};

// joined_event_t struct
struct joined_event_t
{
    uint64_t ts; // timestamp
    unsigned long ad_id; // advertisement id
    unsigned long relational_ad_id;
    size_t cmp_id; // campaign id
    char padding[36]; // padding

    // constructor
    joined_event_t() {}

    // getControlFields method
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>(cmp_id, 0, ts);
    }

    // setControlFields method
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        cmp_id = _key;
        ts = _ts;
    }
};

// win_result struct
struct win_result
{
    uint64_t wid; // id
    uint64_t ts; // timestamp
    size_t cmp_id; // campaign id
    unsigned long lastUpdate; // MAX(TS)
    unsigned long count; // COUNT(*)
    char padding[28]; // padding

    // constructor
    win_result(): lastUpdate(0), count(0) {}

    // getControlFields method
    tuple<size_t, uint64_t, uint64_t> getControlFields() const
    {
        return tuple<size_t, uint64_t, uint64_t>(cmp_id, wid, ts);
    }

    // setControlFields method
    void setControlFields(size_t _key, uint64_t _id, uint64_t _ts)
    {
        cmp_id = _key;
        wid = _id;
        ts = _ts;
    }
};

// struct implementing a window
struct Window {
    unsigned long count;
    uint64_t initial_ts;
    uint64_t last_ts;
    uint64_t last_Update;

    // default constructor
    Window(): count(0), initial_ts((uint64_t) -1), last_ts(0), last_Update(0) {}

    // constructor
    Window(long _count, long _initial_ts, long _last_ts):
	       count(_count), initial_ts(_initial_ts), last_ts(_last_ts) {}

    // set method
    void set(long _count, long _initial_ts, long _last_ts) {
    	count = _count;
    	initial_ts = _initial_ts;
    	last_ts = _last_ts;
    }
};

// some aliases
typedef source_node<event_t *> source_node_t;
typedef multifunction_node<event_t *, tbb::flow::tuple<event_t *>, lightweight> filter_node_t;
typedef function_node<event_t *, continue_msg, lightweight> map_node_t;
typedef multifunction_node<joined_event_t *, tbb::flow::tuple<win_result *>, lightweight> window_node_t;
typedef function_node<win_result *, continue_msg, lightweight> sink_node_t;

// some aliases (batched version)
typedef source_node<vector<event_t *>> source_node_batched_t;
typedef multifunction_node<vector<event_t *>, tbb::flow::tuple<vector<event_t *>>> filter_node_batched_t;
typedef function_node<vector<event_t *>, continue_msg> map_node_batched_t;
typedef multifunction_node<vector<joined_event_t *>, tbb::flow::tuple<vector<win_result *>>> window_node_batched_t;

#endif
