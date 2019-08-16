CXX = /usr/local/gcc9/bin/g++ -std=c++11 -DN_CAMPAIGNS=100
OPT_FLAGS = -g -O3
TBB_HOME = /tmp/tbb2019
CXXFLAGS = -I. -I${TBB_HOME}/include
LDFLAGS = -L${TBB_HOME}/lib/intel64/gcc4.7
LIBS = -ltbb -pthread

TARGETS= test_ysb_flowgraph test_ysb_flowgraph_batched

.PHONY= clean cleanall all

%:%.cpp
	$(CXX) $(OPT_FLAGS) $(CXXFLAGS) $< -o $@ $(LDFLAGS) $(LIBS)

all: $(TARGETS)

clean:
	\rm -f $(TARGETS)

cleanall: clean
	\rm -f *~