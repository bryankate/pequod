#include <iostream>
#include <boost/unordered_map.hpp>
#include <time.h>
#include <vector>
#include <sys/types.h>
#include <unistd.h>
#include "pqserver.hh" 
#include "clp.h"

using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::flush;
using pq::Server;

typedef char BYTE;

#define SIXTEEN_BITS 65536

struct Key {
  BYTE n[12];
};

static Clp_Option options[] = {
	{ "help", 'h', 1000, 0, 0 },
	{ "trials", 't', 1001, Clp_ValInt, Clp_Optional },
	{ "mode", 'm', 1002, Clp_ValStringNotOption, 0 },
};

enum { mode_none, mode_hash, mode_pq, mode_both };

void usage() {
  cerr << "-h --help Print this message." << endl;
  cerr << "-t[=N] --trials[=N] Generate N keys. (default: 1024)" << endl;
  cerr << "-m[=S] --mode[=S] Where S is one of 'hash', 'pq', or 'both'. (default: both)" << endl;
  exit(-1);
}

void pauseContinue(){
  cout << "Attach perf (pid:" << getpid() << ") if needed and press ENTER to continue... " << flush;
  std::cin.ignore( std::numeric_limits <std::streamsize> ::max(), '\n' );
}

int main(int argc, char **argv) {
	boost::unordered_map<string, string> map;
  std::vector< Key > data = std::vector< Key >();
  // options
  Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
  unsigned int trials_, mode_;
  trials_ = 1024;
  mode_ = 0;
  time_t tt[6] = { 0 };
  uint32_t outer, overage;

  while (Clp_Next(clp) != Clp_Done) {
    if (clp->option->long_name == String("help"))
      usage();
    else if (clp->option->long_name == String("trials"))
      trials_ = clp->have_val ? clp->val.i : 1024;
    else if (clp->option->long_name == String("mode")){
      if (clp->val.s == String("hash"))
        mode_ = mode_hash;
      else if (clp->val.s == String("pq"))
        mode_ = mode_pq;
      else if (clp->val.s == String("both"))
        mode_ = mode_both;
      else 
        usage();
    }
    else
      usage();
  }
  if (!mode_)
    mode_ = mode_both;

  outer = trials_ / SIXTEEN_BITS;
  overage = trials_ % SIXTEEN_BITS;

  tt[0] = time(0);
	for (uint32_t i=0; i < outer; ++i){
    uint32_t j = 0;
  	for (; j < SIXTEEN_BITS; ++j){
      Key buf;
      sprintf(buf.n, "s|%04X|%04X", i, j);
      data.push_back(buf);
  	}
  }	

  for (uint32_t i=0; i < overage; ++i){
      Key buf;
      sprintf(buf.n, "s|%04X|%04X", outer, i);
      data.push_back(buf);
  }
  tt[1] = time(0);

  assert(trials_ == data.size());

  if (mode_ ^ mode_both)
    pauseContinue();

  if (mode_ & mode_hash){
    tt[2] = time(0);
	  for (uint32_t i=0; i < data.size(); ++i){
  		map[ data[i].n ] = "";
	  }
    tt[3] = time(0);

    if (trials_ != map.size()) 
      cerr << "Warning: Only " << map.size() << " unique hashes, " << data.size() << " were requested." << endl; 
    map.clear(); 
  }

  if (mode_ & mode_pq){
    tt[4] = time(0);
    pq::Server pqs;
  	for (unsigned int i=0; i < data.size(); ++i)
	  	pqs.insert(data[i].n,"");
    tt[5] = time(0);
  }

  cout << "Trials run: " << data.size() << endl;
	cout << "Generating keys took " << tt[1] - tt[0] << " seconds" << endl;	
  if ( mode_ & mode_hash)
	  cout << "Hashing took " << tt[3] - tt[2] << " seconds" << endl;	
  if ( mode_ & mode_pq)
	  cout << "PQing took " << tt[5] - tt[4] << " seconds" << endl;	

	return 0;
}

