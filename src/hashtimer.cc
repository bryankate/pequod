#include <iostream>
#include <boost/unordered_map.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int_distribution.hpp>
#include <time.h>
#include <vector>
//#include "string.hh"
#include "pqserver.hh" 

using std::cout;
using std::endl;
using std::string;
using pq::Server;
 
// 22888 * 65636 ~ 1.5 billion
//#define MY_I 22888
#define MY_J 65536
#define MY_I 2888
//#define MY_J 24

typedef char BYTE;

struct Key {
  BYTE n[12];
};

int main(int argc, char **argv) {
	boost::unordered_map<string, string> map;
	boost::random::mt19937 gen;
	if (argc > 1)
		gen.seed(atoi(argv[1]));
	else 
		gen.seed(time(0));
	boost::random::uniform_int_distribution<> dist;
 
  std::vector< Key > data = std::vector< Key >();

  time_t time1 = time(0);

	for (uint32_t i=0; i < MY_I; ++i){
    uint32_t j = 0;
  	for (; j < MY_J; ++j){
      Key buf;
      sprintf(buf.n, "s|%04X|%04X", i, j);
//      memcpy(buf->n, "s|XXXX|XXXX", 11);
//      write_in_net_order(buf->n + 2, i);
//      write_in_net_order(buf->n + 7, j);
      data.push_back(buf);
  	}
  }	
  time_t time2 = time(0);

	for (uint32_t i=0; i < MY_I * MY_J; ++i){
    //cout << data[i].n << endl; 
		map[ data[i].n ] = "";
	}
  
  time_t time3 = time(0);

  
  cout << "char size is (" << sizeof(char) << ")" << endl;
  cout << "element size is (" << sizeof(data[25]) << ")" << endl;
  cout << "map is size(" << map.size() << ")" << endl;
  map.clear(); 

  pq::Server pqs;

	for (int i=0; i < MY_I * MY_J; ++i){
		pqs.insert(data[i].n,"");
	}
  
  time_t time4 = time(0);

  cout << "map is size(" << map.size() << ")" << endl;

	cout << "Generating keys took " << time2 - time1 << " seconds" << endl;	
	cout << "Hashing took " << time3 - time2 << " seconds" << endl;	
	cout << "PQing took " << time4 - time3 << " seconds" << endl;	

	return 0;
}


