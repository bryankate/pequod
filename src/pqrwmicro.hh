#ifndef PQRWMICRO_HH
#define PQRWMICRO_HH 1

#include <tamer/tamer.hh>
#include "json.hh"

namespace pq {
class Server;
class Join;

class RwMicro {
  public:
    RwMicro(Json& param, Server& server) 
        : prefresh_(param["prefresh"].as_i(10)), // percentage of refresh requests
          pactive_(param["pactive"].as_i(100)), // percentage of tweets being read
          nops_(param["nops"].as_i(100000)),
          nuser_(param["nusers"].as_i(1000)),
          nfollower_(100), server_(server),
          push_(param["push"].as_b(true)), j_(NULL) {
    }
    void populate();
    void run();
  private:
    int prefresh_;
    int pactive_;
    int nops_;
    int nuser_;
    int nfollower_;
    Server& server_;
    bool push_;
    Join* j_;
};

};

#endif
