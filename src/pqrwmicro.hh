#ifndef PQRWMICRO_HH
#define PQRWMICRO_HH 1

#include <tamer/tamer.hh>
#include "json.hh"

namespace pq {
class Server;

class RwMicro {
  public:
    RwMicro(Json& param, Server& server) 
        : pread_(param["pread"].as_i(10)),
          d_(param["duration"].as_i(10)), nuser_(10000),
          nfollower_(100), pspost_(100), server_(server) {
    }
    void populate();
    void run();
  private:
    int pread_;
    int d_;
    int nuser_;
    int nfollower_;
    int pspost_; // posts per second
    Server& server_;
};

};

#endif
