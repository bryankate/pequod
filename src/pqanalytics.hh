#ifndef PQANALYTICS_HH
#define PQANALYTICS_HH

#include "pqserver.hh"
#include "json.hh"
#include <boost/random.hpp>
#include <map>

namespace pq {

class AnalyticsRunner {
  public:
    AnalyticsRunner(Server& server, const Json& param);

    void populate();
    void run();

  private:
    Server& server_;
    const Json& param_;
    boost::mt19937 gen_;
    boost::random_number_generator<boost::mt19937> rng_;

    // parameters
    bool log_;
    bool push_;
    bool proactive_;
    bool buffer_;
    uint32_t popduration_;
    uint32_t duration_;
    uint32_t pread_;

    uint32_t bytes_;
    std::map<String, uint32_t> bpm_;

    void record_bps(uint32_t time);
};

}
#endif
