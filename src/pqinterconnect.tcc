// -*- mode: c++ -*-
#include "pqinterconnect.hh"

namespace pq {

tamed void Interconnect::subscribe(const String& first, const String& last,
                                   int32_t subscriber, event<scan_result> e) {
    tvars { Json j; }
    twait {
        fd_.call(Json::make_array(pq_subscribe, seq_, first, last,
                                  Json().set("subscribe", subscriber)),
                 make_event(j));
        ++seq_;
    }
    e(scan_result(j && j[2].to_i() == pq_ok ? j[3] : Json::make_array()));
}

}
