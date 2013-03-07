// -*- mode: c++ -*-
#include "pqremoteclient.hh"

namespace pq {

tamed void RemoteClient::add_join(const String& first, const String& last,
                                  const String& joinspec, event<> e) {
    tvars { Json j; unsigned long int seq = this->seq_; }
    twait {
        fd_.call(Json::make_array(pq_add_join, seq_, first, last, joinspec),
                 make_event(j));
        ++seq_;
    }
    e();
}

tamed void RemoteClient::insert(const String& key, const String& value,
                                event<> e) {
    tvars { Json j; unsigned long int seq = this->seq_; }
    twait {
        fd_.call(Json::make_array(pq_insert, seq_, key, value), make_event(j));
        ++seq_;
    }
    if (j[0] != -pq_insert || j[1] != seq)
        std::cerr << "expected insert " << seq << ", got " << j << "\n";
    e();
}

tamed void RemoteClient::erase(const String& key, event<> e) {
    tvars { Json j; unsigned long int seq = this->seq_; }
    twait {
        fd_.call(Json::make_array(pq_erase, seq_, key), make_event(j));
        ++seq_;
    }
    e();
}

tamed void RemoteClient::count(const String& first, const String& last,
                               event<size_t> e) {
    tvars { Json j; unsigned long int seq = this->seq_; }
    twait {
        fd_.call(Json::make_array(pq_count, seq_, first, last), make_event(j));
        ++seq_;
    }
    assert(j[0] == -pq_count && j[1] == seq);
    e(j && j[2].to_i() == pq_ok ? j[3].to_u64() : 0);
}

tamed void RemoteClient::scan(const String& first, const String& last,
                              event<scan_result> e) {
    tvars { Json j; }
    twait {
        fd_.call(Json::make_array(pq_scan, seq_, first, last), make_event(j));
        ++seq_;
    }
    e(scan_result(j && j[2].to_i() == pq_ok ? j[3] : Json::make_array()));
}

tamed void RemoteClient::stats(event<Json> e) {
    tvars { Json j; unsigned long int seq = this->seq_; }
    twait {
        fd_.call(Json::make_array(pq_stats, seq_), make_event(j));
        ++seq_;
    }
    e(j && j[2].to_i() == pq_ok ? j[3] : Json::make_object());
}

}
