// -*- mode: c++ -*-
#include "pqclient.hh"

namespace pq {

tamed void DirectClient::get(const String& key, event<String> e) {
    tvars {
        Table::iterator it;
    }

    twait { server_.validate(key, make_event(it)); }
    auto itend =  server_.table_for(key).end();
    if (it != itend && it->key() == key)
        e(it->value());
    else
        e(String());
}

tamed void DirectClient::count(const String& first, const String& last,
                               event<size_t> e) {
    count(first, last, last, e);
}

tamed void DirectClient::count(const String& first, const String& last, const String& scanlast,
                               event<size_t> e) {
    tvars {
        Table::iterator it;
    }

    twait { server_.validate(first, last, make_event(it)); }
    e(std::distance(it, server_.table_for(first).lower_bound(scanlast)));
}

tamed void DirectClient::add_count(const String& first, const String& last,
                                   event<size_t> e) {
    add_count(first, last, last, e);
}

tamed void DirectClient::add_count(const String& first, const String& last, const String& scanlast,
                                   event<size_t> e) {
    tvars {
        Table::iterator it;
    }

    twait { server_.validate(first, last, make_event(it)); }
    e(e.result() + std::distance(it, server_.table_for(first).lower_bound(scanlast)));
}

tamed void DirectClient::scan(const String& first, const String& last,
                              event<scan_result> e) {
    scan(first, last, last, e);
}

tamed void DirectClient::scan(const String& first, const String& last, const String& scanlast,
                              event<scan_result> e) {
    tvars {
        Table::iterator it;
    }

    twait { server_.validate(first, last, make_event(it)); }
    e(scan_result(it, server_.table_for(first).lower_bound(scanlast)));
}

}
