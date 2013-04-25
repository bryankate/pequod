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
    tvars {
        size_t count;
    }

    twait { server_.validate_count(first, last, make_event(count)); }
    e(count);
}

tamed void DirectClient::add_count(const String& first, const String& last,
                                   event<size_t> e) {
    tvars {
        size_t count;
    }

    twait { server_.validate_count(first, last, make_event(count)); }
    e(e.result() + count);
}

tamed void DirectClient::scan(const String& first, const String& last,
                              event<scan_result> e) {
    tvars {
        Table::iterator it;
    }

    twait { server_.validate(first, last, make_event(it)); }
    e(scan_result(it, server_.table_for(first).lower_bound(last)));
}

}
