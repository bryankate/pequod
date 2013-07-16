// -*- mode: c++ -*-
#include "pqmulticlient.hh"

namespace pq {

MultiClient::MultiClient(const Hosts* hosts, const Hosts* dbhosts,
                         const Partitioner* part, int colocateCacheServer)
    : hosts_(hosts), dbhosts_(dbhosts), part_(part), localNode_(nullptr),
      colocateCacheServer_(colocateCacheServer) {
}

MultiClient::~MultiClient() {
    clear();
}

tamed void MultiClient::connect(tamer::event<> done) {
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        const Host* h;
        int32_t i;
    }

    // for backward compatibility, allow connection to a single server.
    // in this case we interpret the colocated server to be a port number
    if (!hosts_ && !part_ && colocateCacheServer_ > 0) {
        twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)},
                                   colocateCacheServer_, make_event(fd)); }
        localNode_ = new RemoteClient(fd);
    }
    else {
        for (i = 0; i < hosts_->size(); ++i) {
            h = hosts_->get_by_seqid(i);
            sock_helper::make_sockaddr(h->name().c_str(), h->port(), sin);
            twait { tamer::tcp_connect(sin.sin_addr, h->port(), make_event(fd)); }
            if (!fd) {
                std::cerr << "connection error ("
                          << h->name() << ":" << h->port() << "): "
                          << strerror(-fd.error()) << std::endl;
                exit(1);
            }

            clients_.push_back(new RemoteClient(fd));
        }

        if (colocateCacheServer_ >= 0) {
            mandatory_assert(colocateCacheServer_ < hosts_->size());
            localNode_ = clients_[colocateCacheServer_];
        }
    }

    if (dbhosts_ && part_) {
        for (i = 0; i < dbhosts_->size(); ++i) {
            h = dbhosts_->get_by_seqid(i);
            DBPool* pool = new DBPool(h->name(), h->port());
            pool->connect();
            dbclients_.push_back(pool);
        }
    }

    done();
}

tamed void MultiClient::restart(tamer::event<> done) {
    clear();
    twait { connect(make_event()); }
    done();
}

tamed void MultiClient::add_join(const String& first, const String& last,
                                 const String& joinspec, event<Json> e) {
    tvars {
        Json rj;
    }

    if (localNode_)
        localNode_->add_join(first, last, joinspec, e);
    else {
        rj = Json::make_array_reserve(clients_.size());
        twait {
            for (uint32_t i = 0; i < clients_.size(); ++i)
                if (!part_->is_backend(i))
                    clients_[i]->add_join(first, last, joinspec,
                                          make_event(rj[i].value()));
        }

        Json ret;
        for (auto it = rj.abegin(); it != rj.aend(); ++it)
            if ((*it)["message"]) {
                ret.set("message", rj);
                break;
            }
        e(ret);
    }
}

tamed void MultiClient::get(const String& key, event<String> e) {
    cache_for(key)->get(key, e);
}

tamed void MultiClient::insert(const String& key, const String& value, event<> e) {
    cache_for(key)->insert(key, value, e);
}

tamed void MultiClient::erase(const String& key, event<> e) {
    cache_for(key)->erase(key, e);
}

tamed void MultiClient::insert_db(const String& key, const String& value, event<> e) {
    backend_for(key)->insert(key, value, e);
}

tamed void MultiClient::erase_db(const String& key, event<> e) {
    backend_for(key)->erase(key, e);
}

tamed void MultiClient::count(const String& first, const String& last,
                              event<size_t> e) {
    cache_for(first)->count(first, last, e);
}

tamed void MultiClient::count(const String& first, const String& last,
                              const String& scanlast, event<size_t> e) {
    cache_for(first)->count(first, last, scanlast, e);
}

tamed void MultiClient::add_count(const String& first, const String& last,
                                  event<size_t> e) {
    cache_for(first)->add_count(first, last, e);
}

tamed void MultiClient::add_count(const String& first, const String& last,
                                  const String& scanlast, event<size_t> e) {
    cache_for(first)->add_count(first, last, scanlast, e);
}

tamed void MultiClient::scan(const String& first, const String& last,
                             event<scan_result> e) {
    cache_for(first)->scan(first, last, e);
}

tamed void MultiClient::scan(const String& first, const String& last,
                             const String& scanlast, event<scan_result> e) {
    cache_for(first)->scan(first, last, scanlast, e);
}

tamed void MultiClient::stats(event<Json> e) {
    tvars {
        Json j;
    }

    if (localNode_)
        localNode_->stats(e);
    else {
        j = Json::make_array_reserve(clients_.size());
        twait {
            for (uint32_t i = 0; i < clients_.size(); ++i)
                clients_[i]->stats(make_event(j[i].value()));
        }
        e(j);
    }
}

tamed void MultiClient::control(const Json& cmd, tamer::event<Json> done) {
    tvars {
        Json j;
    }

    if (localNode_)
        localNode_->control(cmd, done);
    else  {
        j = Json::make_array_reserve(clients_.size());
        twait {
            for (uint32_t i = 0; i < clients_.size(); ++i)
                clients_[i]->control(cmd, make_event(j[i].value()));
        }
        done(j);
    }
}

tamed void MultiClient::pace(tamer::event<> done) {
    tvars {
        tamer::gather_rendezvous gr;
    }

    for (auto& r : clients_)
        r->pace(gr.make_event());

    twait(gr);
    done();
}

}
