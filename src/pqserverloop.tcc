// -*- mode: c++ -*-
#include <tamer/fd.hh>
#include <stdio.h>
#include <string.h>
#include "pqserver.hh"
#include "mpfd.hh"
#include "pqrpc.hh"
#include "error.hh"

namespace {

tamed void process(pq::Server& server, const Json& j, Json& rj, Json& aj, tamer::event<> done) {
    tvars {
        int command;
        String key, first, last;
        pq::Table* t;
    }

    command = j[0].as_i();
    if (command >= pq_get && command <= pq_add_join
        && !(j[2].is_s() && pq::table_name(j[2].as_s())))
        return;
    if (command >= pq_count && command <= pq_add_join
        && !(j[3].is_s() && pq::table_name(j[2].as_s(), j[3].as_s())))
        return;

    switch (command) {
    case pq_add_join:
        if (j[4].is_s()) {
            pq::Join* join = new pq::Join;
            ErrorAccumulator errh;
            if (join->assign_parse(j[4].as_s(), &errh)) {
                server.add_join(j[2].as_s(), j[3].as_s(), join);
                rj[2] = pq_ok;
            }
            if (!errh.empty())
                rj[3] = errh.join();
        }
        break;
    case pq_get: {
        rj[2] = pq_ok;
        key = j[2].as_s();
        t = &server.table_for(key);
        auto it = t->validate(key, server.next_validate_at());
        if (it != t->end() && it->key() == key)
            rj[3] = it->value();
        else
            rj[3] = String();
        break;
    }
    case pq_insert:
        server.insert(j[2].as_s(), j[3].as_s());
        rj[2] = pq_ok;
        break;
    case pq_erase:
        server.erase(j[2].as_s());
        rj[2] = pq_ok;
        break;
    case pq_count:
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        rj[3] = server.validate_count(first, last);
        break;
    case pq_scan: {
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        auto it = server.validate(first, last);
        auto itend = server.table_for(first).end();
        assert(!aj.shared());
        aj.clear();
        while (it != itend && it->key() < last) {
            aj.push_back(it->key()).push_back(it->value());
            ++it;
        }
        rj[3] = aj;
        break;
    }
    case pq_stats:
        rj[2] = pq_ok;
        rj[3] = server.stats();
        break;
    case pq_control:
        if (j[2].is_o()) {
            if (j[2]["quit"])
                exit(0);
            rj[2] = pq_ok;
            rj[3] = Json::make_object();
        }
        break;
    }

    done();
}

tamed void connector(tamer::fd cfd, pq::Server& server) {
    tvars {
        Json j, rj = Json::make_array(0, 0, 0), aj = Json::make_array();
        msgpack_fd mpfd(cfd);
    }
    while (cfd) {
        twait { mpfd.read_request(make_event(j)); }
        if (!j || !j.is_a() || j.size() < 2 || !j[0].is_i())
            break;

        rj[0] = -j[0].as_i();
        rj[1] = j[1];
        rj[2] = pq_fail;
        rj[3] = Json();

        twait { process(server, j, rj, aj, make_event()); }
        mpfd.write(rj);
    }
    cfd.close();
}

tamed void acceptor(tamer::fd listenfd, pq::Server& server) {
    tvars { tamer::fd cfd; };
    if (!listenfd)
        fprintf(stderr, "listen: %s\n", strerror(-listenfd.error()));
    while (listenfd) {
        twait { listenfd.accept(make_event(cfd)); }
        connector(cfd, server);
    }
}

tamed void interrupt_catcher() {
    twait volatile { tamer::at_signal(SIGINT, make_event()); }
    exit(0);
}

tamed void kill_server(tamer::fd fd, int port, tamer::event<> done) {
    tvars { msgpack_fd mpfd(fd); Json j; double delay = 0.005; }
    twait { mpfd.call(Json::make_array(pq_control, 1, Json().set("quit", true)),
                      make_event(j)); }
    fd.close();
    while (done) {
        delay = std::min(delay * 2, 0.1);
        twait { tamer::at_delay(delay, make_event()); }
        twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, port, make_event(fd)); }
        if (!fd)
            done();
    }
}

} // namespace

tamed void server_loop(int port, bool kill, pq::Server& server) {
    tvars { tamer::fd killer; };
    if (kill) {
        twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, port, make_event(killer)); }
        if (killer) {
            std::cerr << "killing existing server on port " << port << "\n";
            twait { kill_server(killer, port, make_event()); }
        }
    }
    std::cerr << "listening on port " << port << "\n";
    acceptor(tamer::tcp_listen(port), server);
    interrupt_catcher();
}
