// -*- mode: c++ -*-
#include <tamer/fd.hh>
#include <stdio.h>
#include <string.h>
#include "pqserver.hh"
#include "mpfd.hh"
#include "pqrpc.hh"
#include "error.hh"

namespace {

tamed void connector(tamer::fd cfd, pq::Server& server) {
    tvars {
        Json j, rj = Json::make_array(0, 0, 0);
        msgpack_fd mpfd(cfd);
    }
    while (cfd) {
        twait { mpfd.read_request(make_event(j)); }
        if (!j || !j.is_a() || j.size() < 2 || !j[0].is_i())
            break;

        rj[0] = -j[0].as_i();
        rj[1] = j[1];
        rj[2] = pq_fail;

        switch (j[0].as_i()) {
        case pq_add_join:
            if (j[2].is_s() && j[3].is_s() && j[4].is_s()
                && pq::table_name(j[2].as_s(), j[3].as_s())) {
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
        case pq_get:
            if (j[2].is_s() && pq::table_name(j[2].as_s())) {
                rj[2] = pq_ok;
                server.validate(j[2].as_s());
                rj[3] = server[j[2].as_s()].value();
            }
            break;
        case pq_insert:
            if (j[2].is_s() && j[3].is_s()) {
                server.insert(j[2].as_s(), j[3].as_s());
                rj[2] = pq_ok;
            }
            break;
        case pq_erase:
            if (j[2].is_s()) {
                server.erase(j[2].as_s());
                rj[2] = pq_ok;
            }
            break;
        case pq_count:
            if (j[2].is_s() && j[3].is_s()
                && pq::table_name(j[2].as_s(), j[3].as_s())) {
                rj[2] = pq_ok;
                server.validate(j[2].as_s(), j[3].as_s());
                rj[3] = server.count(j[2].as_s(), j[3].as_s());
            }
            break;
        case pq_scan:
            if (j[2].is_s() && j[3].is_s()
                && pq::table_name(j[2].as_s(), j[3].as_s())) {
                rj[2] = pq_ok;
                server.validate(j[2].as_s(), j[3].as_s());
                Json results = Json::make_array();
                auto last = server.lower_bound(j[3].as_s());
                for (auto it = server.lower_bound(j[2].as_s());
                     it != last; ++it)
                    results.push_back(it->key()).push_back(it->value());
                rj[3] = std::move(results);
            }
            break;
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
