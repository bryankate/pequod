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
        twait { mpfd.read(make_event(j)); }
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
    twait { tamer::at_signal(SIGINT, make_event()); }
    exit(0);
}

} // namespace

void server_loop(int port, pq::Server& server) {
    std::cerr << "listening on port " << port << "\n";
    acceptor(tamer::tcp_listen(port), server);
    interrupt_catcher();
}
