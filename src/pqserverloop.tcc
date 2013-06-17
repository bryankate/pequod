// -*- mode: c++ -*-
#include <tamer/fd.hh>
#include <stdio.h>
#include <string.h>
#include "pqserver.hh"
#include "mpfd.hh"
#include "pqrpc.hh"
#include "error.hh"
#include "pqinterconnect.hh"
#include "pqlog.hh"
#include "sock_helper.hh"
#include <vector>
#include <sys/resource.h>

std::vector<pq::Interconnect*> interconnect_;

pq::Log log_(tstamp());
typedef struct {
    uint32_t ninsert;
    uint32_t ncount;
    uint32_t nsubscribe;
    uint32_t nscan;
    uint32_t ninvalidate;
} nrpc;
nrpc diff_;

namespace {

tamed void read_and_process_one(msgpack_fd* mpfd, pq::Server& server,
                                tamer::event<bool> done) {
    tvars {
        Json j, rj = Json::make_array(0, 0, 0), aj = Json::make_array();
        int32_t command;
        String key, first, last;
        pq::Table* t;
        pq::Table::iterator it;
        size_t count;
        int32_t peer = -1;
    }

    twait { mpfd->read_request(make_event(j)); }

    if (!j || !j.is_a() || j.size() < 2 || !j[0].is_i())
        done(false);
    else
        // allow the server to read and start processing another
        // rpc while this one is being handled
        done(true);

    command = j[0].as_i();

    rj[0] = -command;
    rj[1] = j[1];
    rj[2] = pq_fail;
    rj[3] = Json();

    if (command >= pq_get && command <= pq_add_join
        && !(j[2].is_s() && pq::table_name(j[2].as_s())))
        goto finish;
    if (command >= pq_count && command <= pq_add_join
        && !(j[3].is_s() && pq::table_name(j[2].as_s(), j[3].as_s())))
        goto finish;

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
        twait { server.validate(key, make_event(it)); }
        auto itend = server.table_for(key).end();
        if (it != itend && it->key() == key)
            rj[3] = it->value();
        else
            rj[3] = String();
        break;
    }
    case pq_insert:
        server.insert(j[2].as_s(), j[3].as_s());
        rj[2] = pq_ok;
        ++diff_.ninsert;
        break;
    case pq_erase:
        server.erase(j[2].as_s());
        rj[2] = pq_ok;
        break;
    case pq_count:
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        twait { server.validate_count(first, last, make_event(count)); }
        rj[3] = count;
        ++diff_.ncount;
        break;
    case pq_subscribe:
        if (j[4] && j[4].is_o() && j[4]["subscribe"].is_i())
            peer = j[4]["subscribe"].as_i();
        if (unlikely(peer < 0)) {
            rj[2] = pq_fail;
            break;
        }
        ++diff_.nsubscribe;
        // fall through to return scanned range
    case pq_scan: {
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        twait { server.validate(first, last, make_event(it)); }
        if (unlikely(peer >= 0))
            server.subscribe(first, last, peer);

        auto itend = server.table_for(first).end();
        assert(!aj.shared());
        aj.clear();
        while (it != itend && it->key() < last) {
            aj.push_back(it->key()).push_back(it->value());
            ++it;
        }
        rj[3] = aj;
        ++diff_.nscan;
        break;
    }
    case pq_invalidate:
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        server.table_for(first, last).invalidate_remote(first, last);
        ++diff_.ninvalidate;
        break;
    case pq_stats:
        rj[2] = pq_ok;
        rj[3] = server.stats();
        break;
    case pq_control:
        rj[2] = pq_ok;
        rj[3] = Json::make_object();

        // stuff the server does not know about
        if (j[2].is_o()) {
            if (j[2]["get_log"])
                rj[3] = log_.as_json();
            else if (j[2]["write_log"])
                log_.write_json(std::cerr);
            else if (j[2]["clear_log"])
                log_.clear();
            else if (j[2]["interconnect"]) {
                peer = j[2]["interconnect"].as_i();
                assert(peer >= 0 && peer < (int32_t)interconnect_.size());
                interconnect_[peer] = new pq::Interconnect(mpfd);
            }
        }

        server.control(j[2]);
        break;
    }

    finish:
    mpfd->write(rj);
}

tamed void connector(tamer::fd cfd, msgpack_fd* mpfd, pq::Server& server) {
    tvars {
        msgpack_fd* mpfd_;
        bool ok;
    }

    if (mpfd)
        mpfd_ = mpfd;
    else
        mpfd_ = new msgpack_fd(cfd);

    while (cfd) {
        twait { read_and_process_one(mpfd_, server, make_event(ok)); }
        if (!ok)
            break;

        // round robin connections with data to read
        twait { tamer::at_asap(make_event()); }
    }

    cfd.close();
    if (!mpfd)
        delete mpfd_;
}

tamed void acceptor(tamer::fd listenfd, pq::Server& server) {
    tvars { tamer::fd cfd; };
    if (!listenfd)
        fprintf(stderr, "listen: %s\n", strerror(-listenfd.error()));
    while (listenfd) {
        twait { listenfd.accept(make_event(cfd)); }
        connector(cfd, nullptr, server);
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

tamed void initialize_interconnect(pq::Server& server,
                                   const pq::Hosts* hosts, const pq::Host* me,
                                   const pq::Partitioner* part,
                                   tamer::event<bool> done) {
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        bool connected = true;
        int32_t i;
        Json j;
    }

    for (i = 0; i < hosts->size(); ++i) {
        if (!interconnect_[i] && i < me->seqid()) {
            twait {
                auto h = hosts->get_by_seqid(i);
                pq::sock_helper::make_sockaddr(h->name().c_str(), h->port(), sin);
                tamer::tcp_connect(sin.sin_addr, h->port(), make_event(fd));
            }

            if (!fd) {
                connected = false;
                break;
            }

            interconnect_[i] = new pq::Interconnect(fd);
            twait {
                interconnect_[i]->control(Json().set("interconnect", me->seqid()),
                                          make_event(j));
            }

            connector(fd, interconnect_[i]->fd(), server);
        }
        else if (!interconnect_[i] && i != me->seqid()) {
            connected = false;
            break;
        }
    }

    done(connected);
}

tamed void periodic_logger() {
    tvars {
        struct rusage u, lu;
        uint64_t now, utime, stime;
        double scale = 1.0 / 10000;
    }
    memset(&lu, 0, sizeof(struct rusage));

    while(true) {
        mandatory_assert(getrusage(RUSAGE_SELF, &u) == 0, "Failed to getrusage.");
        now = tstamp();
        utime = tv2us(u.ru_utime) - tv2us(lu.ru_utime);
        stime = tv2us(u.ru_stime) - tv2us(lu.ru_stime);

        log_.record_at("utime_us", now, utime);
        log_.record_at("stime_us", now, stime);
        log_.record_at("cpu_pct", now, (utime + stime) * scale);
        log_.record_at("rss_mb", now, u.ru_maxrss / 1024);
        log_.record_at("ninsert", now, diff_.ninsert);
        log_.record_at("ncount", now, diff_.ncount);
        log_.record_at("nsubscribe", now, diff_.nsubscribe);
        log_.record_at("nscan", now, diff_.nscan);
        log_.record_at("ninvalidate", now, diff_.ninvalidate);

        lu = u;
        memset(&diff_, 0, sizeof(nrpc));
        twait volatile { tamer::at_delay_sec(1, make_event()); }
    }
}

} // namespace

tamed void server_loop(int port, bool kill, pq::Server& server,
                       const pq::Hosts* hosts, const pq::Host* me,
                       const pq::Partitioner* part) {

    tvars {
        tamer::fd killer;
        bool connected = false;
        double delay = 0.005;
    }

    memset(&diff_, 0, sizeof(nrpc));
    periodic_logger();

    if (kill) {
        twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, port, make_event(killer)); }
        if (killer) {
            std::cerr << "killing existing server on port " << port << "\n";
            twait { kill_server(killer, port, make_event()); }
        }
    }

    std::cerr << "listening on port " << port << "\n";
    acceptor(tamer::tcp_listen(port), server);

    // if this is a cluster deployment, make connections to each server
    if (hosts) {
        interconnect_.assign(hosts->size(), nullptr);

        do {
            twait { tamer::at_delay(delay, make_event()); }
            twait { initialize_interconnect(server, hosts, me, part,
                                            make_event(connected)); }
        } while(!connected);

        server.set_cluster_details(me->seqid(), interconnect_, part);
    }

    interrupt_catcher();
}
