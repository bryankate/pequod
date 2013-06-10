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
pq::LogDiff diff_({"utime_us", "stime_us", "rss_mb",
                   "ninsert", "ncount", "nsubscribe",
                   "nscan", "ninvalidate"});

namespace {

tamed void process(pq::Server& server, const Json& j, Json& rj, Json& aj, tamer::event<> done) {
    tvars {
        int command;
        String key, first, last;
        pq::Table* t;
        pq::Table::iterator it;
        size_t count;
        int32_t peer = -1;
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
        diff_.add("ninsert", 1);
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
        diff_.add("ncount", 1);
        break;
    case pq_subscribe:
        if (j[4] && j[4].is_o() && j[4]["subscribe"].is_i())
            peer = j[4]["subscribe"].as_i();
        if (unlikely(peer < 0)) {
            rj[2] = pq_fail;
            break;
        }
        diff_.add("nsubscribe", 1);
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
        diff_.add("nscan", 1);
        break;
    }
    case pq_invalidate:
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        server.table_for(first, last).invalidate_remote(first, last);
        diff_.add("ninvalidate", 1);
        break;
    case pq_stats:
        rj[2] = pq_ok;
        rj[3] = server.stats();
        break;
    case pq_control:
        rj[2] = pq_ok;
        rj[3] = Json::make_object();

        if (j[2].is_o()) {
            // server does not know about logs
            if (j[2]["get_log"])
                rj[3] = log_.as_json();
            else if (j[2]["write_log"])
                log_.write_json(std::cerr);
            else if (j[2]["clear_log"])
                log_.clear();
        }

        server.control(j[2]);
        break;
    }

    done();
}

tamed void connector(tamer::fd cfd, msgpack_fd* mpfd, pq::Server& server) {
    tvars {
        Json j, rj = Json::make_array(0, 0, 0), aj = Json::make_array();
        msgpack_fd* mpfd_;
    }

    if (mpfd)
        mpfd_ = mpfd;
    else
        mpfd_ = new msgpack_fd(cfd);

    while (cfd) {
        twait { mpfd_->read_request(make_event(j)); }
        if (!j || !j.is_a() || j.size() < 2 || !j[0].is_i())
            break;

        rj[0] = -j[0].as_i();
        rj[1] = j[1];
        rj[2] = pq_fail;
        rj[3] = Json();

        // handle interconnect control message here so that the
        // fd can be turned into a remote interconnect client
        if (unlikely((j[0].as_i() == pq_control) && j[2].is_o() && j[2]["interconnect"])) {
            int32_t peer = j[2]["interconnect"].as_i();
            assert(peer >= 0 && peer < (int32_t)interconnect_.size());
            interconnect_[peer] = new pq::Interconnect(mpfd_);
        }

        twait { process(server, j, rj, aj, make_event()); }
        mpfd_->write(rj);
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

tamed void periodic_logger(pq::Log& log, pq::LogDiff& diff) {
    tvars {
        struct rusage u, lu;
    }
    memset(&lu, 0, sizeof(struct rusage));

    while(true) {
        mandatory_assert(getrusage(RUSAGE_SELF, &u) == 0, "Failed to getrusage.");
        diff.add("rss_mb", u.ru_maxrss / 1024);
        diff.add("utime_us", tv2us(u.ru_utime) - tv2us(lu.ru_utime));
        diff.add("stime_us", tv2us(u.ru_stime) - tv2us(lu.ru_stime));
        diff.checkpoint(log);

        lu = u;
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

    periodic_logger(log_, diff_);
    interrupt_catcher();
}
