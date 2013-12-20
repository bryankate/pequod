// -*- mode: c++ -*-
#include <tamer/fd.hh>
#include <stdio.h>
#include <string.h>
#include "pqserver.hh"
#include "pqmemory.hh"
#include "mpfd.hh"
#include "pqrpc.hh"
#include "error.hh"
#include "pqinterconnect.hh"
#include "pqlog.hh"
#include "sock_helper.hh"
#include <vector>
#include <sys/resource.h>

std::vector<pq::Interconnect*> interconnect_;
bool backend_ = true;

pq::Log log_(tstamp());
typedef struct {
    uint32_t ninsert;
    uint32_t ncount;
    uint32_t nsubscribe;
    uint32_t nunsubscribe;
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
        String key, first, last, scanlast;
        pq::Table* t;
        pq::Table::iterator it;
        size_t count;
        int32_t peer = -1;
    }

    twait { mpfd->read_request(make_event(j)); }

    if (!j || !j.is_a() || j.size() < 2 || !j[0].is_i()) {
        done(false);
        return;
    }
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
        twait { server.insert(j[2].as_s(), j[3].as_s(), make_event()); }
        rj[2] = pq_ok;
        ++diff_.ninsert;
        break;
    case pq_erase:
        twait { server.erase(j[2].as_s(), make_event()); }
        rj[2] = pq_ok;
        break;
    case pq_count:
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        scanlast = (j[4] && j[4].is_s()) ? j[4].as_s() : last;
        twait { server.validate(first, last, make_event(it)); }
        rj[3] = std::distance(it, server.table_for(first).lower_bound(scanlast));
        ++diff_.ncount;
        break;
    case pq_unsubscribe:
        rj[2] = pq_ok;
        first = j[2].as_s(), last = j[3].as_s();
        if (j[4] && j[4].is_o() && j[4]["subscriber"].is_i())
            peer = j[4]["subscriber"].as_i();
        if (unlikely(peer < 0)) {
            rj[2] = pq_fail;
            break;
        }
        server.unsubscribe(first, last, peer);
        ++diff_.nunsubscribe;
        break;
    case pq_subscribe:
        if (j[4] && j[4].is_o() && j[4]["subscriber"].is_i())
            peer = j[4]["subscriber"].as_i();
        if (unlikely(peer < 0)) {
            rj[2] = pq_fail;
            break;
        }
        first = j[2].as_s(), last = j[3].as_s(), scanlast = last;
        ++diff_.nsubscribe;
        goto do_scan;
    case pq_scan: {
        first = j[2].as_s(), last = j[3].as_s();
        scanlast = (j[4] && j[4].is_s()) ? j[4].as_s() : last;

        do_scan:
        rj[2] = pq_ok;
        twait { server.validate(first, last, make_event(it)); }
        if (unlikely(peer >= 0))
            server.subscribe(first, last, peer);

        auto itend = server.table_for(first).end();
        assert(!aj.shared());
        aj.clear();
        while (it != itend && it->key() < scanlast) {
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
                rj[3] = Json().set("backend", backend_)
                              .set("data", log_.as_json())
                              .set("internal", server.logs());
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
        //twait { tamer::at_asap(make_event()); }
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
        uint64_t now, before, utime, stime;
        double scale = 1.0 / 10000;
    }
    before = 0;
    memset(&lu, 0, sizeof(struct rusage));

    while(true) {
        mandatory_assert(getrusage(RUSAGE_SELF, &u) == 0, "Failed to getrusage.");
        now = tstamp();
        utime = tv2us(u.ru_utime) - tv2us(lu.ru_utime);
        stime = tv2us(u.ru_stime) - tv2us(lu.ru_stime);

        log_.record_at("utime_us", now, utime);
        log_.record_at("stime_us", now, stime);
        log_.record_at("cpu_pct", now, (before) ? ((utime + stime) * scale / fromus(now - before)) : 0);
        log_.record_at("mem_max_rss_mb", now, pq::maxrss_mb(u.ru_maxrss));
        log_.record_at("mem_size_store_mb", now, pq::mem_store_size >> 20);
        log_.record_at("mem_size_other_mb", now, pq::mem_other_size >> 20);
        log_.record_at("ninsert", now, diff_.ninsert);
        log_.record_at("ncount", now, diff_.ncount);
        log_.record_at("nsubscribe", now, diff_.nsubscribe);
        log_.record_at("nunsubscribe", now, diff_.nunsubscribe);
        log_.record_at("nscan", now, diff_.nscan);
        log_.record_at("ninvalidate", now, diff_.ninvalidate);

        lu = u;
        before = now;
        memset(&diff_, 0, sizeof(nrpc));
        twait volatile { tamer::at_delay_sec(1, make_event()); }
    }
}

tamed void periodic_eviction(pq::Server& server, uint64_t low, uint64_t high) {
    mandatory_assert(pq::enable_memory_tracking && "Cannot evict without memory tracking.");

    while(true) {
        // todo: use store size once its allocation is broken out
        if (pq::mem_other_size >= high) {
            do {
                if (!server.evict_one())
                    break;
            } while(pq::mem_other_size > low);
        }

        twait volatile { tamer::at_delay_sec(1, make_event()); }
    }
}

} // namespace

tamed void server_loop(pq::Server& server, int port, bool kill,
                       const pq::Hosts* hosts, const pq::Host* me,
                       const pq::Partitioner* part,
                       uint64_t mem_lo_mb, uint64_t mem_hi_mb) {
    tvars {
        tamer::fd killer;
        bool connected = false;
        double delay = 0.005;
    }

    memset(&diff_, 0, sizeof(nrpc));
    periodic_logger();

    if (mem_hi_mb) {
        assert(mem_lo_mb < mem_hi_mb);
        periodic_eviction(server, mem_lo_mb << 20, mem_hi_mb << 20);
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
        backend_ = part->is_backend(me->seqid());
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
