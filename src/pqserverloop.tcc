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
#include <set>
#include <sys/resource.h>

const pq::Host* me_ = nullptr;
const pq::Partitioner* part_ = nullptr;
std::vector<pq::Interconnect*> interconnect_;
std::set<msgpack_fd*> clients_;
bool ready_ = false;
uint32_t round_robin_ = 0;

pq::Log log_(tstamp());
typedef struct {
    uint32_t ninsert;
    uint32_t ncount;
    uint32_t nsubscribe;
    uint32_t nunsubscribe;
    uint32_t nscan;
    uint32_t ninvalidate;
    uint32_t nnotify;
} nrpc;
nrpc diff_;

static const String noop_val = String::make_fill('.', 512);

namespace {

static std::vector<std::string>::iterator
blocked_locations_shrink(std::vector<std::string>::iterator first,
                         std::vector<std::string>::iterator last) {
    std::vector<std::string>::iterator out = first;
    while (first != last) {
        std::vector<std::string>::iterator next = first + 1;
        if (next != last && *next == *first) {
            size_t n = 2;
            for (++next; next != last && *next == *first; ++next, ++n)
                /* nada */;
            std::stringstream buf;
            buf << *first << " * " << n;
            *out = buf.str();
        } else if (out != first)
            *out = *first;
        ++out;
        first = next;
    }
    return out;
}

tamed void read_and_process_one(msgpack_fd* mpfd, pq::Server& server,
                                tamer::event<bool> done) {
    tvars {
        Json j, rj = Json::array(0, 0, 0), aj = Json::make_array();
        int32_t command;
        String key, first, last, scanlast;
        pq::Table* t;
        pq::Table::iterator it;
        size_t count;
        int32_t peer = -1;
    }

    twait { mpfd->read_request(make_event(j)); }

    if (!j || !j.is_a() || j.size() < 2 || !j[0].is_i()) {
        std::cerr << "bad rpc: " << j << std::endl;
        done(false);
        return;
    } else
        // allow the server to read and start processing another
        // rpc while this one is being handled (iff it blocks)
        done(true);

    command = j[0].as_i();
    assert(ready_ || command == pq_control);

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
        auto itend = it.table_end();
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
        rj[3] = std::distance(it, server.table_for(first, last).lower_bound(scanlast));
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
        assert(part_ && part_->owner(first) == me_->seqid());
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

        auto itend = it.table_end();
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
    case pq_notify_insert:
        key = j[2].as_s();
        server.table_for(key).insert(key, j[3].as_s());
        rj[2] = pq_ok;
        ++diff_.nnotify;
        break;
    case pq_notify_erase:
        key = j[2].as_s();
        server.table_for(key).erase(key);
        rj[2] = pq_ok;
        ++diff_.nnotify;
        break;
    case pq_stats:
        rj[2] = pq_ok;
        rj[3] = server.stats();
        rj[3]["id"] = server.me();
        break;
    case pq_control:
        rj[2] = pq_ok;
        rj[3] = Json::make_object();

        // stuff the server does not know about
        if (j[2].is_o()) {
            if (j[2]["is_ready"])
                rj[3] = ready_;
            else if (j[2]["get_log"])
                rj[3] = Json().set("backend", (part_) ? part_->is_backend(me_->seqid()) : false)
                              .set("data", log_.as_json())
                              .set("internal", server.logs());
            else if (j[2]["write_log"])
                log_.write_json(std::cerr);
            else if (j[2]["clear_log"])
                log_.clear();
            else if (j[2]["tamer_blocking"]) {
                std::vector<std::string> x;
                tamer::driver::main->blocked_locations(x);
                std::sort(x.begin(), x.end(), String::natural_comparator());
                rj[3] = Json(x.begin(), blocked_locations_shrink(x.begin(), x.end()));
            }
            else if (j[2]["client_status"]) {
                rj[3].set("clients", Json::array())
                     .set("interconnect", Json::array());
                for (auto& c : clients_)
                    if (c != mpfd)
                        rj[3]["clients"].push_back(c->status());
                Json interconnect;
                for (size_t idx = 0; idx != interconnect_.size(); ++idx)
                    if (pq::Interconnect* i = interconnect_[idx]) {
                        Json j = Json().set("id", idx);
                        interconnect.push_back(j.merge(i->fd()->status()));
                    } else
                        interconnect.push_back(Json());
                rj[3]["interconnect"] = interconnect;
            }
            else if (j[2]["interconnect"]) {
                peer = j[2]["interconnect"].as_i();
                assert(peer >= 0 && peer < (int32_t)interconnect_.size());

                interconnect_[peer] = new pq::Interconnect(mpfd, peer);
                interconnect_[peer]->set_wrlowat(1 << 12);
                clients_.erase(mpfd);
            }
        }

        server.control(j[2]);
        break;
    case pq_noop_get:
        rj[2] = pq_ok;
        key = j[2].as_s();
        rj[3] = noop_val;
        break;
    }

 finish:
    mpfd->write(rj);
}

tamed void connector(tamer::fd cfd, msgpack_fd* mpfd, pq::Server& server) {
    tvars {
        msgpack_fd* mpfd_;
        bool ok;
        uint32_t proc = 0;
    }

    if (mpfd)
        mpfd_ = mpfd;
    else {
        mpfd_ = new msgpack_fd(cfd);
        mpfd_->set_description(String("clientfd") + String(cfd.value()));
        clients_.insert(mpfd_);
    }

    mpfd_->set_wrlowat(1 << 13);

    while (cfd) {
        twait { read_and_process_one(mpfd_, server, make_event(ok)); }
        if (!ok)
            break;

        // round robin connections with data to read
        if (round_robin_ && ++proc == round_robin_) {
            twait { tamer::at_asap(make_event()); }
            proc = 0;
        }
    }

    if (clients_.erase(mpfd_))
        std::cerr << "closed client connection: " << strerror(-cfd.error())
                  << std::endl;
    else
        std::cerr << "closed interconnect: " << strerror(-cfd.error())
                  << std::endl;

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
    twait { mpfd.call(Json::array(pq_control, 1, Json().set("quit", true)),
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

tamed void initialize_interconnect(pq::Server& server, const pq::Hosts* hosts, 
                                   tamer::event<bool> done) {
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        bool connected = true;
        int32_t i;
        Json j;
    }

    for (i = 0; i < hosts->size(); ++i) {
        if (!interconnect_[i] && i < me_->seqid()) {
            twait {
                auto h = hosts->get_by_seqid(i);
                pq::sock_helper::make_sockaddr(h->name().c_str(), h->port(), sin);
                tamer::tcp_connect(sin.sin_addr, h->port(), make_event(fd));
            }

            if (!fd) {
                connected = false;
                break;
            }

            interconnect_[i] = new pq::Interconnect(fd, i);
            interconnect_[i]->set_wrlowat(1 << 12);
            twait {
                interconnect_[i]->control(Json().set("interconnect", me_->seqid()),
                                          make_event(j));
            }

            connector(fd, interconnect_[i]->fd(), server);
        }
        else if (!interconnect_[i] && i != me_->seqid()) {
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
        log_.record_at("nnotify", now, diff_.nnotify);

        lu = u;
        before = now;
        memset(&diff_, 0, sizeof(nrpc));
        twait volatile { tamer::at_delay_sec(1, make_event()); }
    }
}

} // namespace

tamed void server_loop(pq::Server& server, int port, bool kill,
                       const pq::Hosts* hosts, const pq::Host* me,
                       const pq::Partitioner* part, uint32_t round_robin) {
    tvars {
        tamer::fd killer;
        bool connected = false;
        double delay = 0.005;
    }

    memset(&diff_, 0, sizeof(nrpc));
    periodic_logger();

    round_robin_ = round_robin;

    if (kill) {
        twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, port, make_event(killer)); }
        if (killer) {
            std::cerr << "killing existing server on port " << port << "\n";
            twait { kill_server(killer, port, make_event()); }
        }
    }

    if (hosts) {
        mandatory_assert(part && me);
        part_ = part;
        me_ = me;
        interconnect_.assign(hosts->size(), nullptr);
    }

    std::cerr << "listening on port " << port << "\n";
    acceptor(tamer::tcp_listen(port), server);

    // if this is a cluster deployment, make connections to each server
    if (hosts) {
        do {
            twait { tamer::at_delay(delay, make_event()); }
            twait { initialize_interconnect(server, hosts, 
                                            make_event(connected)); }
        } while(!connected);

        server.set_cluster_details(me_->seqid(), interconnect_, part_);
    }

    ready_ = true;
    interrupt_catcher();
}

tamed void block_report_loop(int32_t delay) {
    while (1) {
        twait { tamer::at_delay(delay, make_event(), true); }
        {
            std::vector<std::string> x;
            tamer::driver::main->blocked_locations(x);
            std::cerr << tamer::now() << ": Blocked at:\n";
            std::sort(x.begin(), x.end(), String::natural_comparator());
            auto e = blocked_locations_shrink(x.begin(), x.end());
            for (auto it = x.begin(); it != e; ++it)
                std::cerr << "  " << *it << std::endl;
        }
    }
}
