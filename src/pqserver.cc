#include <boost/random/random_number_generator.hpp>
#include <unistd.h>
#include <set>
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "pqtwitter.hh"
#include "pqtwitternew.hh"
#include "pqfacebook.hh"
#include "pqhackernews.hh"
#include "pqanalytics.hh"
#include "pqclient.hh"
#include "clp.h"
#include "time.hh"
#include "hashclient.hh"
#include "hnshim.hh"
#include "pqrwmicro.hh"

#if HAVE_POSTGRESQL_LIBPQ_FE_H
#include <postgresql/libpq-fe.h>
#include "pgclient.hh"
#endif


namespace pq {

// XXX check circular expansion


Table::Table(Str name)
    : ninsert_(0), nmodify_(0), nerase_(0), namelen_(name.length()) {
    assert(namelen_ <= (int) sizeof(name_));
    memcpy(name_, name.data(), namelen_);
}

const Table Table::empty_table{Str()};

Table::~Table() {
    clear();
}

void Table::clear() {
    while (SourceRange* r = source_ranges_.unlink_leftmost_without_rebalance()) {
        r->clear_without_deref();
        delete r;
    }
    while (JoinRange* r = join_ranges_.unlink_leftmost_without_rebalance())
        delete r;
    // delete store last since join_ranges_ have refs to Datums
    while (Datum* d = store_.unlink_leftmost_without_rebalance())
        delete d;
}

void Table::add_source(SourceRange* r) {
    for (auto it = source_ranges_.begin_contains(r->interval());
	 it != source_ranges_.end(); ++it)
	if (it->join() == r->join() && it->joinpos() == r->joinpos()) {
	    // XXX may copy too much. This will not actually cause visible
	    // bugs I think?, but will grow the store
	    it->take_results(*r);
            delete r;
	    return;
	}
    source_ranges_.insert(r);
}

void Table::remove_source(Str first, Str last, ValidJoinRange* sink) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
	 it != source_ranges_.end(); )
	if (it->join() == sink->join()) {
            SourceRange* sr = it.operator->();
            ++it;

            sr->remove_sink(sink);
            if (sr->empty()) {
                source_ranges_.erase(sr);
                delete sr;
            }
	} else
            ++it;
}

void Table::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    FileErrorHandler xerrh(stderr);
    errh = errh ? errh : &xerrh;

    // check for redundant join
    for (auto it = join_ranges_.begin_overlaps(first, last);
         it != join_ranges_.end(); ++it)
        if (it->join()->same_structure(*join)) {
            errh->error("join on [%p{Str}, %p{Str}) has same structure as overlapping join\n(new join ignored)", &first, &last);
            return;
        }

    join_ranges_.insert(new JoinRange(first, last, join));
}

void Server::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    join->attach(*this);
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_join(first, last, join, errh);
}

void Table::insert(Str key, String value) {
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
        value = String();
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
        d->value().swap(value);
    }
    notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    ++ninsert_;
}

void Table::erase(Str key) {
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end())
        erase(it);
    ++nerase_;
}

Json Server::stats() const {
    size_t store_size = 0, source_ranges_size = 0, join_ranges_size = 0,
        valid_ranges_size = 0;
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);
    for (auto& t : tables_) {
        store_size += t.store_.size();
        source_ranges_size += t.source_ranges_.size();
        join_ranges_size += t.join_ranges_.size();
        for (auto it = t.join_ranges_.begin(); it != t.join_ranges_.end(); ++it)
            valid_ranges_size += it->valid_ranges_size();
    }

    Json tables = Json::make_array();
    for (auto t = tables_by_name_.begin(); t != tables_by_name_.end(); ++t) {
        Json pt = Json().set("name", t->name())
                        .set("ninsert", t->ninsert_)
                        .set("nmodify", t->nmodify_)
                        .set("nerase", t->nerase_);
        tables.push_back(pt);
    }

    return Json().set("store_size", store_size)
	.set("source_ranges_size", source_ranges_size)
	.set("join_ranges_size", join_ranges_size)
	.set("valid_ranges_size", valid_ranges_size)
        .set("server_user_time", to_real(ru.ru_utime))
        .set("server_system_time", to_real(ru.ru_stime))
        .set("server_max_rss_mb", ru.ru_maxrss / 1024)
        .set("source_allocated_key_bytes", SourceRange::allocated_key_bytes)
        .set("sink_allocated_key_bytes", ServerRangeBase::allocated_key_bytes)
        .set("tables", tables);
}

void Server::print(std::ostream& stream) {
    stream << "sources:" << std::endl;
    bool any = false;
    for (auto& t : tables_by_name_)
        if (!t.source_ranges_.empty()) {
            stream << t.source_ranges_;
            any = true;
        }
    if (!any)
        stream << "<empty>\n";
}

} // namespace


static Clp_Option options[] = {
    // modes (which builtin app to run
    { "twitter", 0, 1000, 0, Clp_Negate },
    { "twitternew", 0, 1001, 0, Clp_Negate },
    { "facebook", 'f', 1002, 0, Clp_Negate },
    { "rwmicro", 0, 1003, 0, Clp_Negate },
    { "tests", 0, 1004, 0, 0 },
    { "hn", 'h', 1005, 0, Clp_Negate },
    { "analytics", 0, 1006, 0, Clp_Negate },
#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
    { "memcached", 0, 1009, 0, Clp_Negate },
#endif
    { "builtinhash", 'b', 1008, 0, Clp_Negate },

    // rpc params
    { "client", 'c', 2000, Clp_ValInt, Clp_Optional },
    { "listen", 'l', 2001, Clp_ValInt, Clp_Optional },

    // params that are generally useful to multiple apps
    { "push", 'p', 3000, 0, Clp_Negate },
    { "pull", 3001, 0, Clp_Negate },
    { "duration", 'd', 3002, Clp_ValInt, 0 },
    { "nusers", 'n', 3003, Clp_ValInt, 0 },
    { "synchronous", 0, 3004, 0, Clp_Negate },
    { "seed", 0, 3005, Clp_ValInt, 0 },
    { "log", 0, 3006, 0, Clp_Negate },
    { "nops", 'o', 3007, Clp_ValInt, 0 },

    // mostly twitter params
    { "shape", 0, 4000, Clp_ValDouble, 0 },
    { "popduration", 0, 4001, Clp_ValInt, 0 },
    { "pread", 0, 4002, Clp_ValInt, 0 },
    { "ppost", 0, 4003, Clp_ValInt, 0 },
    { "psubscribe", 0, 4005, Clp_ValInt, 0 },
    { "graph", 0, 4006, Clp_ValStringNotOption, 0 },

    // mostly HN params
    { "narticles", 'a', 5000, Clp_ValInt, 0 },
    { "vote_rate", 'v', 5001, Clp_ValInt, 0 },
    { "comment_rate", 'r', 5002, Clp_ValInt, 0 },
    { "pg", 0, 5003, 0, Clp_Negate },
    { "hnusers", 'x', 5004, Clp_ValInt, 0 },
    { "large", 0, 5005, 0, Clp_Negate },
    { "super_materialize", 's', 5006, 0, Clp_Negate },

    // mostly analytics params
    { "proactive", 0, 6000, 0, Clp_Negate },
    { "buffer", 0, 6001, 0, Clp_Negate }
};

enum { mode_unknown, mode_twitter, mode_twitternew, mode_hn, mode_facebook,
       mode_analytics, mode_listen, mode_tests, mode_rwmicro };
static char envstr[] = "TAMER_NOLIBEVENT=1";

int main(int argc, char** argv) {
    putenv(envstr);
    tamer::initialize();

    int mode = mode_unknown, listen_port = 8000, client_port = -1;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    Json tp_param = Json().set("nusers", 5000);
    std::set<String> testcases;
    while (Clp_Next(clp) != Clp_Done) {
        // modes
        if (clp->option->long_name == String("twitter"))
            mode = mode_twitter;
        else if (clp->option->long_name == String("twitternew"))
            mode = mode_twitternew;
        else if (clp->option->long_name == String("facebook"))
            mode = mode_facebook;
        else if (clp->option->long_name == String("rwmicro"))
            mode = mode_rwmicro;
        else if (clp->option->long_name == String("tests"))
            mode = mode_tests;
        else if (clp->option->long_name == String("hn"))
            mode = mode_hn;
        else if (clp->option->long_name == String("analytics"))
            mode = mode_analytics;
        else if (clp->option->long_name == String("memcached"))
            tp_param.set("memcached", !clp->negated);
        else if (clp->option->long_name == String("builtinhash"))
            tp_param.set("builtinhash", !clp->negated);

        // rpc
        else if (clp->option->long_name == String("client"))
            client_port = clp->have_val ? clp->val.i : 8000;
        else if (clp->option->long_name == String("listen")) {
            mode = mode_listen;
            if (clp->have_val)
                listen_port = clp->val.i;
        }

        // general
	if (clp->option->long_name == String("push"))
	    tp_param.set("push", !clp->negated);
        if (clp->option->long_name == String("pull"))
            tp_param.set("pull", !clp->negated);
        else if (clp->option->long_name == String("duration"))
            tp_param.set("duration", clp->val.i);
	else if (clp->option->long_name == String("nusers"))
	    tp_param.set("nusers", clp->val.i);
        else if (clp->option->long_name == String("synchronous"))
            tp_param.set("synchronous", !clp->negated);
        else if (clp->option->long_name == String("seed"))
            tp_param.set("seed", clp->val.i);
        else if (clp->option->long_name == String("log"))
            tp_param.set("log", !clp->negated);
        else if (clp->option->long_name == String("nops"))
            tp_param.set("nops", clp->val.i);

        // twitter
        else if (clp->option->long_name == String("shape"))
            tp_param.set("shape", clp->val.d);
        else if (clp->option->long_name == String("popduration"))
            tp_param.set("popduration", clp->val.i);
        else if (clp->option->long_name == String("pread"))
            tp_param.set("pread", clp->val.i);
        else if (clp->option->long_name == String("ppost"))
            tp_param.set("ppost", clp->val.i);
        else if (clp->option->long_name == String("psubscribe"))
            tp_param.set("psubscribe", clp->val.i);
        else if (clp->option->long_name == String("graph"))
            tp_param.set("graph", clp->val.s);

        // hn
	else if (clp->option->long_name == String("narticles"))
	    tp_param.set("narticles", clp->val.i);
	else if (clp->option->long_name == String("vote_rate"))
	    tp_param.set("vote_rate", clp->val.i);
	else if (clp->option->long_name == String("comment_rate"))
	    tp_param.set("comment_rate", clp->val.i);
        else if (clp->option->long_name == String("pg"))
            tp_param.set("pg", !clp->negated);
        else if (clp->option->long_name == String("hnusers"))
            tp_param.set("hnusers", clp->val.i);
        else if (clp->option->long_name == String("large"))
            tp_param.set("large", !clp->negated);
	else if (clp->option->long_name == String("super_materialize"))
	    tp_param.set("super_materialize", !clp->negated);

        // analytics
        else if (clp->option->long_name == String("proactive"))
            tp_param.set("proactive", !clp->negated);
        else if (clp->option->long_name == String("buffer"))
            tp_param.set("buffer", !clp->negated);

        // run single unit test
        else
            testcases.insert(clp->vstr);
    }

    pq::Server server;
    if (mode == mode_tests || !testcases.empty()) {
        extern void unit_tests(const std::set<String> &);
        unit_tests(testcases);
    } else if (mode == mode_listen) {
        extern void server_loop(int port, pq::Server& server);
        server_loop(listen_port, server);
    } else if (mode == mode_twitter || mode == mode_unknown) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 8);
        pq::TwitterPopulator tp(tp_param);
        if (tp_param["memcached"]) {
#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
            mandatory_assert(tp_param["push"], "memcached pull is not supported");
            pq::MemcachedClient client;
            pq::TwitterHashShim<pq::MemcachedClient> shim(client);
            pq::TwitterRunner<decltype(shim)> tr(shim, tp);
            tr.populate();
            tr.run(tamer::event<>());
#else
            mandatory_assert(false);
#endif
        } else if (tp_param["builtinhash"]) {
            mandatory_assert(tp_param["push"], "builtinhash pull is not supported");
            pq::BuiltinHashClient client;
            pq::TwitterHashShim<pq::BuiltinHashClient> shim(client);
            pq::TwitterRunner<decltype(shim)> tr(shim, tp);
            tr.populate();
            tr.run(tamer::event<>());
        } else if (client_port >= 0) {
            run_twitter_remote(tp, client_port);
        } else {
            pq::DirectClient dc(server);
            pq::TwitterShim<pq::DirectClient> shim(dc);
            pq::TwitterRunner<decltype(shim)> tr(shim, tp);
            tr.populate();
            tr.run(tamer::event<>());
        }
    } else if (mode == mode_twitternew) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 8);
        pq::TwitterNewPopulator tp(tp_param);

        if (client_port >= 0)
            run_twitter_new_remote(tp, client_port);
        else {
            pq::DirectClient client(server);
            pq::TwitterNewShim<pq::DirectClient> shim(client);
            pq::TwitterNewRunner<decltype(shim)> tr(shim, tp);
            tr.populate();
            tr.run(tamer::event<>());
        }
    } else if (mode == mode_hn) {
        if (tp_param["large"]) {
            tp_param.set("narticles", 100000);
            tp_param.set("hnusers", 50000);
        }
        pq::HackernewsPopulator hp(tp_param);
        if (tp_param["builtinhash"]) {
            pq::BuiltinHashClient client;
            pq::HashHackerNewsShim<pq::BuiltinHashClient> shim(client);
            pq::HackernewsRunner<decltype(shim)> hr(shim, hp);
            hr.populate();
            hr.run();
        } else if (tp_param["memcached"]) {
#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
            pq::MemcachedClient client;
            pq::HashHackerNewsShim<pq::MemcachedClient> shim(client);
            pq::HackernewsRunner<decltype(shim)> hr(shim, hp);
            hr.populate();
            hr.run();
#else
            mandatory_assert(false);
#endif
        } else if (tp_param["pg"]) {
#if HAVE_POSTGRESQL_LIBPQ_FE_H
            pq::PostgresClient client;
            pq::SQLHackernewsShim<pq::PostgresClient> shim(client);
            pq::HackernewsRunner<decltype(shim)> hr(shim, hp);
            hr.populate();
            hr.run();
#else
            mandatory_assert(false);
#endif
        } else {
            pq::PQHackerNewsShim<pq::Server> shim(server);
            pq::HackernewsRunner<decltype(shim)> hr(shim, hp);
            hr.populate();
            hr.run();
        }
    } else if (mode == mode_facebook) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 5);
        pq::FacebookPopulator fp(tp_param);
        pq::facebook_populate(server, fp);
        pq::facebook_run(server, fp);
    } else if (mode == mode_analytics) {
        pq::AnalyticsRunner ar(server, tp_param);
        ar.populate();
        ar.run();
    } else if (mode == mode_rwmicro) {
        pq::RwMicro rw(tp_param, server);
        rw.populate();
        rw.run();
    }

    tamer::loop();
    tamer::cleanup();
}
