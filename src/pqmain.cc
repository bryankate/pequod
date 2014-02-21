#include "clp.h"
#include "time.hh"
#include "json.hh"
#include "hosts.hh"
#include "partitioner.hh"
#include "sock_helper.hh"
#include "pqserver.hh"
#include "pqpersistent.hh"
#include "pqdbpool.hh"
#include "pqclient.hh"
#include "twitter.hh"
#include "twitternew.hh"
#include "facebook.hh"
#include "hackernews.hh"
#include "hackernewsshim.hh"
#include "analytics.hh"
#include "rwmicro.hh"
#include "redisadapter.hh"
#include "memcacheadapter.hh"
#include "hashtableadapter.hh"
#include <boost/random/random_number_generator.hpp>
#include <unistd.h>
#include <set>

static Clp_Option options[] = {
    // modes (which builtin app to run
    { "twitter", 0, 1000, 0, Clp_Negate },
    { "twitternew", 0, 1001, 0, Clp_Negate },
    { "facebook", 'f', 1002, 0, Clp_Negate },
    { "rwmicro", 0, 1003, 0, Clp_Negate },
    { "tests", 0, 1004, 0, 0 },
    { "hn", 'h', 1005, 0, Clp_Negate },
    { "analytics", 0, 1006, 0, Clp_Negate },
    { "builtinhash", 'b', 1008, 0, Clp_Negate },
    { "memcached", 0, 1009, 0, Clp_Negate },
    { "redis", 0, 1010, 0, Clp_Negate},

    // rpc params
    { "client", 'c', 2000, Clp_ValInt, Clp_Optional },
    { "listen", 'l', 2001, Clp_ValInt, Clp_Optional },
    { "kill", 'k', 2002, 0, Clp_Negate },
    { "hostfile", 'H', 2003, Clp_ValStringNotOption, 0 },
    { "dbhostfile", 0, 2004, Clp_ValStringNotOption, 0 },
    { "partfunc", 'P', 2005, Clp_ValStringNotOption, 0 },
    { "nbacking", 'B', 2006, Clp_ValInt, 0 },
    { "writearound", 0, 2007, 0, Clp_Negate },
    { "round-robin", 0, 2008, Clp_ValInt, 0 },
    { "block-report", 0, 2009, Clp_ValInt, 0 },

    // params that are generally useful to multiple apps
    { "push", 'p', 3000, 0, Clp_Negate },
    { "pull", 0, 3001, 0, Clp_Negate },
    { "duration", 'd', 3002, Clp_ValInt, 0 },
    { "nusers", 'n', 3003, Clp_ValInt, 0 },
    { "synchronous", 0, 3004, 0, Clp_Negate },
    { "seed", 0, 3005, Clp_ValInt, 0 },
    { "log", 0, 3006, 0, Clp_Negate },
    { "nops", 'o', 3007, Clp_ValInt, 0 },
    { "verbose", 0, 3008, 0, Clp_Negate },
    { "subtables", 0, 3009, 0, Clp_Negate },
    { "ngroups", 0, 3010, Clp_ValInt, 0 },
    { "groupid", 0, 3011, Clp_ValInt, 0 },
    { "initialize", 0, 3012, 0, Clp_Negate },
    { "populate", 0, 3013, 0, Clp_Negate },
    { "execute", 0, 3014, 0, Clp_Negate },
    { "dbname", 0, 3015, Clp_ValString, 0 },
    { "dbenvpath", 0, 3016, Clp_ValString, 0 },
    { "dbhost", 0, 3017, Clp_ValString, 0 },
    { "dbport", 0, 3018, Clp_ValInt, 0 },
    { "dbpool-min", 0, 3019, Clp_ValInt, 0 },
    { "dbpool-max", 0, 3020, Clp_ValInt, 0 },
    { "dbpool-depth", 0, 3021, Clp_ValInt, 0 },
    { "postgres", 0, 3022, 0, Clp_Negate },
    { "monitordb", 0, 3023, 0, Clp_Negate },
    { "mem-lo", 0, 3024, Clp_ValInt, 0 },
    { "mem-hi", 0, 3025, Clp_ValInt, 0 },
    { "evict-inline", 0, 3026, 0, Clp_Negate },
    { "evict-periodic", 0, 3027, 0, Clp_Negate },
    { "print-table", 0, 3028, Clp_ValStringNotOption, 0 },
    { "progress-report", 0, 3029, 0, Clp_Negate },

    // mostly twitter params
    { "shape", 0, 4000, Clp_ValDouble, 0 },
    { "popduration", 0, 4001, Clp_ValInt, 0 },
    { "pread", 0, 4002, Clp_ValDouble, 0 },
    { "ppost", 0, 4003, Clp_ValDouble, 0 },
    { "psubscribe", 0, 4004, Clp_ValDouble, 0 },
    { "plogout", 0, 4006, Clp_ValDouble, 0 },
    { "graph", 0, 4007, Clp_ValStringNotOption, 0 },
    { "visualize", 0, 4008, 0, Clp_Negate },
    { "celebrity", 0, 4010, Clp_ValInt, 0 },
    { "celebrity2", 0, 4011, Clp_ValInt, 0 },
    { "celebrity3", 0, 4012, Clp_ValInt, 0 },
    { "celebrity4", 0, 4013, Clp_ValInt, 0 },
    { "postlimit", 0, 4014, Clp_ValInt, 0 },
    { "checklimit", 0, 4015, Clp_ValInt, 0 },
    { "fetch", 0, 4016, 0, Clp_Negate },
    { "prevalidate", 0, 4017, 0, Clp_Negate },
    { "prevalidate-inactive", 0, 4018, 0, Clp_Negate },
    { "prevalidate-before-sub", 0, 4019, 0, Clp_Negate },
    { "binary", 0, 4020, 0, Clp_Negate },
    { "dbshim", 0, 4021, 0, Clp_Negate },
    { "master-host", 0, 4022, Clp_ValStringNotOption, 0 },
    { "master-port", 0, 4023, Clp_ValInt, 0 },

    // mostly HN params
    { "narticles", 'a', 5000, Clp_ValInt, 0 },
    { "vote_rate", 'v', 5001, Clp_ValInt, 0 },
    { "comment_rate", 'r', 5002, Clp_ValInt, 0 },
    { "pg", 0, 5003, 0, Clp_Negate },
    { "hnusers", 'x', 5004, Clp_ValInt, 0 },
    { "large", 0, 5005, 0, Clp_Negate },
    { "super_materialize", 's', 5006, 0, Clp_Negate },
    { "populate_only", 0, 5007, 0, Clp_Negate },
    { "run_only", 0, 5008, 0, Clp_Negate },

    // mostly analytics params
    { "proactive", 0, 6000, 0, Clp_Negate },
    { "buffer", 0, 6001, 0, Clp_Negate },

    // rwmicro params
    { "prefresh", 0, 7000, Clp_ValDouble, 0 },
    { "nfollower", 0, 7001, Clp_ValDouble, 0 },
    { "pprerefresh", 0, 7002, Clp_ValDouble, 0 },
    { "pactive", 0, 7003, Clp_ValDouble, 0 },
    { "client_push", 0, 7004, 0, Clp_Negate },
    { "postlen", 0, 7005, Clp_ValInt, 0 },
};

enum { mode_unknown, mode_twitter, mode_twitternew, mode_hn, mode_facebook,
       mode_analytics, mode_listen, mode_tests, mode_rwmicro };
enum { db_unknown, db_postgres };

int main(int argc, char** argv) {
    tamer::initialize();

    int mode = mode_unknown, db = db_unknown;
    int listen_port = 8000, client_port = -1, nbacking = 0;
    bool kill_old_server = false;
    String hostfile, dbhostfile, partfunc;
    pq::DBPoolParams db_param;
    bool monitordb = false;
    uint64_t mem_hi_mb = 0, mem_lo_mb = 0;
    uint32_t round_robin = 0;
    bool evict_inline = false, evict_periodic = false;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    Json tp_param = Json().set("nusers", 5000);
    int32_t block_report = 0;
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
        else if (clp->option->long_name == String("redis"))
            tp_param.set("redis", !clp->negated);
        else if (clp->option->long_name == String("builtinhash"))
            tp_param.set("builtinhash", !clp->negated);

        // rpc
        else if (clp->option->long_name == String("client"))
            client_port = clp->have_val ? clp->val.i : 8000;
        else if (clp->option->long_name == String("listen")) {
            mode = mode_listen;
            if (clp->have_val)
                listen_port = clp->val.i;
        } else if (clp->option->long_name == String("kill"))
            kill_old_server = !clp->negated;
        else if (clp->option->long_name == String("hostfile"))
             hostfile = clp->val.s;
        else if (clp->option->long_name == String("dbhostfile"))
             dbhostfile = clp->val.s;
        else if (clp->option->long_name == String("partfunc"))
             partfunc = clp->val.s;
        else if (clp->option->long_name == String("nbacking"))
            nbacking = clp->val.i;
        else if (clp->option->long_name == String("writearound"))
            tp_param.set("writearound", !clp->negated);
        else if (clp->option->long_name == String("round-robin"))
            round_robin = clp->val.i;
        else if (clp->option->long_name == String("block-report"))
            block_report = !clp->val.i;

        // general
        else if (clp->option->long_name == String("push"))
	    tp_param.set("push", !clp->negated);
        else if (clp->option->long_name == String("pull"))
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
        else if (clp->option->long_name == String("verbose"))
            tp_param.set("verbose", !clp->negated);
        else if (clp->option->long_name == String("subtables"))
            pq::Join::allow_subtables = !clp->negated;
        else if (clp->option->long_name == String("ngroups"))
            tp_param.set("ngroups", clp->val.i);
        else if (clp->option->long_name == String("groupid"))
            tp_param.set("groupid", clp->val.i);
        else if (clp->option->long_name == String("initialize"))
            tp_param.set("initialize", !clp->negated);
        else if (clp->option->long_name == String("populate"))
            tp_param.set("populate", !clp->negated);
        else if (clp->option->long_name == String("execute"))
            tp_param.set("execute", !clp->negated);
        else if (clp->option->long_name == String("dbname"))
            db_param.dbname = clp->val.s;
        else if (clp->option->long_name == String("dbhost"))
            db_param.host = clp->val.s;
        else if (clp->option->long_name == String("dbport"))
            db_param.port = clp->val.i;
        else if (clp->option->long_name == String("dbpool-min"))
            db_param.min = clp->val.i;
        else if (clp->option->long_name == String("dbpool-max"))
            db_param.max = clp->val.i;
        else if (clp->option->long_name == String("dbpool-depth"))
            db_param.pipeline_depth = clp->val.i;
        else if (clp->option->long_name == String("postgres"))
            db = db_postgres;
        else if (clp->option->long_name == String("monitordb"))
            monitordb = !clp->negated;
        else if (clp->option->long_name == String("mem-lo"))
            mem_lo_mb = clp->val.i;
        else if (clp->option->long_name == String("mem-hi"))
            mem_hi_mb = clp->val.i;
        else if (clp->option->long_name == String("evict-inline"))
            evict_inline = !clp->negated;
        else if (clp->option->long_name == String("evict-periodic"))
            evict_periodic = !clp->negated;
        else if (clp->option->long_name == String("print-table"))
            tp_param.set("print_table", clp->val.s);
        else if (clp->option->long_name == String("progress-report"))
            tp_param.set("progress_report", !clp->negated);

        // twitter
        else if (clp->option->long_name == String("shape"))
            tp_param.set("shape", clp->val.d);
        else if (clp->option->long_name == String("popduration"))
            tp_param.set("popduration", clp->val.i);
        else if (clp->option->long_name == String("pread"))
            tp_param.set("pread", clp->val.d);
        else if (clp->option->long_name == String("ppost"))
            tp_param.set("ppost", clp->val.d);
        else if (clp->option->long_name == String("psubscribe"))
            tp_param.set("psubscribe", clp->val.d);
        else if (clp->option->long_name == String("plogout"))
            tp_param.set("plogout", clp->val.d);
        else if (clp->option->long_name == String("graph"))
            tp_param.set("graph", clp->val.s);
        else if (clp->option->long_name == String("visualize"))
            tp_param.set("visualize", !clp->negated);
        else if (clp->option->long_name == String("celebrity"))
            tp_param.set("celebrity", clp->val.i);
        else if (clp->option->long_name == String("celebrity2"))
            tp_param.set("celebrity", clp->val.i).set("celebrity_type", 2);
        else if (clp->option->long_name == String("celebrity3"))
            tp_param.set("celebrity", clp->val.i).set("celebrity_type", 3);
        else if (clp->option->long_name == String("celebrity4"))
            tp_param.set("celebrity", clp->val.i).set("celebrity_type", 4);
        else if (clp->option->long_name == String("postlimit"))
            tp_param.set("postlimit", clp->val.i);
        else if (clp->option->long_name == String("checklimit"))
            tp_param.set("checklimit", clp->val.i);
        else if (clp->option->long_name == String("fetch"))
            tp_param.set("fetch", !clp->negated);
        else if (clp->option->long_name == String("prevalidate"))
            tp_param.set("prevalidate", !clp->negated);
        else if (clp->option->long_name == String("prevalidate-inactive"))
              tp_param.set("prevalidate_inactive", !clp->negated);
        else if (clp->option->long_name == String("prevalidate-before-sub"))
              tp_param.set("prevalidate_before_sub", !clp->negated);
        else if (clp->option->long_name == String("binary"))
            tp_param.set("binary", !clp->negated);
        else if (clp->option->long_name == String("dbshim"))
            tp_param.set("dbshim", !clp->negated);
        else if (clp->option->long_name == String("master-host"))
            tp_param.set("master_host", clp->val.s);
        else if (clp->option->long_name == String("master-port"))
            tp_param.set("master_port", clp->val.i);

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
        else if (clp->option->long_name == String("populate_only"))
            tp_param.set("populate_only", !clp->negated);
        else if (clp->option->long_name == String("run_only"))
            tp_param.set("run_only", !clp->negated);

        // analytics
        else if (clp->option->long_name == String("proactive"))
            tp_param.set("proactive", !clp->negated);
        else if (clp->option->long_name == String("buffer"))
            tp_param.set("buffer", !clp->negated);

        // rwmicro
        else if (clp->option->long_name == String("prefresh"))
            tp_param.set("prefresh", clp->val.d);
        else if (clp->option->long_name == String("nfollower"))
            tp_param.set("nfollower", clp->val.d);
        else if (clp->option->long_name == String("pprerefresh"))
            tp_param.set("pprerefresh", clp->val.d);
        else if (clp->option->long_name == String("pactive"))
            tp_param.set("pactive", clp->val.d);
        else if (clp->option->long_name == String("client_push"))
            tp_param.set("client_push", !clp->negated);
        else if (clp->option->long_name == String("postlen"))
            tp_param.set("postlen", clp->val.i);

        // run single unit test
        else
            testcases.insert(clp->vstr);
    }

    pq::Server server;
    const pq::Hosts* hosts = nullptr;
    const pq::Hosts* dbhosts = nullptr;
    const pq::Partitioner* part = nullptr;

    if (db != db_unknown) {
        pq::PersistentStore* pstore = nullptr;

        if (db == db_postgres) {
#if HAVE_LIBPQ
            pq::PostgresStore* pg = new pq::PostgresStore(db_param);
            pg->connect();
            pstore = pg;
#else
            mandatory_assert(false && "Not configured for PostgreSQL.");
#endif
        }
        else
            mandatory_assert(false && "Unknown DB type.");

        server.set_persistent_store(pstore, !monitordb);
        if (monitordb)
            pstore->run_monitor(server);
    }

    if (evict_inline && mem_hi_mb) {
        mandatory_assert(mem_lo_mb && "Need to set a low water mark.");
        server.set_eviction_details(mem_lo_mb, mem_hi_mb);
    }

    if (hostfile)
        hosts = pq::Hosts::get_instance(hostfile);
    if (dbhostfile)
        dbhosts = pq::Hosts::get_instance(dbhostfile);
    if (block_report) {
        extern void block_report_loop(int32_t delay);
        block_report_loop(block_report);
    }

    if (mode == mode_tests || !testcases.empty()) {
        extern void unit_tests(const std::set<String> &);
        unit_tests(testcases);
    } else if (mode == mode_listen) {
        const pq::Host* me = nullptr;
        if (hosts) {
            mandatory_assert(partfunc && "Need to specify a partition function!");
            part = pq::Partitioner::make(partfunc, nbacking, hosts->count(), -1);
            char hostname[100];
            gethostname(hostname, sizeof(hostname));
            me = hosts->get_by_uid(pq::sock_helper::get_uid(hostname, listen_port));
        }

        if (evict_periodic && mem_hi_mb)
            mandatory_assert(mem_lo_mb && "Need to set a low water mark.");
        else
            mem_hi_mb = 0;

        extern void server_loop(pq::Server& server, int port, bool kill,
                                const pq::Hosts* hosts, const pq::Host* me,
                                const pq::Partitioner* part,
                                uint64_t mem_lo_mb, uint64_t mem_hi_mb,
                                uint32_t round_robin);
        server_loop(server, listen_port, kill_old_server,
                    hosts, me, part, mem_lo_mb, mem_hi_mb, round_robin);
    } else if (mode == mode_twitter || mode == mode_unknown) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 8);

        pq::TwitterPopulator tp(tp_param);
        if (hosts)
            part = pq::Partitioner::make("twitter", nbacking, hosts->count(), -1);

        if (tp_param.get("memcached").as_b(false)) {
            mandatory_assert(hosts && part);
            mandatory_assert(tp_param["push"], "memcached pull is not supported");
            run_twitter_memcache(tp, hosts, part);
        } else if (tp_param["builtinhash"]) {
            mandatory_assert(tp_param["push"], "builtinhash pull is not supported");
            pq::BuiltinHashClient client;
            pq::TwitterHashShim<pq::BuiltinHashClient> shim(client);
            pq::TwitterRunner<decltype(shim)> tr(shim, tp);
            tr.populate(tamer::event<>());
            tr.run(tamer::event<>());
        } else if (client_port >= 0 || hosts) {
            run_twitter_remote(tp, client_port, hosts, dbhosts, part);
        } else {
            pq::DirectClient dc(server);
            pq::TwitterShim<pq::DirectClient> shim(dc);
            pq::TwitterRunner<decltype(shim)> tr(shim, tp);
            tr.populate(tamer::event<>());
            tr.run(tamer::event<>());
        }
    } else if (mode == mode_twitternew) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 8);
        pq::TwitterNewPopulator *tp = new pq::TwitterNewPopulator(tp_param);

        if (hosts)
            part = pq::Partitioner::make((tp->binary()) ? "twitternew" : "twitternew-text",
                                         nbacking, hosts->count(), -1);

        if (tp_param.get("dbshim").as_b(false))
            run_twitter_new_dbshim(*tp, db_param);
        else if (tp_param.get("redis").as_b(false))
            run_twitter_new_redis(*tp, hosts, part);
        else if (tp_param.get("memcached").as_b(false))
            run_twitter_new_memcache(*tp, hosts, part);
        else if (client_port >= 0 || hosts) {
            if (tp_param.get("writearound").as_b(false))
                mandatory_assert(dbhosts && part);

            run_twitter_new_remote(*tp, client_port, hosts, part, dbhosts, &db_param);
        }
        else
            run_twitter_new_local(*tp, server);

    } else if (mode == mode_hn) {
        if (tp_param["large"]) {
            tp_param.set("narticles", 100000);
            tp_param.set("hnusers", 50000);
        }
        pq::HackernewsPopulator* hp = new pq::HackernewsPopulator(tp_param);
        if (tp_param["builtinhash"]) {
            pq::BuiltinHashClient client;
            pq::HashHackerNewsShim<pq::BuiltinHashClient> shim(client);
            pq::HackernewsRunner<decltype(shim)> hr(shim, *hp);
            hr.populate(tamer::event<>());
            hr.run(tamer::event<>());
        } else if (tp_param["memcached"]) {
            run_hn_remote_memcache(*hp);
        } else if (tp_param["pg"]) {
            run_hn_remote_db(*hp, db_param);
        } else if (tp_param["redis"])
            run_hn_remote_redis(*hp);
	else {
	    if (client_port >= 0 || hosts) {
	        if (hosts)
	            part = pq::Partitioner::make("hackernews", nbacking, hosts->count(), -1);
	        if (tp_param.get("writearound").as_b(false))
	            mandatory_assert(dbhosts && part);
	        run_hn_remote(*hp, client_port, hosts, dbhosts, part);
	    }
	    else {
	        pq::DirectClient dc(server);
	        pq::PQHackerNewsShim<pq::DirectClient> shim(dc, hp->writearound());
	        pq::HackernewsRunner<decltype(shim)> hr(shim, *hp);
	        hr.populate(tamer::event<>());
	        hr.run(tamer::event<>());
	    }
        }
    } else if (mode == mode_facebook) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 5);
        pq::FacebookPopulator fp(tp_param);
        pq::facebook_populate(server, fp);
        pq::facebook_run(server, fp);
    } else if (mode == mode_analytics) {
        if (client_port >= 0)
            pq::run_analytics_remote(tp_param, client_port);
        else {
            pq::DirectClient client(server);
            pq::AnalyticsShim<pq::DirectClient> shim(client);
            pq::AnalyticsRunner<decltype(shim)> ar(shim, tp_param);
            ar.safe_run();
        }
    } else if (mode == mode_rwmicro) {
        if (tp_param["builtinhash"]) {
            pq::BuiltinHashClient client;
            pq::TwitterHashShim<pq::BuiltinHashClient> shim(client);
            pq::RwMicro<decltype(shim)> rw(tp_param, shim);
            rw.safe_run();
        }
        else if (tp_param["redis"])
            pq::run_rwmicro_redis(tp_param);
        else if (tp_param["memcache"])
            pq::run_rwmicro_memcache(tp_param);
        else if (client_port >= 0)
            pq::run_rwmicro_pqremote(tp_param, client_port);
        else {
            pq::DirectClient client(server);
            pq::TwitterShim<pq::DirectClient> shim(client);
            pq::RwMicro<decltype(shim)> rw(tp_param, shim);
            rw.safe_run();
        }
    }

    tamer::loop();
    tamer::cleanup();
}
