#include "hackernews.hh"
#include "pqmulticlient.hh"
#include "redisadapter.hh"
#include "memcacheadapter.hh"
#include "pqdbpool.hh"

namespace pq {

tamed void run_hn_remote(HackernewsPopulator& hp, int client_port,
                         const Hosts* hosts, const Hosts* dbhosts,
                         const Partitioner* part) {
    tvars {
        MultiClient* mc = new MultiClient(hosts, part, client_port);
        PQHackerNewsShim<MultiClient>* shim = new PQHackerNewsShim<MultiClient>(*mc, hp.writearound());
        HackernewsRunner<PQHackerNewsShim<MultiClient>>* hr = new HackernewsRunner<PQHackerNewsShim<MultiClient> >(*shim, hp);
        double start, midway, end;
    }
    twait { mc->connect(make_event()); }
    twait { hr->populate(make_event()); }
    midway = tstamp();
    twait { hr->run(make_event()); }
    end = tstamp();
    std::cerr << "Populate took " << (midway-start)/1000000 << " Run took " << (end-midway)/1000000 << "\n";
    delete hr;
    delete shim;
    delete mc;
}


typedef pq::SQLHackerNewsShim<pq::DBPool> db_shim_type;

tamed void run_hn_remote_db(HackernewsPopulator& hp, const DBPoolParams& dbparams) {
    tvars {
        pq::DBPool* client = new pq::DBPool(dbparams);
        db_shim_type* shim = new db_shim_type(*client);
        pq::HackernewsRunner<db_shim_type>* hr = new pq::HackernewsRunner<db_shim_type>(*shim, hp);
    }

    client->connect();
    twait { hr->populate(make_event()); }
    twait { hr->run(make_event()); }
    delete hr;
    delete shim;
    delete client;
}


#if HAVE_HIREDIS_HIREDIS_H
typedef pq::HashHackerNewsShim<pq::RedisClient> redis_shim_type;

tamed void run_hn_remote_redis(HackernewsPopulator& hp) {
    tvars {
        pq::RedisClient* client = new pq::RedisClient();
        redis_shim_type* shim = new redis_shim_type(*client);
        pq::HackernewsRunner<redis_shim_type>* hr = new pq::HackernewsRunner<redis_shim_type>(*shim, hp);
    }

    client->connect();
    twait { hr->populate(make_event()); }
    twait { hr->run(make_event()); }
    delete hr;
    delete shim;
    delete client;
}
#else
void run_hn_remote_redis(HackernewsPopulator&) {
    mandatory_assert(false && "Not configured for Redis!");
}
#endif


#if HAVE_MEMCACHED_PROTOCOL_BINARY_H
typedef pq::HashHackerNewsShim<pq::MemcacheClient> memcache_shim_type;

tamed void run_hn_remote_memcache(HackernewsPopulator& hp) {
    tvars {
        pq::MemcacheClient* client = new pq::MemcacheClient();
        memcache_shim_type* shim = new memcache_shim_type(*client);
        pq::HackernewsRunner<memcache_shim_type>* hr = new pq::HackernewsRunner<memcache_shim_type>(*shim, hp);
    }

    twait { client->connect(make_event()); }
    twait { hr->populate(make_event()); }
    twait { hr->run(make_event()); }
    delete hr;
    delete shim;
    delete client;
}
#else
void run_hn_remote_memcache(HackernewsPopulator&) {
    mandatory_assert(false && "Not configured for Memcache!");
}
#endif

}
