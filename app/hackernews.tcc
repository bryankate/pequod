#include "hackernews.hh"
#include "pqmulticlient.hh"
#include "redisadapter.hh"

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

typedef pq::HashHackerNewsShim<pq::RedisfdHashClient> redis_shim_type;

tamed void run_hn_remote_redis(HackernewsPopulator& hp) {
    tvars {
        tamer::fd fd;
        pq::RedisfdHashClient* client;
        redis_shim_type* shim;
        pq::HackernewsRunner<redis_shim_type>* hr;
    }
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, 6379, make_event(fd)); }
    client = new pq::RedisfdHashClient(fd);
    shim = new redis_shim_type(*client);
    hr = new pq::HackernewsRunner<redis_shim_type>(*shim, hp);
    twait { hr->populate(make_event()); }
    twait { hr->run(make_event()); }
}

}
