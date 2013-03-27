#include "pqhackernews.hh"
#include "pqremoteclient.hh"
#include "hashclient.hh"

namespace pq {

tamed void run_hn_remote(HackernewsPopulator& hp, int client_port) {
    tvars {
        tamer::fd fd;
        RemoteClient* rc;
        PQHackerNewsShim<RemoteClient>* shim;
        HackernewsRunner<PQHackerNewsShim<RemoteClient> >* hr;
        double start, midway, end;
    }
    std::cerr << "connecting to port " << client_port << "\n";
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, client_port, make_event(fd)); }
    if (!fd) {
        std::cerr << "port " << client_port << ": "
                  << strerror(-fd.error()) << "\n";
        exit(1);
    }
    rc = new RemoteClient(fd);
    shim = new PQHackerNewsShim<RemoteClient>(*rc);
    hr = new HackernewsRunner<PQHackerNewsShim<RemoteClient> >(*shim, hp);

    start = tstamp();
    twait { hr->populate(make_event()); }
    midway = tstamp();
    twait { hr->run(make_event()); }
    end = tstamp();
    std::cerr << "Populate took " << (midway-start)/1000000 << " Run took " << (end-midway)/1000000 << "\n";
    delete hr;
    delete shim;
    delete rc;
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
