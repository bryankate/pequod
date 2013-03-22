#include "pqhackernews.hh"
#include "pqremoteclient.hh"

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
    printf("DONE POPULATE\n");
    midway = tstamp();
    twait { hr->run(make_event()); }
    printf("DONE RUN\n");
    end = tstamp();
    std::cout << "Populate took " << (midway-start)/1000000 << " Run took " << (end-midway)/1000000 << "\n";
    delete hr;
    delete shim;
    delete rc;
}

}
