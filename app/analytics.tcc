#include "analytics.hh"
#include <tamer/tamer.hh>
#include "pqremoteclient.hh"

namespace pq {

tamed void run_analytics_remote(const Json& tp, int client_port) {
    tvars {
        tamer::fd fd;
        RemoteClient* rc;
        AnalyticsShim<RemoteClient>* shim;
        AnalyticsRunner<AnalyticsShim<RemoteClient> >* ar;
    }
    std::cerr << "connecting to port " << client_port << "\n";
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, client_port, make_event(fd)); }
    if (!fd) {
        std::cerr << "port " << client_port << ": "
                  << strerror(-fd.error()) << "\n";
        exit(1);
    }
    rc = new RemoteClient(fd);
    shim = new AnalyticsShim<RemoteClient>(*rc);
    ar = new AnalyticsRunner<AnalyticsShim<RemoteClient> >(*shim, tp);
    ar->safe_run();
}
}
