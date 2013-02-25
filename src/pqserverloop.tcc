// -*- mode: c++ -*-
#include <tamer/fd.hh>
#include <stdio.h>
#include <string.h>
#include "pqserver.hh"

namespace {

tamed void connector(tamer::fd cfd) {
    fprintf(stderr, "connection received (and closed)\n");
    cfd.close();
}

tamed void acceptor(tamer::fd listenfd) {
    tvars { tamer::fd cfd; };
    if (!listenfd)
	fprintf(stderr, "listen: %s\n", strerror(-listenfd.error()));
    while (listenfd) {
	twait { listenfd.accept(make_event(cfd)); }
	connector(cfd);
    }
}

} // namespace

void server_loop(int port, pq::Server&) {
    putenv("TAMER_NOLIBEVENT=1");
    tamer::initialize();
    acceptor(tamer::tcp_listen(port));
    tamer::loop();
    tamer::cleanup();
}
