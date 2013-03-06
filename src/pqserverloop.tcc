// -*- mode: c++ -*-
#include <tamer/fd.hh>
#include <stdio.h>
#include <string.h>
#include "pqserver.hh"
#include "mpfd.hh"

namespace {

tamed void connector(tamer::fd cfd) {
    tvars {
        Json j;
        msgpack_fd mpfd(cfd);
    }
    while (cfd) {
        twait { mpfd.read(make_event(j)); }
    }
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
    acceptor(tamer::tcp_listen(port));
}
