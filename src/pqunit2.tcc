// -*- mode: c++ -*-
#include "mpfd.hh"
#include "check.hh"

namespace {
void small_socket_buffer(int f) {
    int x = 1024;
    int ret = setsockopt(f, SOL_SOCKET, SO_RCVBUF, &x, sizeof(x));
    assert(ret == 0);
    ret = setsockopt(f, SOL_SOCKET, SO_SNDBUF, &x, sizeof(x));
    assert(ret == 0);
}

tamed void test_mpfd_client(struct sockaddr* saddr, socklen_t saddr_len) {
    tvars { tamer::fd cfd; msgpack_fd* mpfd; int ret; Json j[10];
        std::string fill((size_t) 8192, 'x'); }
    cfd = tamer::fd::socket(AF_INET, SOCK_STREAM, 0);
    twait { cfd.connect(saddr, saddr_len, make_event(ret)); }
    assert(ret == 0);
    small_socket_buffer(cfd.value());
    mpfd = new msgpack_fd(cfd);
    twait {
        for (int i = 0; i < 10; ++i)
            mpfd->call(Json::make_array(1, i, "Foo", fill), make_event(j[i]));
    }
    for (int i = 0; i < 10; ++i) {
        if (j[i].is_a())
            j[i].resize(3);
        std::cerr << j[i] << '\n';
    }
}

tamed void test_mpfd_server(tamer::fd serverfd) {
    tvars { tamer::fd cfd; msgpack_fd* mpfd; int ret; int n = 0; Json j; }
    twait { serverfd.accept(make_event(cfd)); }
    small_socket_buffer(cfd.value());
    mpfd = new msgpack_fd(cfd);
    while (cfd && n < 6) {
        twait { mpfd->read_request(make_event(j)); }
        if (n == 5) {
            std::cout << getpid() << " shutdown\n";
            shutdown(cfd.value(), SHUT_RD);
        }
        if (j && j.is_a())
            mpfd->write(Json::make_array(-j[0].as_i(), j[1], j[2].as_s() + " REPLY", j[3].as_s().substring(0, 4096)));
        ++n;
    }
    twait { mpfd->flush(make_event()); }
    twait { tamer::at_delay_sec(10, make_event()); }
    delete mpfd;
    cfd.close();
    serverfd.close();
    exit(0);
}
}

void test_mpfd() {
    tamer::fd listenfd = tamer::tcp_listen(0);
    assert(listenfd);
    struct sockaddr_in saddr;
    socklen_t saddr_len = sizeof(saddr);
    int r = getsockname(listenfd.value(), (struct sockaddr*) &saddr, &saddr_len);
    assert(r == 0);

    pid_t p = fork();
    if (p != 0) {
        listenfd.close();
        test_mpfd_client((struct sockaddr*) &saddr, saddr_len);
    } else
        test_mpfd_server(listenfd);
}


namespace {
tamed void test_mpfd2_client(tamer::fd rfd, tamer::fd wfd) {
    tvars { msgpack_fd* mpfd; int ret; Json j[10];
        std::string fill((size_t) 8192, 'x'); }
    mpfd = new msgpack_fd(rfd, wfd);
    twait {
        for (int i = 0; i < 10; ++i)
            mpfd->call(Json::make_array(1, i, "Foo", fill), make_event(j[i]));
    }
    for (int i = 0; i < 10; ++i) {
        if (j[i].is_a())
            j[i].resize(3);
        std::cerr << j[i] << '\n';
    }
}

tamed void test_mpfd2_server(tamer::fd rfd, tamer::fd wfd) {
    tvars { msgpack_fd* mpfd; int ret; int n = 0; Json j; }
    mpfd = new msgpack_fd(rfd, wfd);
    while ((rfd || wfd) && n < 6) {
        twait { mpfd->read_request(make_event(j)); }
        if (n == 5) {
            std::cout << getpid() << " shutdown\n";
            rfd.close();
        }
        if (j && j.is_a())
            mpfd->write(Json::make_array(-j[0].as_i(), j[1], j[2].as_s() + " REPLY", j[3].as_s().substring(0, 4096)));
        ++n;
    }
    twait { mpfd->flush(make_event()); }
    twait { tamer::at_delay_sec(10, make_event()); }
    delete mpfd;
    rfd.close();
    wfd.close();
    exit(0);
}
}

void test_mpfd2() {
    tamer::fd c2p[2], p2c[2];
    tamer::fd::pipe(c2p);
    tamer::fd::pipe(p2c);
    pid_t p = fork();
    if (p != 0)
        test_mpfd2_client(p2c[0], c2p[1]);
    else
        test_mpfd2_server(c2p[0], p2c[1]);
}
