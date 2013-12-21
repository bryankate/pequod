// -*- mode: c++ -*-
#include "mpfd.hh"
#include "redisadapter.hh"
#include "memcacheadapter.hh"
#include "pqpersistent.hh"
#include "check.hh"
#include <fcntl.h>

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
tamed void one_test_mpfd2_client(msgpack_fd* mpfd, Json req, Json& reply,
                                 tamer::event<> done) {
    twait { mpfd->call(req, make_event(reply)); }
    if (reply.is_a())
        reply.resize(3);
    std::cerr << reply << '\n';
    done();
}

tamed void test_mpfd2_client(tamer::fd rfd, tamer::fd wfd) {
    tvars { msgpack_fd* mpfd; int ret; Json j[10];
        std::string fill((size_t) 8192, 'x'); }
#ifdef F_SETPIPE_SZ
    fcntl(rfd.value(), F_SETPIPE_SZ, (int) 4096);
    fcntl(wfd.value(), F_SETPIPE_SZ, (int) 4096);
#endif
    mpfd = new msgpack_fd(rfd, wfd);
    twait {
        for (int i = 0; i < 10; ++i)
            one_test_mpfd2_client(mpfd, Json::make_array(1, i, "Foo", fill), j[i], make_event());
    }
}

tamed void test_mpfd2_server(tamer::fd rfd, tamer::fd wfd) {
    tvars { msgpack_fd* mpfd; int ret; int n = 0; Json j; }
    mpfd = new msgpack_fd(rfd, wfd);
    while ((rfd || wfd) && n < 6) {
        twait { mpfd->read_request(make_event(j)); }
        if (j && j.is_a())
            mpfd->write(Json::make_array(-j[0].as_i(), j[1], j[2].as_s() + " REPLY", j[3].as_s().substring(0, 4096)));
        ++n;
    }
    twait { mpfd->flush(make_event()); }
    rfd.close();
    twait { tamer::at_delay_sec(10, make_event()); }
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

#if HAVE_HIREDIS_HIREDIS_H
tamed void test_redis() {
    tvars {
        pq::RedisClient client;
        String v;
        int32_t newlen = -1;
        std::vector<String> results;
    }
    client.connect();

    twait { client.set("hello", "world", make_event()); }
    twait { client.get("hello", make_event(v)); }
    CHECK_EQ(v, "world");

    twait { client.get("k2", make_event(v)); }
    CHECK_EQ(v, "");

    twait { client.append("k2", "abc", make_event()); }
    twait { client.length("k2", make_event(newlen)); }
    CHECK_EQ(newlen, 3);

    twait { client.append("k2", "def", make_event()); }
    twait { client.length("k2", make_event(newlen)); }
    CHECK_EQ(newlen, 6);

    twait { client.get("k2", make_event(v)); }
    CHECK_EQ(v, "abcdef");

    twait { client.getrange("k2", 1, -1, make_event(v)); }
    CHECK_EQ(v, "bcdef");

    twait { client.increment("k0", make_event()); }
    twait { client.get("k0", make_event(v)); }
    CHECK_EQ(v, "1");

    twait { client.increment("k0", make_event()); }
    twait { client.get("k0", make_event(v)); }
    CHECK_EQ(v, "2");

    twait { client.increment("k0", make_event()); }
    twait { client.get("k0", make_event(v)); }
    CHECK_EQ(v, "3");

    twait { client.sadd("s0", "b", make_event()); }
    twait { client.sadd("s0", "c", make_event()); }
    twait { client.sadd("s0", "a", make_event()); }
    twait { client.sadd("s0", "d", make_event()); }
    twait { client.smembers("s0", make_event(results)); }
    CHECK_EQ(results.size(), (uint32_t)4);

    results.clear();
    twait { client.zadd("ss0", "b", 1, make_event()); }
    twait { client.zadd("ss0", "c", 1, make_event()); }
    twait { client.zadd("ss0", "a", 1, make_event()); }
    twait { client.zadd("ss0", "d", 2, make_event()); }
    twait { client.zrangebyscore("ss0", 1, 2, make_event(results)); }
    CHECK_EQ(results.size(), (uint32_t)3);

    results.clear();
    twait { client.zrangebyscore("ss0", 1, 3, make_event(results)); }
    CHECK_EQ(results.size(), (uint32_t)4);
}
#else
void test_redis() {
    mandatory_assert(false && "Not configured for redis!");
}
#endif

#if HAVE_MEMCACHED_PROTOCOL_BINARY_H
tamed void test_memcache() {
    tvars {
        pq::MemcacheClient client;
        String v;
        int32_t newlen = -1;
        std::vector<String> results;
    }
    twait { client.connect(make_event()); }

    twait { client.set("hello", "world", make_event()); }
    twait { client.get("hello", make_event(v)); }
    CHECK_EQ(v, "world");

    twait { client.get("k2", make_event(v)); }
    CHECK_EQ(v, "");

    twait { client.append("k2", "abc", make_event()); }
    twait { client.length("k2", make_event(newlen)); }
    CHECK_EQ(newlen, 3);

    twait { client.append("k2", "def", make_event()); }
    twait { client.length("k2", make_event(newlen)); }
    CHECK_EQ(newlen, 6);

    twait { client.get("k2", make_event(v)); }
    CHECK_EQ(v, "abcdef");

    twait { client.get("k2", 1, make_event(v)); }
    CHECK_EQ(v, "bcdef");

    twait { client.increment("k0", make_event()); }
    twait { client.get("k0", make_event(v)); }
    CHECK_EQ(v, "1");

    twait { client.increment("k0", make_event()); }
    twait { client.get("k0", make_event(v)); }
    CHECK_EQ(v, "2");

    twait { client.increment("k0", make_event()); }
    twait { client.get("k0", make_event(v)); }
    CHECK_EQ(v, "3");
}
#else
void test_memcache() {
    mandatory_assert(false && "Not configured for memcache!");
}
#endif

#if HAVE_LIBPQ
tamed void test_postgres() {
    tvars {
        pq::DBPoolParams params;
        pq::PersistentStore* store;
        pq::PostgresStore* pg;
        pq::PersistentStore::ResultSet res;
        String s1 = "xxx";
        String s2 = "zzz";
        String s3;
    }

    params.dbname = "pqunit";
    params.host = "127.0.0.1";
    params.port = 5432;

    pg = new pq::PostgresStore(params);
    store = pg;

    // this will fail if postgres is not running or the pqunit db does not exist
    pg->connect();

    twait { store->put(s1, s2, make_event()); }
    twait { store->get(s1, make_event(s3)); }
    CHECK_EQ(s3, s2);

    twait {
        String keys[] = {"c","d","e","f","g","h","i","j","m","n"};
        for (int i = 0; i < 10; ++i)
            store->put(keys[i], keys[i], make_event());
    }

    twait { store->scan("b", "p", make_event(res)); }
    CHECK_EQ(res.size(), (uint32_t)10);
    res.clear();

    twait { store->scan("a", "c", make_event(res)); }
    CHECK_EQ(res.size(), (uint32_t)0);
    res.clear();

    twait { store->scan("c", "d0", make_event(res)); }
    CHECK_EQ(res.size(), (uint32_t)2);
    res.clear();

    twait { store->scan("c0", "g", make_event(res)); }
    CHECK_EQ(res.size(), (uint32_t)3);
    res.clear();

    twait { store->scan("j", "p0", make_event(res)); }
    CHECK_EQ(res.size(), (uint32_t)3);
    res.clear();

    delete store;
}
#else
void test_postgres() {
    mandatory_assert(false && "Not configured for postgres!");
}
#endif

