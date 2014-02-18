#include "rwmicro.hh"
#include <tamer/tamer.hh>
#include "redisadapter.hh"
#include "memcacheadapter.hh"
#include "twittershim.hh"
#include "pqremoteclient.hh"

namespace pq {

#if HAVE_HIREDIS_HIREDIS_H
typedef pq::TwitterHashShim<pq::RedisClient> redis_shim_type;

tamed void run_rwmicro_redis(Json& tp_param) {
    pq::RedisClient* client = new pq::RedisClient();
    redis_shim_type* shim = new redis_shim_type(*client);
    pq::RwMicro<redis_shim_type>* rw = new pq::RwMicro<redis_shim_type>(tp_param, *shim);

    client->connect();
    rw->safe_run();
}
#else
void run_rwmicro_redis(Json&) {
    mandatory_assert(false && "Not configured for Redis!");
}
#endif

#if HAVE_MEMCACHED_PROTOCOL_BINARY_H
typedef pq::TwitterHashShim<pq::MemcacheClient> memcache_shim_type;

tamed void run_rwmicro_memcache(Json& tp_param) {
    tvars {
        pq::MemcacheClient* client = new pq::MemcacheClient();
        memcache_shim_type* shim = new memcache_shim_type(*client);
        pq::RwMicro<memcache_shim_type>* rw = new pq::RwMicro<memcache_shim_type>(tp_param, *shim);
    }

    twait { client->connect(make_event()); }
    rw->safe_run();
}
#else
void run_rwmicro_memcache(Json&) {
    mandatory_assert(false && "Not configured for Memcache!");
}
#endif

typedef pq::TwitterShim<pq::RemoteClient> pq_shim_type;

tamed void run_rwmicro_pqremote(Json& tp_param, int client_port) {
    tvars {
        tamer::fd fd;
        pq::RemoteClient* rc;
        pq_shim_type* shim;
        pq::RwMicro<pq_shim_type>* rw;
    }
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, client_port, make_event(fd)); }
    rc = new RemoteClient(fd);
    rc->set_wrlowat(1 << 11);
    shim = new pq_shim_type(*rc);
    rw = new pq::RwMicro<pq_shim_type>(tp_param, *shim);
    rw->safe_run();
}

};
