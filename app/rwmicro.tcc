#include "rwmicro.hh"
#include <tamer/tamer.hh>
#include "redisadapter.hh"
#include "twittershim.hh"
#include "pqremoteclient.hh"

namespace pq {

typedef pq::TwitterHashShim<pq::RedisClient> redis_shim_type;

tamed void run_rwmicro_redis(Json& tp_param) {
    pq::RedisClient* client = new pq::RedisClient();
    redis_shim_type* shim = new redis_shim_type(*client);
    pq::RwMicro<redis_shim_type>* rw = new pq::RwMicro<redis_shim_type>(tp_param, *shim);

    client->connect();
    rw->safe_run();
}

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
    shim = new pq_shim_type(*rc, tp_param["writearound"].as_b(false));
    rw = new pq::RwMicro<pq_shim_type>(tp_param, *shim);
    rw->safe_run();
}

};
