#include "pqrwmicro.hh"
#include <tamer/tamer.hh>
#include "hashclient.hh"

namespace pq {

typedef pq::TwitterHashShim<pq::RedisfdHashClient> shim_type;

tamed void run_rwmicro_redisfd(Json& tp_param) {
    tvars {
        pq::RedisfdHashClient* client;
        tamer::fd fd;
        shim_type* shim;
        pq::RwMicro<shim_type> *rw;
    }
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, 6379, make_event(fd)); }
    client = new pq::RedisfdHashClient(fd);
    shim = new shim_type(*client);
    rw = new pq::RwMicro<shim_type>(tp_param, *shim);
    rw->safe_run();
}

};
