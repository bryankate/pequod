#include <iostream>
#include <tamer/tamer.hh>
#include "clp.h"
#include "string.hh"
#include "pqremoteclient.hh"
#include "memcacheadapter.hh"
#include "redisadapter.hh"
#include "json.hh"
#include "sock_helper.hh"

using std::cout;
using std::cerr;
using std::endl;
using namespace pq;

enum { mode_pequod = 0, mode_redis = 1, mode_memcached = 2 };
static const String value = String::make_fill('.', 256);

tamed void populate(const Json& params) {
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        RemoteClient* pclient = nullptr;
        MemcacheClient* mclient = nullptr;
        RedisClient* rclient = nullptr;
        Json j;
        char key[32];
        uint32_t ksz = params["prefix"].as_s().length() + 10;
        int32_t i;
        tamer::gather_rendezvous gr;
    }

    assert(ksz < 32);

    switch(params["mode"].as_i()) {
        case mode_pequod:
            sock_helper::make_sockaddr(params["host"].as_s().c_str(), params["port"].as_i(), sin);
            twait { tamer::tcp_connect(sin.sin_addr, params["port"].as_i(), make_event(fd)); }
            if (!fd) {
                cerr << "Could not connect to pequod server!" << endl;
                exit(-1);
            }

            pclient = new RemoteClient(fd);
            break;

        case mode_memcached:
            mclient = new MemcacheClient(params["host"].as_s(), params["port"].as_i());
            twait { mclient->connect(make_event()); }
            break;

        case mode_redis:
            rclient = new RedisClient(params["host"].as_s(), params["port"].as_i());
            rclient->connect();
            break;
    }

    for (i = params["minkey"].as_i(); i < params["maxkey"].as_i(); ++i) {
        sprintf(key, "%s%010u", params["prefix"].as_s().c_str(), i);

        switch(params["mode"].as_i()) {
            case mode_pequod:
                pclient->insert(Str(key, ksz), value, gr.make_event());
                twait { pclient->pace(make_event()); }
                break;

            case mode_memcached:
                mclient->set(Str(key, ksz), value, gr.make_event());
                twait { mclient->pace(make_event()); }
                break;

            case mode_redis:
                rclient->set(Str(key, ksz), value, gr.make_event());
                twait { rclient->pace(make_event()); }
                break;
        }
    }
    twait(gr);

    delete pclient;
    delete mclient;
    delete rclient;
}

static char envstr[] = "TAMER_NOLIBEVENT=1";
static Clp_Option options[] = {{ "host", 'h', 1000, Clp_ValStringNotOption, 0 },
                               { "port", 'p', 1001, Clp_ValInt, 0 },
                               { "minkey", 0, 1002, Clp_ValInt, 0 },
                               { "maxkey", 0, 1003, Clp_ValInt, 0 },
                               { "prefix", 0, 1004, Clp_ValStringNotOption, 0 },
                               { "memcached", 0, 1005, 0, Clp_Negate },
                               { "redis", 0, 1006, 0, Clp_Negate}};

int main(int argc, char** argv) {
    putenv(envstr);
    tamer::initialize();

    Json params = Json().set("host", "localhost")
                        .set("port", 9000)
                        .set("prefix", "m|")
                        .set("minkey", 0)
                        .set("maxkey", 1000000)
                        .set("mode", mode_pequod);
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);

    while (Clp_Next(clp) != Clp_Done) {
        if (clp->option->long_name == String("host"))
            params.set("host", clp->val.s);
        else if (clp->option->long_name == String("port"))
            params.set("port", clp->val.i);
        else if (clp->option->long_name == String("minkey"))
            params.set("minkey", clp->val.i);
        else if (clp->option->long_name == String("maxkey"))
            params.set("maxkey", clp->val.i);
        else if (clp->option->long_name == String("prefix"))
            params.set("prefix", clp->val.s);
        else if (clp->option->long_name == String("memcached"))
            params.set("mode", mode_memcached);
        else if (clp->option->long_name == String("redis"))
            params.set("mode", mode_redis);
        else
            assert(false && "Not a parsable option.");
    }

    populate(params);
    tamer::loop();
    tamer::cleanup();

    return 0;
}
