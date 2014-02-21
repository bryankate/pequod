#include <iostream>
#include <tamer/tamer.hh>
#include "clp.h"
#include "string.hh"
#include "hosts.hh"
#include "pqremoteclient.hh"
#include "json.hh"
#include "sock_helper.hh"

using std::cout;
using std::cerr;
using std::endl;
using namespace pq;

tamed void get_info(const String& host, uint32_t port, Json& result) {
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        RemoteClient *rclient;
        Json j, rj;
    }

    rj.set("host", host);
    rj.set("port", port);

    sock_helper::make_sockaddr(host.c_str(), port, sin);
    twait { tamer::tcp_connect(sin.sin_addr, port, make_event(fd)); }
    if (!fd) {
        cerr << "Could not connect to server (" << host << ":" << port << ")" << endl;
        exit(-1);
    }

    rclient = new RemoteClient(fd);
    
    twait { rclient->stats(make_event(j)); }
    rj.set("stats", j);

    twait { rclient->control(Json().set("client_status", true), make_event(j)); }
    rj.set("mpfd_status", j);

    result.push_back(rj);
    delete rclient;
}

static char envstr[] = "TAMER_NOLIBEVENT=1";
static Clp_Option options[] = {{ "host", 'h', 1000, Clp_ValStringNotOption, 0 },
                               { "port", 'p', 1001, Clp_ValInt, 0 },
                               { "hostfile", 'H', 1002, Clp_ValStringNotOption, 0 }};

int main(int argc, char** argv) {
    putenv(envstr);
    tamer::initialize();

    String host = "localhost";
    uint32_t port = 7000;
    String hostfile;
    const pq::Hosts* hosts = nullptr;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    Json result = Json::array();

    while (Clp_Next(clp) != Clp_Done) {
        if (clp->option->long_name == String("host"))
            host = clp->val.s;
        else if (clp->option->long_name == String("port"))
            port = clp->val.i;
        else if (clp->option->long_name == String("hostfile"))
            hostfile = clp->val.s;
        else
            assert(false && "Not a parsable option.");
    }

    if (hostfile) {
        hosts = Hosts::get_instance(hostfile);
        for (auto& h : hosts->all())
            get_info(h.name(), h.port(), result);
    }
    else
        get_info(host, port, result);

    tamer::loop();
    tamer::cleanup();

    if (result.size() == 1)
        result = result[0];

    cout << result.unparse(Json::indent_depth(4)) << endl;
    return 0;
}
