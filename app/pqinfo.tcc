#include <iostream>
#include <tamer/tamer.hh>
#include "clp.h"
#include "string.hh"
#include "pqremoteclient.hh"
#include "json.hh"
#include "sock_helper.hh"

using std::cout;
using std::cerr;
using std::endl;
using namespace pq;

tamed void print_stats(const String& host, uint32_t port) {
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        RemoteClient *rclient;
        Json j, rj;
    }

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

    cout << rj.unparse(Json::indent_depth(4)) << endl;
    delete rclient;
}

static char envstr[] = "TAMER_NOLIBEVENT=1";
static Clp_Option options[] = {{ "host", 'h', 1000, Clp_ValStringNotOption, 0 },
                               { "port", 'p', 1001, Clp_ValInt, 0 }};

int main(int argc, char** argv) {
    putenv(envstr);
    tamer::initialize();

    String host = "localhost";
    uint32_t port = 9000;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);

    while (Clp_Next(clp) != Clp_Done) {
        if (clp->option->long_name == String("host"))
            host = clp->val.s;
        else if (clp->option->long_name == String("port"))
            port = clp->val.i;
        else
            assert(false && "Not a parsable option.");
    }

    print_stats(host, port);
    tamer::loop();
    tamer::cleanup();

    return 0;
}
