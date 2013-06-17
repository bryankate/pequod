#include <iostream>
#include "clp.h"
#include "string.hh"
#include "pqremoteclient.hh"
#include <tamer/tamer.hh>
#include "json.hh"
#include "hosts.hh"
#include "sock_helper.hh"

using std::cout;
using std::cerr;
using std::endl;
using namespace pq;

static Clp_Option options[] = {{ "daemonhosts", 'D', 1000, Clp_ValStringNotOption, 0 },
                               { "newhosts", 'H', 1001, Clp_ValStringNotOption, 0 },
                               { "stdoutredir", 'o', 1002, Clp_ValStringNotOption, 0 },
                               { "stderrredir", 'e', 1003, Clp_ValStringNotOption, 0 },
                               { "partfunc", 'P', 1004, Clp_ValStringNotOption, 0 }};


tamed void forkit(String dhostsfile, String nhostsfile, String outpath, String errpath, String part){
    tvars {
        tamer::fd fd;
        struct sockaddr_in sin;
        const Hosts *dhosts, *nhosts;
        const Host *curhost;
        RemoteClient *rclient;
        Json j;
        int32_t h;
    }

    dhosts = Hosts::get_instance(dhostsfile);
    nhosts = Hosts::get_instance(nhostsfile);
    assert(dhosts && nhosts);

    for (h = 0; h < dhosts->size(); ++h) {
        curhost = dhosts->get_by_seqid(h);
        cout << "Connecting to " << curhost->name() << ":" << curhost->port() << endl;

        sock_helper::make_sockaddr(curhost->name().c_str(), curhost->port(), sin);
        twait { tamer::tcp_connect(sin.sin_addr, curhost->port(), make_event(fd)); }
        if (!fd) {
            cerr << "fd FAIL: name:" << curhost->name() << " port:" << curhost->port() << endl;
            exit(-1);
        }

        rclient = new RemoteClient(fd);

        cout << "Trying to fork " << curhost->name() << ":" << curhost->port() << endl;
        twait {
            rclient->control(Json().set("spawn", Json().set("stdout", outpath + String(h) + ".txt")
                                                       .set("stderr", errpath + String(h) + ".txt")
                                                       .set("hostfile", nhostsfile)
                                                       .set("partfunc", part)
                                                       .set("nbacking", dhosts->size())
                                                       .set("port", nhosts->get_by_seqid(h)->port())),
                             make_event(j));
        }
        delete rclient;
    }
}


int main(int argc, char** argv) {
    String dhosts, nhosts, outpath, errpath, part;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);

    while (Clp_Next(clp) != Clp_Done) {
        if (clp->option->long_name == String("daemonhosts"))
            dhosts = clp->val.s;
        else if (clp->option->long_name == String("newhosts"))
            nhosts = clp->val.s;
        else if (clp->option->long_name == String("stdoutredir"))
            outpath = clp->val.s;
        else if (clp->option->long_name == String("stderrredir"))
            errpath = clp->val.s;
        else if (clp->option->long_name == String("partfunc"))
            part = clp->val.s;
        else
            assert(false && "Not a parsable option.");
    }

    tamer::initialize();
    forkit( dhosts, nhosts, outpath, errpath, part);
    tamer::loop();
    tamer::cleanup();

    cout << "Completed fork." << endl;

    return 0;
}
