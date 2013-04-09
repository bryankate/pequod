#include "hosts.hh"
#include "sock_helper.hh"
#include <assert.h>
#include <fstream>
#include <sstream>
#include <string>
#include <boost/algorithm/string.hpp>

using std::string;
using std::vector;
using std::ifstream;
using std::istringstream;

namespace pq {

Host::Host(const String &name, uint32_t port, uint32_t seqid)
    : name_(name), port_(port), uid_(sock_helper::get_uid(name.c_str(), port)), seqid_(seqid) {
}

String Host::uid_string() const {
    return name_ + ":" + String(port_);
}


Hosts::Hosts(const String &hostFile) {

    ifstream infile(hostFile.c_str(), ifstream::in);
    string line;
    uint32_t seq = 0;

    mandatory_assert(infile && "Host file cannot be opened.");
    while(infile.good()) {

        if (!getline(infile, line)) {
            break;
        }

        boost::trim(line);

        if ((line.empty()) || (line[0] == '#')) {
            continue;
        }

        string name;
        uint32_t port;
        istringstream iss(line);

        iss >> name >> port;
        hosts_.push_back(Host(name.c_str(), port, seq++));
    }
}

const Host *Hosts::get_by_uid(uint64_t uid) const {

    for (auto h = hosts_.begin(); h != hosts_.end(); ++h) {
        if (h->uid() == uid) {
            return &hosts_[h->seqid()];
        }
    }

    assert(false && "Could not find host for given uid.");
    return NULL;
}

Hosts *Hosts::get_instance(const String &hostFile) {
    return new Hosts(hostFile);
}

}
