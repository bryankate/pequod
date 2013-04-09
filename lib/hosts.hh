#ifndef HOSTS_HH_
#define HOSTS_HH_

#include "compiler.hh"
#include "string.hh"
#include <vector>

namespace pq {

class Host {
  public:
    Host(const String &name, uint32_t port, uint32_t seqid);

    inline const String &name() const;
    inline uint32_t port() const;
    inline uint64_t uid() const;
    inline int seqid() const;

    String uid_string() const;

  private:

    String name_;
    uint32_t port_;
    uint64_t uid_;      // a unique identifier based on the hostname and port
    int seqid_;		// this host's index in the Hosts.all() vector
};


class Hosts {
  public:
    static Hosts *get_instance(const String &hostFile);

    const Host *get_by_uid(uint64_t uid) const;
    inline const Host *get_by_seqid(int seqid) const;
    inline const std::vector<Host> &all() const;
    inline int size() const;
    inline uint32_t count() const; // same as size(), deprecated

  private:
    std::vector<Host> hosts_;

    Hosts(const String &hostFile);
};


inline const String &Host::name() const {
    return name_;
}

inline uint32_t Host::port() const {
    return port_;
}

inline uint64_t Host::uid() const {
    return uid_;
}

inline int Host::seqid() const {
    return seqid_;
}

inline const Host *Hosts::get_by_seqid(int seqid) const {
    assert((unsigned) seqid < hosts_.size());
    return &hosts_[seqid];
}

inline const std::vector<Host> &Hosts::all() const {
    return hosts_;
}

inline int Hosts::size() const {
    return hosts_.size();
}

inline uint32_t Hosts::count() const {
    return hosts_.size();
}

}
#endif
