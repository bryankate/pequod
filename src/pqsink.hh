#ifndef PEQUOD_SINK_HH
#define PEQUOD_SINK_HH
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "local_vector.hh"
#include "rb.hh"
namespace pq {
class Join;
class Server;
class Match;
class SourceAccumulator;

class ServerRange {
  public:
    enum range_type {
        joinsink = 1, validjoin = 2
    };
    ServerRange(Str first, Str last, range_type type, Join *join = 0);
    virtual ~ServerRange();

    typedef Str endpoint_type;
    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;
    inline Str subtree_iend() const;
    inline void set_subtree_iend(Str subtree_iend);

    inline range_type type() const;
    inline Join* join() const;
    inline bool expired_at(uint64_t t) const;

    void validate(Str first, Str last, Server& server);

    friend std::ostream& operator<<(std::ostream&, const ServerRange&);

    static uint64_t allocated_key_bytes;

  private:
    Str ibegin_;
    Str iend_;
    Str subtree_iend_;
  public:
    rblinks<ServerRange> rblinks_;
  private:
    range_type type_;
    Join* join_;
    uint64_t expires_at_;
    char buf_[32];

    void validate(Match& mf, Match& ml, int joinpos, Server& server,
                  SourceAccumulator* accum);
};

class ValidJoinRange : public ServerRange {
  public:
    inline ValidJoinRange(Str first, Str last, Join *join);

    bool valid() const {return true;}
    void deref() {}
};

class ServerRangeSet {
  public:
    inline ServerRangeSet(Str first, Str last, int types);

    inline void push_back(ServerRange* r);

    void validate(Server& server);

    friend std::ostream& operator<<(std::ostream&, const ServerRangeSet&);
    inline int total_size() const;

  private:
    local_vector<ServerRange*, 5> r_;
    Str first_;
    Str last_;
    int types_;

    void validate_join(ServerRange* jr, Server& server);
};

inline Str ServerRange::ibegin() const {
    return ibegin_;
}

inline Str ServerRange::iend() const {
    return iend_;
}

inline interval<Str> ServerRange::interval() const {
    return make_interval(ibegin(), iend());
}

inline Str ServerRange::subtree_iend() const {
    return subtree_iend_;
}

inline void ServerRange::set_subtree_iend(Str subtree_iend) {
    subtree_iend_ = subtree_iend;
}

inline ServerRange::range_type ServerRange::type() const {
    return type_;
}

inline Join* ServerRange::join() const {
    return join_;
}

inline bool ServerRange::expired_at(uint64_t t) const {
    return expires_at_ && (expires_at_ < t);
}

inline ValidJoinRange::ValidJoinRange(Str first, Str last, Join* join)
    : ServerRange(first, last, validjoin, join) {
}

inline ServerRangeSet::ServerRangeSet(Str first, Str last, int types)
    : first_(first), last_(last), types_(types) {
}

inline void ServerRangeSet::push_back(ServerRange* r) {
    if (r->type() & types_)
        r_.push_back(r);
}

inline int ServerRangeSet::total_size() const {
    return r_.size();
}

}
#endif
