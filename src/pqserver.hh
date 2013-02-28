#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH 1
#include <boost/intrusive/set.hpp>
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "interval_tree.hh"
#include "local_vector.hh"
class Json;

namespace pq {
class Join;
class Match;
class Server;

namespace bi = boost::intrusive;
typedef bi::set_base_hook<bi::link_mode<bi::normal_link>,
			  bi::optimize_size<true> > pequod_set_base_hook;
typedef bi::set_member_hook<bi::link_mode<bi::normal_link>,
			    bi::optimize_size<true> > pequod_set_member_hook;

class Datum : public pequod_set_base_hook {
  public:
    explicit Datum(const String& key)
	: key_(key) {
    }
    Datum(const String& key, const String& value)
	: key_(key), value_(value) {
    }

    const String& key() const {
	return key_;
    }
    const String& value() const {
        return value_;
    }

  private:
    String key_;
  public:
    String value_;
    pequod_set_member_hook member_hook_;
};

struct DatumCompare {
    template <typename T>
    inline bool operator()(const Datum& a, const String_base<T>& b) const {
	return a.key() < b;
    }
    inline bool operator()(const Datum& a, Str b) const {
	return a.key() < b;
    }
    template <typename T>
    inline bool operator()(const String_base<T>& a, const Datum& b) const {
	return a < b.key();
    }
    inline bool operator()(Str a, const Datum& b) const {
	return a < b.key();
    }
};

struct DatumDispose {
    inline void operator()(Datum* ptr) {
	delete ptr;
    }
};

typedef bi::set<Datum> ServerStore;

class ServerRange {
  public:
    enum range_type {
        copy = 1, joinsink = 2, validjoin = 4, joinsource = 8
    };
    static ServerRange* make(Str first, Str last, range_type type, Join *join = 0);

    typedef Str endpoint_type;
    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;
    inline Str subtree_iend() const;
    inline void set_subtree_iend(Str subtree_iend);

    inline range_type type() const;
    inline Join* join() const;
    inline bool expired_at(uint64_t t) const;

    void add_sink(const Match& m);

    enum notify_type {
	notify_erase = -1, notify_update = 0, notify_insert = 1
    };
    void notify(const Datum* d, int notifier, Server& server) const;
    void validate(Str first, Str last, Server& server);

    friend std::ostream& operator<<(std::ostream&, const ServerRange&);

  private:
    int ibegin_len_;
    int iend_len_;
    Str subtree_iend_;
  public:
    rblinks<ServerRange> rblinks_;
  private:
    range_type type_;
    Join* join_;
    uint64_t expires_at_;
    mutable local_vector<String, 4> resultkeys_;
    char keys_[0];

    inline ServerRange(Str first, Str last, range_type type, Join* join);
    ~ServerRange() = default;
    void validate(Match& mf, Match& ml, int joinpos, Server& server);
};

class ServerRangeSet {
  public:
    inline ServerRangeSet(Server* store, Str first, Str last, int types);
    inline ServerRangeSet(Server* store, int types);

    void push_back(ServerRange* r);

    inline void notify(const Datum* d, int notifier);
    void validate();

    friend std::ostream& operator<<(std::ostream&, const ServerRangeSet&);
    inline int total_size() const;

  private:
    enum { rangecap = 5 };
    int nr_;
    int sw_;
    ServerRange* r_[rangecap];
    Server* server_;
    Str first_;
    Str last_;
    int types_;

    void hard_visit(const Datum* datum);
    void validate_join(ServerRange* jr, int ts);
};

class Server {
  public:
    typedef ServerStore store_type;

    Server() {
    }

    typedef store_type::const_iterator const_iterator;
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const_iterator find(Str str) const;
    inline const_iterator lower_bound(Str str) const;
    inline size_t count(Str first, Str last) const;

    void insert(const String& key, const String& value, bool notify);
    void erase(const String& key, bool notify);

    template <typename I>
    void replace_range(Str first, Str last, I first_value, I last_value);

    void add_copy(Str first, Str last, Join* j, const Match& m);
    void add_join(Str first, Str last, Join* j);
    inline void add_validjoin(Str first, Str last, Join* j);

    inline void validate(Str first, Str last);
    inline size_t validate_count(Str first, Str last);

    Json stats() const;
    void print(std::ostream& stream);

  private:
    store_type store_;
    interval_tree<ServerRange> source_ranges_;
    interval_tree<ServerRange> sink_ranges_;
    interval_tree<ServerRange> join_ranges_;
};

inline bool operator<(const Datum& a, const Datum& b) {
    return a.key() < b.key();
}
template <typename T>
inline bool operator<(const Datum& a, const String_base<T>& b) {
    return a.key() < b;
}
template <typename T>
inline bool operator<(const String_base<T>& a, const Datum& b) {
    return a < b.key();
}

inline bool operator==(const Datum& a, const Datum& b) {
    return a.key() == b.key();
}
template <typename T>
inline bool operator==(const Datum& a, const String_base<T>& b) {
    return a.key() == b;
}
template <typename T>
inline bool operator==(const String_base<T>& a, const Datum& b) {
    return a == b.key();
}

inline typename Server::const_iterator Server::begin() const {
    return store_.begin();
}

inline typename Server::const_iterator Server::end() const {
    return store_.end();
}

inline typename Server::const_iterator Server::find(Str str) const {
    return store_.find(str, DatumCompare());
}

inline typename Server::const_iterator Server::lower_bound(Str str) const {
    return store_.lower_bound(str, DatumCompare());
}

inline Str ServerRange::ibegin() const {
    return Str(keys_, ibegin_len_);
}

inline Str ServerRange::iend() const {
    return Str(keys_ + ibegin_len_, iend_len_);
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

inline ServerRangeSet::ServerRangeSet(Server* server, Str first, Str last,
				      int types)
    : nr_(0), sw_(0), server_(server), first_(first), last_(last),
      types_(types) {
}

inline ServerRangeSet::ServerRangeSet(Server* server, int types)
    : nr_(0), sw_(0), server_(server), types_(types) {
}

inline void ServerRangeSet::notify(const Datum* datum, int notifier) {
    if (sw_ != 0)
	hard_visit(datum);
    for (int i = 0; i != nr_; ++i)
	if (r_[i])
	    r_[i]->notify(datum, notifier, *server_);
}

inline int ServerRangeSet::total_size() const {
    return sw_ ? 8 * sizeof(sw_) + 1 - ffs_msb((unsigned) sw_) : nr_;
}

inline void Server::add_validjoin(Str first, Str last, Join* join) {
    ServerRange* r = ServerRange::make(first, last, ServerRange::validjoin, join);
    sink_ranges_.insert(r);
}

template <typename I>
void Server::replace_range(Str first, Str last, I first_value, I last_value) {
#if 0
    auto it = store_.bounded_range(first, last, DatumCompare(), true, false);
#else
    auto it = std::make_pair(store_.lower_bound(first, DatumCompare()),
			     store_.lower_bound(last, DatumCompare()));
#endif
    ServerRangeSet srs(this, first, last, ServerRange::copy);
    for (auto it = source_ranges_.begin_overlaps(interval<Str>(first, last));
	 it != source_ranges_.end(); ++it)
	srs.push_back(it.operator->());

    while (first_value != last_value && it.first != it.second) {
	int cmp = it.first->key().compare(first_value->first);
	Datum* d;
	if (cmp > 0) {
	    d = new Datum(first_value->first, first_value->second);
	    (void) store_.insert(it.first, *d);
	} else if (cmp == 0) {
	    d = it.first.operator->();
	    d->value_ = first_value->second;
	    ++it.first;
	} else {
	    d = it.first.operator->();
	    it.first = store_.erase(it.first);
	}
	srs.notify(d, cmp);
	if (cmp >= 0)
	    ++first_value;
	if (cmp < 0)
	    delete d;
    }
    while (first_value != last_value) {
	Datum* d = new Datum(first_value->first, first_value->second);
	(void) store_.insert(it.first, *d);
	srs.notify(d, 1);
	++first_value;
    }
    while (it.first != it.second) {
	Datum* d = it.first.operator->();
	it.first = store_.erase(it.first);
	srs.notify(d, -1);
	delete d;
    }
}

inline void Server::validate(Str first, Str last) {
    ServerRangeSet srs(this, first, last,
                       ServerRange::joinsink | ServerRange::validjoin);
    for (auto it = sink_ranges_.begin_overlaps(interval<Str>(first, last));
	 it != sink_ranges_.end(); ++it)
	srs.push_back(it.operator->());
    srs.validate();
}

inline size_t Server::count(Str first, Str last) const {
    return std::distance(lower_bound(first), lower_bound(last));
}

inline size_t Server::validate_count(Str first, Str last) {
    validate(first, last);
    return count(first, last);
}

} // namespace
#endif
