#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH 1
#include <boost/intrusive/set.hpp>
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "interval_tree.hh"

namespace pq {
class JoinState;
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

class ServerRange : public interval<String> {
  public:
    enum range_type { copy = 1, sink = 2 };
    ServerRange(const String& first, const String& last, range_type type,
		Join* join, const Match& m);

    inline range_type type() const;
    inline const String& subtree_iend() const;
    inline void set_subtree_iend(const String& subtree_iend);

    enum notify_type {
	notify_erase = -1, notify_update = 0, notify_insert = 1
    };
    void notify(const Datum* d, int notifier, Server& server) const;

    friend std::ostream& operator<<(std::ostream&, const ServerRange&);

    rblinks<ServerRange> rblinks_;
  private:
    range_type type_;
    String subtree_iend_;
    Join* join_;
    mutable String resultkey_;
};

class ServerRangeSet {
  public:
    inline ServerRangeSet(Server* store, Str first, Str last, int types);
    inline ServerRangeSet(Server* store, int types);

    void push_back(ServerRange* r);

    inline void notify(const Datum* d, int notifier);

    friend std::ostream& operator<<(std::ostream&, const ServerRangeSet&);

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
};

class ServerRangeCollector {
  public:
    inline ServerRangeCollector(ServerRangeSet& srs);
    inline void operator()(ServerRange& r);
  private:
    ServerRangeSet* srs_;
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

    void insert(const String& key, const String& value);
    void erase(const String& key);

    template <typename I1, typename I2>
    void replace_range(I1 first_key, I1 last_key, I2 first_value);

    void process_join(const JoinState* js, Str first, Str last);

    inline void add_copy(Str first, Str last, Join* j, const Match& m);

  private:
    store_type store_;
    interval_tree<ServerRange> ranges_;
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

inline const String& ServerRange::subtree_iend() const {
    return subtree_iend_;
}

inline void ServerRange::set_subtree_iend(const String& subtree_iend) {
    subtree_iend_ = subtree_iend;
}

inline ServerRange::range_type ServerRange::type() const {
    return type_;
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

inline ServerRangeCollector::ServerRangeCollector(ServerRangeSet& srs)
    : srs_(&srs) {
}

inline void ServerRangeCollector::operator()(ServerRange& r) {
    srs_->push_back(&r);
}

inline void Server::add_copy(Str first, Str last, Join* join,
			     const Match& m) {
    ServerRange* r = new ServerRange(first, last, ServerRange::copy, join, m);
    ranges_.insert(r);
}

template <typename I1, typename I2>
void Server::replace_range(I1 first_key, I1 last_key, I2 first_value) {
#if 0
    auto it = store_.bounded_range(*first_key, *last_key, DatumCompare(),
				   true, false);
#else
    auto it = std::make_pair(store_.lower_bound(*first_key, DatumCompare()),
			     store_.lower_bound(*last_key, DatumCompare()));
#endif
    ServerRangeSet srs(this, *first_key, *last_key, ServerRange::copy);
    ranges_.visit_overlaps(interval<Str>(*first_key, *last_key),
			   ServerRangeCollector(srs));

    while (first_key != last_key && it.first != it.second) {
	int cmp = it.first->key().compare(*first_key);
	Datum* d;
	if (cmp > 0) {
	    d = new Datum(*first_key, *first_value);
	    (void) store_.insert(it.first, *d);
	} else if (cmp == 0) {
	    d = it.first.operator->();
	    d->value_ = *first_value;
	    ++it.first;
	} else {
	    d = it.first.operator->();
	    it.first = store_.erase(it.first);
	}
	srs.notify(d, cmp);
	if (cmp >= 0)
	    ++first_key, ++first_value;
	if (cmp < 0)
	    delete d;
    }
    while (first_key != last_key) {
	Datum* d = new Datum(*first_key, *first_value);
	(void) store_.insert(it.first, *d);
	srs.notify(d, 1);
	++first_key, ++first_value;
    }
    while (it.first != it.second) {
	Datum* d = it.first.operator->();
	it.first = store_.erase(it.first);
	srs.notify(d, -1);
	delete d;
    }
}

} // namespace
#endif
