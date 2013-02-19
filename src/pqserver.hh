#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH 1
#include <boost/intrusive/set.hpp>
#include "str.hh"
#include "string.hh"
#include "interval.hh"

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

class ServerRange : public interval<String> {
  public:
    enum range_type { source };
    ServerRange(const String& first, const String& last,
		Join* join, const Match& m);

    inline const String& subtree_iend() const;
    inline void set_subtree_iend(const String& subtree_iend);

    void insert(const Datum& d, Server& server) const;
    void erase(const Datum& d, Server& server) const;

  private:
    String subtree_iend_;
    Join* join_;
    mutable String resultkey_;
};

class Server {
    typedef bi::set<Datum> store_type;

  public:
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

  private:
    store_type store_;
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

template <typename I1, typename I2>
void Server::replace_range(I1 first_key, I1 last_key, I2 first_value) {
#if 0
    auto it = store_.bounded_range(*first_key, *last_key, DatumCompare(),
				   true, false);
#else
    auto it = std::make_pair(store_.lower_bound(*first_key, DatumCompare()),
			     store_.lower_bound(*last_key, DatumCompare()));
#endif
    while (first_key != last_key && it.first != it.second) {
	int cmp = it.first->key().compare(*first_key);
	if (cmp > 0) {
	    Datum* d = new Datum(*first_key, *first_value);
	    (void) store_.insert(it.first, *d);
	    ++first_key, ++first_value;
	} else if (cmp == 0) {
	    it.first->value_ = *first_value;
	    ++first_key, ++first_value;
	} else {
	    delete &*it.first;
	    ++it.first;
	}
    }
    while (first_key != last_key) {
	Datum* d = new Datum(*first_key, *first_value);
	(void) store_.insert(it.first, *d);
	++first_key, ++first_value;
    }
    while (it.first != it.second) {
	delete &*it.first;
	++it.first;
    }
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

} // namespace
#endif
