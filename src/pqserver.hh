#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH 1
#include <boost/intrusive/set.hpp>
#include "str.hh"
#include "string.hh"

namespace pq {
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
    inline bool operator()(const Datum& a, str b) const {
	return a.key() < b;
    }
    template <typename T>
    inline bool operator()(const String_base<T>& a, const Datum& b) const {
	return a < b.key();
    }
    inline bool operator()(str a, const Datum& b) const {
	return a < b.key();
    }
};

class Server {
  public:
    Server() {
    }

    template <typename I1, typename I2>
    void replace_range(I1 first_key, I1 last_key, I2 first_value);

  private:
    bi::set<Datum> store_;
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

} // namespace
#endif
