#ifndef PEQUOD_DATUM_HH
#define PEQUOD_DATUM_HH
#include <boost/intrusive/set.hpp>
#include "pqbase.hh"
#include "local_str.hh"

namespace pq {
class Sink;
class Table;

typedef boost::intrusive::set_base_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>,
    boost::intrusive::optimize_size<true> > pequod_set_base_hook;
typedef boost::intrusive::set_member_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>,
    boost::intrusive::optimize_size<true> > pequod_set_member_hook;

template <typename T> class KeyHook {
  public:
    inline const T& key_holder() const {
        return *static_cast<const T*>(this);
    }
};

class Datum : public pequod_set_base_hook, public KeyHook<Datum> {
  public:
    static const char table_marker[];

    explicit inline Datum(Str key);
    inline Datum(Str key, const Sink* owner);
    inline Datum(Str key, const String& value);

    inline bool is_table() const;
    inline const Table& table() const;
    inline Table& table();

    inline bool valid() const;
    inline void invalidate();

    inline const Sink* owner() const;

    inline void ref();
    inline void deref();

    typedef Str key_type;
    inline key_type key() const;
    inline const String& value() const;
    inline String& value();

    static const Datum empty_datum;
    static const Datum max_datum;

  private:
    LocalStr<24> key_;
    String value_;
    int refcount_;
    int owner_position_;
    const Sink* owner_;
  public:
    pequod_set_member_hook member_hook_;

    friend class Sink;
};

struct KeyCompare {
    template <typename K, typename T>
    inline bool operator()(const KeyHook<K>& a, const String_base<T>& b) const {
	return a.key_holder().key() < b;
    }
    template <typename K>
    inline bool operator()(const KeyHook<K>& a, Str b) const {
	return a.key_holder().key() < b;
    }
    template <typename K, typename T>
    inline bool operator()(const String_base<T>& a, const KeyHook<K>& b) const {
	return a < b.key_holder().key();
    }
    template <typename K>
    inline bool operator()(Str a, const KeyHook<K>& b) const {
	return a < b.key_holder().key();
    }
};

struct DatumDispose {
    inline void operator()(Datum* ptr) {
	delete ptr;
    }
};

typedef boost::intrusive::set<Datum> ServerStore;


inline bool operator<(const Datum& a, const Datum& b) {
    return a.key() < b.key();
}
inline bool operator==(const Datum& a, const Datum& b) {
    return a.key() == b.key();
}
inline bool operator>(const Datum& a, const Datum& b) {
    return a.key() > b.key();
}

inline Datum::Datum(Str key)
    : key_(key), refcount_(0), owner_{nullptr} {
}

inline Datum::Datum(Str key, const Sink* owner)
    : key_{key}, refcount_{0}, owner_{owner} {
}

inline Datum::Datum(Str key, const String& value)
    : key_(key), value_(value), refcount_(0), owner_{nullptr} {
}

inline bool Datum::is_table() const {
    return value_.data() == table_marker;
}

inline bool Datum::valid() const {
    return !is_invalidate_marker(value_);
}

inline void Datum::invalidate() {
    value_ = invalidate_marker();
    if (refcount_ == 0)
        delete this;
}

inline const Sink* Datum::owner() const {
    return owner_;
}

inline void Datum::ref() {
    ++refcount_;
}

inline void Datum::deref() {
    if (--refcount_ == 0 && !valid())
        delete this;
}

inline Str Datum::key() const {
    return key_;
}

inline const String& Datum::value() const {
    return value_;
}

inline String& Datum::value() {
    return value_;
}

inline std::ostream& operator<<(std::ostream& stream, const Datum& d) {
    return stream << d.key() << "=" << (d.valid() ? d.value() : String("INVALID"));
}

} // namespace
#endif
