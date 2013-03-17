#ifndef PEQUOD_DATUM_HH
#define PEQUOD_DATUM_HH
#include <boost/intrusive/set.hpp>
#include "string.hh"
#include "str.hh"
#include "local_str.hh"

namespace pq {
class SinkRange;

typedef boost::intrusive::set_base_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>,
    boost::intrusive::optimize_size<true> > pequod_set_base_hook;
typedef boost::intrusive::set_member_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>,
    boost::intrusive::optimize_size<true> > pequod_set_member_hook;

class Datum : public pequod_set_base_hook {
  public:
    explicit inline Datum(Str key);
    inline Datum(Str key, const SinkRange* owner);
    inline Datum(Str key, const String& value);

    inline bool valid() const;
    inline void invalidate();

    inline const SinkRange* owner() const;

    inline void ref();
    inline void deref();

    inline Str key() const;
    inline const String& value() const;
    inline String& value();

    static const Datum empty_datum;

  private:
    LocalStr<24> key_;
    String value_;
    int refcount_;
    const SinkRange* owner_;
  public:
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

inline Datum::Datum(Str key, const SinkRange* owner)
    : key_{key}, refcount_{0}, owner_{owner} {
}

inline Datum::Datum(Str key, const String& value)
    : key_(key), value_(value), refcount_(0), owner_{nullptr} {
}

inline bool Datum::valid() const {
    return !key_.empty();
}

inline void Datum::invalidate() {
    key_ = Str();
    if (refcount_ == 0)
        delete this;
}

inline const SinkRange* Datum::owner() const {
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

} // namespace
#endif
