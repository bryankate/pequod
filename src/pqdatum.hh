#ifndef PEQUOD_DATUM_HH
#define PEQUOD_DATUM_HH
#include <boost/intrusive/set.hpp>
#include "string.hh"
#include "str.hh"
#include "local_str.hh"

namespace pq {
class ValidJoinRange;

typedef boost::intrusive::set_base_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>,
    boost::intrusive::optimize_size<true> > pequod_set_base_hook;
typedef boost::intrusive::set_member_hook<
    boost::intrusive::link_mode<boost::intrusive::normal_link>,
    boost::intrusive::optimize_size<true> > pequod_set_member_hook;

class Datum : public pequod_set_base_hook {
  public:
    explicit Datum(Str key)
        : key_(key), refcount_(0), owner_{nullptr} {
    }
    Datum(Str key, const ValidJoinRange* owner)
        : key_{key}, refcount_{0}, owner_{owner} {
    }
    Datum(Str key, const String& value)
	: key_(key), value_(value), refcount_(0), owner_{nullptr} {
    }

    bool valid() const {
        return !key_.empty();
    }
    void invalidate() {
        key_ = Str();
        if (refcount_ == 0)
            delete this;
    }

    const ValidJoinRange* owner() const {
        return owner_;
    }

    void ref() {
        ++refcount_;
    }
    void deref() {
        if (--refcount_ == 0 && !valid())
            delete this;
    }

    Str key() const {
	return key_;
    }
    const String& value() const {
        return value_;
    }
    String& value() {
        return value_;
    }

  private:
    LocalStr<24> key_;
    String value_;
    int refcount_;
    const ValidJoinRange* owner_;
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

} // namespace
#endif
