#ifndef LOCAL_STRING_HH
#define LOCAL_STRING_HH
#include "string_base.hh"
#if SIZEOF_VOID_P != 8 && SIZEOF_VOID_P != 4
# error "unknown SIZEOF_VOID_P"
#endif

class LocalString : public String_base<LocalString> {
  public:
    inline LocalString();
    inline LocalString(const LocalString& x);
    inline LocalString(LocalString&& x);
    inline LocalString(const String& x);
    inline LocalString(String&& x);
    template <typename T>
    inline LocalString(const String_base<T>& x);
    inline ~LocalString();

    inline const char* data() const;
    inline int length() const;

    inline LocalString& operator=(const LocalString& x);
    inline LocalString& operator=(LocalString&& x);
    inline LocalString& operator=(const String& x);
    inline LocalString& operator=(String&& x);
    template <typename T>
    inline LocalString& operator=(const String_base<T>& x);

    inline void swap(LocalString& x);

  private:
    enum {
#if SIZEOF_VOID_P == 8
	local_capacity = 23
#else
	local_capacity = 15
#endif
    };

    struct remote_rep_type __attribute__((packed)) {
	String::rep_type rem;
	char padding[3];
	char remote_tag;	// should be at same position as local_rep_type::tag
				// but not strictly required
    };

    struct local_rep_type {
	char data[local_capacity];
	char tag;
    };

    union rep_type {
	remote_rep_type rem;
	local_rep_type loc;
    };

    rep_type r_;

};

inline LocalString::LocalString() {
    static_assert(sizeof(remote_rep_type) == sizeof(local_rep_type),
		  "odd String::rep_type size");
    r_.loc.tag = 1;
}

inline LocalString::LocalString(const LocalString& x)
    : r_(x.r_) {
    if (!r_.loc.tag)
	r_.rem.str.ref();
}

inline LocalString::LocalString(LocalString&& x) {
    using std::swap;
    r_.loc.tag = 1;
    swap(r_, x.r_);
}

inline LocalString::LocalString(const String& x) {
    if (x.length() <= local_capacity) {
	memcpy(r_.loc.data, x.data(), x.length());
	r_.loc.tag = x.length() + 1;
    } else {
	r_.rem.str = x.internal_rep();
	r_.rem.str.ref();
	r_.loc.tag = 0;
    }
}

inline LocalString::LocalString(String&& x) {
    if (x.length() <= local_capacity) {
	memcpy(r_.loc.data, x.data(), x.length());
	r_.loc.tag = x.length() + 1;
    } else {
	r_.rem.str.memo = 0;
	x.swap(r_.rem.str);
	r_.loc.tag = 0;
    }
}

template <typename T>
inline LocalString::LocalString(const String_base<T>& x) {
    if (x.length() <= local_capacity) {
	memcpy(r_.loc.data, x.data(), x.length());
	r_.loc.tag = x.length() + 1;
    } else {
	String xx(x);
	r_.rem.str.memo = 0;
	xx.swap(r_.rem.str);
	r_.loc.tag = 0;
    }
}

inline LocalString::~LocalString() {
    if (!r_.loc.tag)
	r_.rem.str.deref();
}

inline const char* LocalString::data() const {
    return r_.loc.tag ? r_.loc.data : r_.rem.str.data;
}

inline int LocalString::length() const {
    return r_.loc.tag ? r_.loc.tag - 1 : r_.rem.str.length;
}

inline LocalString& LocalString::operator=(const LocalString& x) {
    if (!x.r_.loc.tag)
	x.r_.rem.str.ref();
    if (!r_.loc.tag)
	r_.rem.str.deref();
    r_ = x.r_;
    return *this;
}

inline LocalString& LocalString::operator=(LocalString&& x) {
    swap(x);
    return *this;
}

inline LocalString& LocalString::operator=(const String& x) {
    if (!r_.loc.tag)
	r_.rem.str.deref();
    if (x.length() <= local_capacity) {
	memcpy(r_.loc.data, x.data(), x.length());
	r_.loc.tag = x.length() + 1;
    } else {
	r_.rem.str = x.internal_rep();
	r_.rem.str.ref();
	r_.loc.tag = 0;
    }
    return *this;
}

inline LocalString& LocalString::operator=(String&& x) {
    if (!r_.loc.tag)
	r_.rem.str.deref();
    if (x.length() <= local_capacity) {
	memcpy(r_.loc.data, x.data(), x.length());
	r_.loc.tag = x.length() + 1;
    } else {
	r_.rem.str.memo = 0;
	x.swap(r_.rem.str);
	r_.loc.tag = 0;
    }
    return *this;
}

template <typename T>
inline LocalString& LocalString::operator=(const String_base<T>& x) {
    if (!r_.loc.tag)
	r_.rem.str.deref();
    if (x.length() <= local_capacity) {
	memmove(r_.loc.data, x.data(), x.length());
	r_.loc.tag = x.length() + 1;
    } else {
	String xx(x);
	r_.rem.str.memo = 0;
	xx.swap(r_.rem.str);
	r_.loc.tag = 0;
    }
    return *this;
}

inline void LocalString::swap(LocalString& x) {
    using std::swap;
    swap(r_, x.r_);
}

#endif
