#ifndef PEQUOD_LOCAL_STR_HH
#define PEQUOD_LOCAL_STR_HH
#include "string_base.hh"

template <int C = 20>
class LocalStr : public String_base<LocalStr<C> > {
  public:
    enum { local_capacity = C };

    inline LocalStr();
    inline LocalStr(const LocalStr<C>& x);
    inline LocalStr(LocalStr<C>&& x);
    template <typename T>
    inline LocalStr(const String_base<T>& x);
    inline LocalStr(const char* cstr);
    inline ~LocalStr();

    inline const char* data() const;
    inline int length() const;

    inline bool is_local() const;

    inline LocalStr<C>& operator=(const LocalStr<C>& x);
    inline LocalStr<C>& operator=(LocalStr<C>&& x);
    template <typename T>
    inline LocalStr<C>& operator=(const String_base<T>& x);

  private:
    int length_;
    union {
        char* rem;
        char loc[local_capacity];
    } u_;

    inline void initialize(const char* data, int length);
    inline void uninitialize();
};

template <int C>
inline void LocalStr<C>::initialize(const char* data, int length) {
    length_ = length;
    if (length > local_capacity) {
        u_.rem = new char[length];
        memcpy(u_.rem, data, length);
    } else
        memcpy(u_.loc, data, length);
}

template <int C>
inline void LocalStr<C>::uninitialize() {
    if (length_ > local_capacity)
        delete[] u_.rem;
}

template <int C>
inline LocalStr<C>::LocalStr()
    : length_(0) {
}

template <int C>
inline LocalStr<C>::LocalStr(const LocalStr<C>& x) {
    initialize(x.data(), x.length());
}

template <int C>
inline LocalStr<C>::LocalStr(LocalStr<C>&& x)
    : length_(x.length_) {
    if (length_ > local_capacity) {
        u_.rem = x.u_.rem;
        x.length_ = 0;
    } else
        memcpy(u_.loc, x.u_.loc, length_);
}

template <int C> template <typename T>
inline LocalStr<C>::LocalStr(const String_base<T>& x) {
    initialize(x.data(), x.length());
}

template <int C>
inline LocalStr<C>::LocalStr(const char* cstr) {
    initialize(cstr, strlen(cstr));
}

template <int C>
inline LocalStr<C>::~LocalStr() {
    uninitialize();
}

template <int C>
inline const char* LocalStr<C>::data() const {
    return length_ > local_capacity ? u_.rem : u_.loc;
}

template <int C>
inline int LocalStr<C>::length() const {
    return length_;
}

template <int C>
inline bool LocalStr<C>::is_local() const {
    return length() <= local_capacity;
}

template <int C>
inline LocalStr<C>& LocalStr<C>::operator=(const LocalStr<C>& x) {
    if (&x != this) {
        uninitialize();
        initialize(x.data(), x.length());
    }
    delete this;
}

template <int C>
inline LocalStr<C>& LocalStr<C>::operator=(LocalStr<C>&& x) {
    using std::swap;
    swap(length_, x.length_);
    char* old_rem = u_.rem;
    if (length_ > local_capacity)
        u_.rem = x.u_.rem;
    else
        memcpy(u_.loc, x.u_.loc, length_);
    if (x.length_ > local_capacity)
        x.u_.rem = old_rem;
    return *this;
}

template <int C> template <typename T>
inline LocalStr<C>& LocalStr<C>::operator=(const String_base<T>& x) {
    char* old_rem = length_ > local_capacity ? u_.rem : 0;
    length_ = x.length();
    if (length_ > local_capacity)
        u_.rem = new char[length_];
    memmove(const_cast<char*>(data()), x.data(), length_);
    delete[] old_rem;
    return *this;
}

#endif
