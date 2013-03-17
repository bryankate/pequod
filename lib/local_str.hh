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

    inline char* mutable_data();
    inline uint8_t* mutable_udata();

    inline bool is_local() const;

    inline LocalStr<C>& operator=(const LocalStr<C>& x);
    inline LocalStr<C>& operator=(LocalStr<C>&& x);
    template <typename T>
    inline LocalStr<C>& operator=(const String_base<T>& x);

    inline void assign_uninitialized(int length);

  private:
    union {
        int length;
        struct {
            int length;
            char* data;
        } rem;
        struct {
            int length;
            char data[local_capacity];
        } loc;
    } u_;

    inline void initialize(const char* data, int length);
    inline void uninitialize();
};

template <int C>
inline void LocalStr<C>::initialize(const char* data, int length) {
    u_.length = length;
    if (length > local_capacity) {
        u_.rem.data = new char[length];
        memcpy(u_.rem.data, data, length);
    } else
        memcpy(u_.loc.data, data, length);
}

template <int C>
inline void LocalStr<C>::uninitialize() {
    if (u_.length > local_capacity)
        delete[] u_.rem.data;
}

template <int C>
inline LocalStr<C>::LocalStr() {
    u_.length = 0;
}

template <int C>
inline LocalStr<C>::LocalStr(const LocalStr<C>& x) {
    initialize(x.data(), x.length());
}

template <int C>
inline LocalStr<C>::LocalStr(LocalStr<C>&& x) {
    u_.length = x.u_.length;
    if (u_.length > local_capacity) {
        u_.rem.data = x.u_.rem.data;
        x.u_.length = 0;
    } else
        memcpy(u_.loc.data, x.u_.loc.data, u_.length);
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
    return u_.length > local_capacity ? u_.rem.data : u_.loc.data;
}

template <int C>
inline char* LocalStr<C>::mutable_data() {
    return u_.length > local_capacity ? u_.rem.data : u_.loc.data;
}

template <int C>
inline uint8_t* LocalStr<C>::mutable_udata() {
    return reinterpret_cast<uint8_t*>(mutable_data());
}

template <int C>
inline int LocalStr<C>::length() const {
    return u_.length;
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
    return *this;
}

template <int C>
inline LocalStr<C>& LocalStr<C>::operator=(LocalStr<C>&& x) {
    using std::swap;
    swap(u_.length, x.u_.length);
    char* old_rem = u_.rem.data;
    if (u_.length > local_capacity)
        u_.rem.data = x.u_.rem.data;
    else
        memcpy(u_.loc.data, x.u_.loc.data, u_.length);
    if (x.u_.length > local_capacity)
        x.u_.rem.data = old_rem;
    return *this;
}

template <int C> template <typename T>
inline LocalStr<C>& LocalStr<C>::operator=(const String_base<T>& x) {
    char* old_rem = u_.length > local_capacity ? u_.rem.data : 0;
    u_.length = x.length();
    if (u_.length > local_capacity)
        u_.rem.data = new char[u_.length];
    memmove(const_cast<char*>(data()), x.data(), u_.length);
    delete[] old_rem;
    return *this;
}

template <int C>
inline void LocalStr<C>::assign_uninitialized(int length) {
    if (length <= local_capacity
        ? u_.length > local_capacity
        : length > u_.length)
        delete[] u_.rem.data;
    if (length > local_capacity && length > u_.length)
        u_.rem.data = new char[length];
    u_.length = length;
}

#endif
