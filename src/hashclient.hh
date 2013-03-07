#ifndef HASHCLIENT_HH
#define HASHCLIENT_HH
#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
#include <libmemcached/memcached.hpp>
#endif
#include "str.hh"
#include "hashtable.hh"
#include "string.hh"

namespace pq {

#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
class MemcachedClient {
  public:
    MemcachedClient() {
        const char *config = "--SERVER=localhost --BINARY-PROTOCOL";
        c_ = ::memcached(config, strlen(config));
        mandatory_assert(c_);
        expire_at_ = ::time(NULL) + 20 * 60 * 60; // never expire
    }
    void set(const Str k, const Str v) {
        auto r = memcached_set(c_, k.data(), k.length(),
                               v.data(), v.length(), expire_at_, 0);
        check_error(r);
    }
    void append(const Str k, const Str v) {
        auto r = memcached_append(c_, k.data(), k.length(),
                                  v.data(), v.length(), expire_at_, 0);
        if (r == MEMCACHED_NOTSTORED)
            set(k, v);
        else
            check_error(r);
    }
    const char *get(const Str okey, int32_t offset, size_t *value_length) {
        String k(okey);
        k += String("@");
        k += String(offset);
        uint32_t flags;
        memcached_return_t error;
        const char *v = memcached_get(c_, k.data(), k.length(), value_length, &flags, 
                                      &error);
        check_error(error);
        return v;
    }
    void done_get(const char *v) {
        delete v;
    }
  private:
    void check_error(memcached_return_t r) {
        if (r != MEMCACHED_SUCCESS) {
            std::cerr << memcached_strerror(NULL, r) << std::endl;
            mandatory_assert(0);
        }
    }
    memcached_st *c_;
    time_t expire_at_;
};
#endif

class BuiltinHashClient {
  public:
    BuiltinHashClient() {
        h_.rehash(100000);
    }
    void set(const Str k, const Str v) {
        h_[k] = v;
    }
    void append(const Str k, const Str v) {
        auto& ev = h_[k];
        ev.append(v);
    }
    const char *get(const Str k, int32_t offset, size_t *value_length) {
        auto it = h_.find(k);
        mandatory_assert(it != h_.end());
        mandatory_assert(it->second.length() >= offset);
        *value_length = it->second.length() - offset;
        return it->second.data() + offset;
    }
    void done_get(const char *) {
    }
  private:
    HashTable<String, String> h_;
};

}
#endif
