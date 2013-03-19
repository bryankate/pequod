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
        if (error != MEMCACHED_SUCCESS) {
            mandatory_assert(v == NULL);
            *value_length = 0;
        }
        return v;
    }
    void done_get(const char *v) {
        if (v)
            delete v;
    }
    void increment(const Str) {
        mandatory_assert(0, "unimplemented: need to change the server side");
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
        if (it == h_.end()) {
            *value_length = 0;
            return NULL;
        }
        mandatory_assert(it->second.length() >= offset);
        *value_length = it->second.length() - offset;
        return it->second.data() + offset;
    }
    void done_get(const char *) {
    }
    void increment(const Str k) {
        auto& ev = h_[k];
        if (ev.empty())
            ev = String(1);
        else
            ev = String(ev.to_i() + 1);
    }
  private:
    HashTable<String, String> h_;
};


template <typename S>
class TwitterHashShim {
  public:
    TwitterHashShim(S& server);

    template <typename R>
    inline void subscribe(uint32_t subscriber, uint32_t poster, tamer::preevent<R> e);
    template <typename R>
    inline void mark_celebrity(uint32_t poster, tamer::preevent<R> e);
    template <typename R>
    inline void post(uint32_t poster, uint32_t time, Str value, tamer::preevent<R> e);
    inline void initialize(TwitterPopulator& tp, tamer::event<> e);
    inline void prepare_push_post(uint32_t poster, uint32_t time, Str value);
    template <typename R>
    inline void push_post(uint32_t subscriber, tamer::preevent<R> e);
    typedef DirectClient::scan_result scan_result;
    template <typename R>
    inline void timeline_scan(uint32_t subscriber, uint32_t start_time, uint32_t now, tamer::preevent<R, scan_result> e);
    template <typename R>
    inline void timeline_add_count(uint32_t subscriber, uint32_t start_time, uint32_t now, tamer::preevent<R, size_t> e);
    template <typename R>
    inline void pace(tamer::preevent<R> e);
    template <typename R>
    inline void stats(tamer::preevent<R, Json> e);

  private:
    S& server_;
    char buf_[128];
    int buflen_;
    struct tlstatus {
        uint32_t time;
        size_t pos;
        tlstatus() : time(0), pos(0) {}
    };
    std::vector<tlstatus> last_refresh_;
};

template <typename S>
TwitterHashShim<S>::TwitterHashShim(S& server)
    : server_(server) {
}

template <typename S>
inline void TwitterHashShim<S>::initialize(TwitterPopulator&, tamer::event<> done) {
    done();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::subscribe(uint32_t subscriber, uint32_t poster, tamer::preevent<R> done) {
    sprintf(buf_, "s|%05u %05u", subscriber, poster);
    server_.append(Str(buf_, 7), Str(buf_ + 9, 5));
    done();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::mark_celebrity(uint32_t poster, tamer::preevent<R> done) {
    (void) poster;
    done();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::post(uint32_t poster, uint32_t time, Str value, tamer::preevent<R> done) {
    sprintf(buf_, "p|%05u|%010u", poster, time);
    server_.set(Str(buf_, 18), value);
    done();
}

template <typename S>
inline void TwitterHashShim<S>::prepare_push_post(uint32_t poster, uint32_t time, Str value) {
    sprintf(buf_, "t|%05u %05u|%010u\254", 0, poster, time);
    assert(sizeof(buf_) >= size_t(value.length()) + 26);
    memcpy(buf_ + 25, value.data(), value.length());
    buflen_ = 25 + value.length();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::push_post(uint32_t subscriber, tamer::preevent<R> done) {
    sprintf(buf_ + 2, "%05u", subscriber);
    server_.append(Str(buf_, 7), Str(buf_ + 8, buflen_ - 8));
    done();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::timeline_scan(uint32_t subscriber, uint32_t start_time, uint32_t, tamer::preevent<R, scan_result> done) {
    (void) subscriber, (void) start_time;
    assert(0);
    done.unblock();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::timeline_add_count(uint32_t subscriber, uint32_t start_time, uint32_t now, tamer::preevent<R, size_t> done) {
    if (last_refresh_.size() <= subscriber)
        last_refresh_.resize(subscriber + 1);
    tlstatus& tls = last_refresh_[subscriber];
    if (tls.time > start_time)
        tls.pos = 0;
    tls.time = now;
    sprintf(buf_, "t|%05u", subscriber);
    size_t len;
    const char* v = server_.get(Str(buf_, 7), tls.pos, &len);
    tls.pos += len;
    Str str(v, len);
    size_t n = 0;
    for (int pos = 0; (pos = str.find_left('\254', pos)) != -1; ++pos) {
        const char *p;
        for (p = v + pos; *p != '|'; --p);
        if (uint32_t(atoi(p + 1)) >= start_time)
            ++n;
    }
    done(done.result() + n);
    server_.done_get(v);
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::pace(tamer::preevent<R> done) {
    done();
}

template <typename S> template <typename R>
inline void TwitterHashShim<S>::stats(tamer::preevent<R, Json> done) {
    done(Json());
}

} // namespace pq
#endif
