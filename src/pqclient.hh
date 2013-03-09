#ifndef PEQUOD_CLIENT_HH
#define PEQUOD_CLIENT_HH
#include "pqserver.hh"
#include <tamer/tamer.hh>
namespace pq {
using tamer::event;
using tamer::preevent;

class DirectClient {
  public:
    inline DirectClient(Server& server);

    template <typename R>
    inline void add_join(const String& first, const String& last,
                         const String& join_text, preevent<R> e);

    template <typename R>
    inline void insert(const String& key, const String& value, preevent<R> e);
    template <typename R>
    inline void erase(const String& key, preevent<R> e);

    template <typename R>
    inline void count(const String& first, const String& last,
                      preevent<R, size_t> e);
    template <typename R>
    inline void add_count(const String& first, const String& last,
                          preevent<R, size_t> e);

    class scan_result {
      public:
        typedef ServerStore::const_iterator iterator;
        scan_result() = default;
        inline scan_result(iterator first, iterator last);
        inline iterator begin() const;
        inline iterator end() const;
      private:
        iterator first_;
        iterator last_;
    };
    template <typename R>
    inline void scan(const String& first, const String& last,
                     preevent<R, scan_result> e);

    template <typename R>
    inline void stats(preevent<R, Json> e);

  private:
    Server& server_;
};


inline DirectClient::DirectClient(Server& server)
    : server_(server) {
}

template <typename R>
inline void DirectClient::add_join(const String& first, const String& last,
                                   const String& join_text, preevent<R> e) {
    Join* j = new Join;
    j->assign_parse(join_text);
    server_.add_join(first, last, j);
    e();
}

template <typename R>
inline void DirectClient::insert(const String& key, const String& value,
                                 preevent<R> e) {
    server_.insert(key, value);
    e();
}

template <typename R>
inline void DirectClient::erase(const String& key, preevent<R> e) {
    server_.erase(key);
    e();
}

template <typename R>
inline void DirectClient::count(const String& first, const String& last,
                                preevent<R, size_t> e) {
    server_.validate(first, last);
    e(server_.count(first, last));
}

template <typename R>
inline void DirectClient::add_count(const String& first, const String& last,
                                    preevent<R, size_t> e) {
    server_.validate(first, last);
    e(e.result() + server_.count(first, last));
}

inline DirectClient::scan_result::scan_result(iterator first, iterator last)
    : first_(first), last_(last) {
}

inline auto DirectClient::scan_result::begin() const -> iterator {
    return first_;
}

inline auto DirectClient::scan_result::end() const -> iterator {
    return last_;
}

template <typename R>
inline void DirectClient::scan(const String& first, const String& last,
                               preevent<R, scan_result> e) {
    server_.validate(first, last);
    e(scan_result(server_.lower_bound(first),
                  server_.lower_bound(last)));
}

template <typename R>
inline void DirectClient::stats(preevent<R, Json> e) {
    e(server_.stats());
}

} // namespace pq
#endif
