#ifndef PEQUOD_CLIENT_HH
#define PEQUOD_CLIENT_HH
#include "pqserver.hh"
#include <tamer/tamer.hh>
namespace pq {
using tamer::event;

class DirectClient {
  public:
    inline DirectClient(Server& server);

    inline void add_join(const String& first, const String& last,
                         const String& join_text, event<> e);

    inline void insert(const String& key, const String& value, event<> e);
    inline void erase(const String& key, event<> e);

    inline void count(const String& first, const String& last,
                      event<size_t> e);

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
    inline void scan(const String& first, const String& last,
                     event<scan_result> e);

    inline void stats(event<Json> e);

  private:
    Server& server_;
};


inline DirectClient::DirectClient(Server& server)
    : server_(server) {
}

inline void DirectClient::add_join(const String& first, const String& last,
                                   const String& join_text, event<> e) {
    Join* j = new Join;
    j->assign_parse(join_text);
    server_.add_join(first, last, j);
    e();
}

inline void DirectClient::insert(const String& key, const String& value,
                                 event<> e) {
    server_.insert(key, value);
    e();
}

inline void DirectClient::erase(const String& key, event<> e) {
    server_.erase(key);
    e();
}

inline void DirectClient::count(const String& first, const String& last,
                                event<size_t> e) {
    server_.validate(first, last);
    e(server_.count(first, last));
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

inline void DirectClient::scan(const String& first, const String& last,
                               event<scan_result> e) {
    server_.validate(first, last);
    e(scan_result(server_.lower_bound(first),
                  server_.lower_bound(last)));
}

inline void DirectClient::stats(event<Json> e) {
    e(server_.stats());
}

} // namespace pq
#endif
