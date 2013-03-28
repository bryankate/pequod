#ifndef PEQUOD_CLIENT_HH
#define PEQUOD_CLIENT_HH
#include "pqserver.hh"
#include "error.hh"
#include <tamer/tamer.hh>
namespace pq {
using tamer::event;
using tamer::preevent;

class DirectClient {
  public:
    inline DirectClient(Server& server);

    inline void add_join(const String& first, const String& last,
                         const String& join_text, tamer::event<Json> e);

    inline void get(const String& key, tamer::event<String> e);

    inline void insert(const String& key, const String& value, tamer::event<> e);
    inline void erase(const String& key, tamer::event<> e);

    inline void count(const String& first, const String& last,
                      tamer::event<size_t> e);
    inline void add_count(const String& first, const String& last,
                          tamer::event<size_t> e);

    inline void pace(tamer::event<> done);

    typedef Table::iterator iterator;
    class scan_result {
      public:
        scan_result() = default;
        inline scan_result(iterator first, iterator last);
        inline iterator begin() const;
        inline iterator end() const;
        inline void flush();
        inline size_t size() const;
      private:
        iterator first_;
        iterator last_;
    };
    inline void scan(const String& first, const String& last,
                     tamer::event<scan_result> e);

    inline void stats(tamer::event<Json> e);


    // preevent versions
    template <typename R>
    inline void get(const String& key, preevent<R, String> e);

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

    template <typename R>
    inline void pace(preevent<R> done);

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

inline void DirectClient::add_join(const String& first, const String& last,
                                   const String& join_text, event<Json> e) {
    ErrorAccumulator errh;
    Json rj = Json().set("range", Json::make_array(first, last));
    Join* j = new Join;
    if (j->assign_parse(join_text, &errh)) {
        server_.add_join(first, last, j);
        rj.set("ok", true);
    }
    if (!errh.empty())
        rj.set("message", errh.join());
    e(rj);
}

inline void DirectClient::get(const String& key, event<String> e) {
    Table& t = server_.table_for(key);
    auto it = t.validate(key, server_.next_validate_at());
    if (it != t.end() && it->key() == key)
        e(it->value());
    else
        e(String());
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
    e(server_.validate_count(first, last));
}

inline void DirectClient::add_count(const String& first, const String& last,
                                    event<size_t> e) {
    e(e.result() + server_.validate_count(first, last));
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

inline void DirectClient::scan_result::flush() {
}

inline size_t DirectClient::scan_result::size() const {
    return std::distance(first_, last_);
}

inline void DirectClient::scan(const String& first, const String& last,
                               event<scan_result> e) {
    Table& t = server_.table_for(first);
    auto it = t.validate(first, last, server_.next_validate_at());
    e(scan_result(it, t.lower_bound(last)));
}

inline void DirectClient::pace(event<> done) {
    done();
}

inline void DirectClient::stats(event<Json> e) {
    e(server_.stats());
}


template <typename R>
inline void DirectClient::get(const String& key, preevent<R, String> e) {
    Table& t = server_.table_for(key);
    auto it = t.validate(key, server_.next_validate_at());
    if (it != t.end() && it->key() == key)
        e(it->value());
    else
        e(String());
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
    e(server_.validate_count(first, last));
}

template <typename R>
inline void DirectClient::add_count(const String& first, const String& last,
                                    preevent<R, size_t> e) {
    e(e.result() + server_.validate_count(first, last));
}

template <typename R>
inline void DirectClient::scan(const String& first, const String& last,
                               preevent<R, scan_result> e) {
    Table& t = server_.table_for(first);
    auto it = t.validate(first, last, server_.next_validate_at());
    e(scan_result(it, t.lower_bound(last)));
}

template <typename R>
inline void DirectClient::pace(preevent<R> done) {
    done();
}

template <typename R>
inline void DirectClient::stats(preevent<R, Json> e) {
    e(server_.stats());
}

} // namespace pq
#endif
