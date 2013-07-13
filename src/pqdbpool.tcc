#include "pqdbpool.hh"
#ifdef HAVE_PQXX_PQXX
#include "pqxx/pipeline"
#endif

using namespace tamer;

namespace pq {

DBPool::DBPool(const String& host, uint32_t port)
    : host_(host), port_(port), min_(5), max_(10) {
}

DBPool::DBPool(const String& host, uint32_t port, uint32_t min, uint32_t max)
    : host_(host), port_(port), min_(min), max_(max) {
}

DBPool::~DBPool() {
    clear();
}

void DBPool::connect() {
#ifdef HAVE_PQXX_PQXX
    for (uint32_t i = 0; i < min_; ++i) {
        pqxx::connection* conn = connect_one();
        assert(conn);

        conn_.push_back(conn);
        pool_.push(conn);
    }
#else
    mandatory_assert(false && "Database not configured.");
#endif
}

void DBPool::clear() {
#ifdef HAVE_PQXX_PQXX
    while(!pool_.empty())
        pool_.pop();

    while(!conn_.empty()) {
        pqxx::connection* conn = conn_.back();
        conn_.pop_back();
        delete conn;
    }
#endif
}

tamed void DBPool::insert(const String& key, const String& value, event<> e) {
#ifdef HAVE_PQXX_PQXX
    tvars {
        pqxx::connection* conn;
    }

    twait { next_connection(make_event(conn)); }
    do_insert(conn, key, value, e);
#endif
}

tamed void DBPool::erase(const String& key, event<> e) {
#ifdef HAVE_PQXX_PQXX
    tvars {
         pqxx::connection* conn;
     }

     twait { next_connection(make_event(conn)); }
     do_erase(conn, key, e);
#endif
}

#ifdef HAVE_PQXX_PQXX
tamed void DBPool::do_insert(pqxx::connection* conn, const String& key, const String& value, event<> e) {
    tvars {
        pqxx::work txn(*conn);
        pqxx::pipeline pipe(txn);
        std::string k = txn.quote(std::string(key.data(), key.length()));
        std::string v = txn.quote(std::string(value.data(), value.length()));
        int32_t qid;
    }

    qid = pipe.insert("WITH upsert AS "
                      "(UPDATE cache SET value=" + v +
                      " WHERE key=" + k +
                      " RETURNING cache.* ) "
                      "INSERT INTO cache "
                      "SELECT * FROM (SELECT " + k + " k, " + v + " v) AS tmp_table "
                      "WHERE CAST(tmp_table.k AS TEXT) NOT IN (SELECT key FROM upsert)");

    while(!pipe.is_finished(qid)) {
        twait { tamer::at_fd_read(conn->sock(), make_event()); }
        pipe.resume();
    }
    pipe.retrieve(qid);

    replace_connection(conn);
    e();
}

tamed void DBPool::do_erase(pqxx::connection* conn, const String& key, event<> e) {
    tvars {
        pqxx::work txn(*conn);
        pqxx::pipeline pipe(txn);
        std::string k = txn.quote(std::string(key.data(), key.length()));
        int32_t qid;
    }

    qid = pipe.insert("DELETE FROM cache WHERE key=" + k);

    while(!pipe.is_finished(qid)) {
        twait { tamer::at_fd_read(conn->sock(), make_event()); }
        pipe.resume();
    }
    pipe.retrieve(qid);

    replace_connection(conn);
    e();
}

void DBPool::next_connection(tamer::event<pqxx::connection*> e) {
    if (!pool_.empty()) {
        e(pool_.front());
        pool_.pop();
    }
    else if (conn_.size() < max_) {
        conn_.push_back(connect_one());
        e(conn_.back());
    }
    else
        waiting_.push(e);
}

void DBPool::replace_connection(pqxx::connection* conn) {
    if (!waiting_.empty()) {
        waiting_.front()(conn);
        waiting_.pop();
    }
    else
        pool_.push(conn);
}

pqxx::connection* DBPool::connect_one() {
    String cs = "dbname=pequod host=" + host_ + " port=" + String(port_);
    return new pqxx::connection(cs.c_str());
}
#endif

}
