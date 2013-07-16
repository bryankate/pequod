#include "pqdbpool.hh"

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
#if HAVE_LIBPQ
    for (uint32_t i = 0; i < min_; ++i) {
        PGconn* conn = connect_one();
        assert(conn);

        conn_.push_back(conn);
        pool_.push(conn);
    }
#else
    mandatory_assert(false && "Database not configured.");
#endif
}

void DBPool::clear() {
#if HAVE_LIBPQ
    while(!pool_.empty())
        pool_.pop();

    while(!conn_.empty()) {
        PGconn* conn = conn_.back();
        conn_.pop_back();
        PQfinish(conn);
    }
#endif
}

#if HAVE_LIBPQ
tamed void DBPool::insert(String key, String value, event<> e) {
    tvars {
        PGconn* conn;
        String k, v, query;
        int32_t err;
    }

    twait { next_connection(make_event(conn)); }

    char buff[256];
    size_t sz;

    // todo: this does not handle strings with binary data!
    sz = PQescapeStringConn(conn, buff, key.data(), key.length(), &err);
    mandatory_assert(!err);
    k = String(buff, sz);

    sz = PQescapeStringConn(conn, buff, value.data(), value.length(), &err);
    mandatory_assert(!err);
    v = String(buff, sz);

    query = "WITH upsert AS "
            "(UPDATE cache SET value=\'" + v + "\'" +
            "WHERE key=\'" + k + "\'" +
            "RETURNING cache.* ) "
            "INSERT INTO cache "
            "SELECT * FROM (SELECT \'" + k + "\' k, \'" + v + "\' v) AS tmp_table "
            "WHERE CAST(tmp_table.k AS TEXT) NOT IN (SELECT key FROM upsert)";

    err = PQsendQuery(conn, query.c_str());
    mandatory_assert(err == 1 && "Could not send query to DB.");

    do {
        twait { tamer::at_fd_read(PQsocket(conn), make_event()); }
        err = PQconsumeInput(conn);
        mandatory_assert(err == 1 && "Error reading data from DB.");
    } while(PQisBusy(conn));

    PGresult* result = PQgetResult(conn);
    if (PQresultStatus(result) != PGRES_COMMAND_OK)
        mandatory_assert(false && "Error getting result of DB query.");

    PQclear(result);
    result = PQgetResult(conn);
    mandatory_assert(!result && "Should only be one result for an insert!");

    replace_connection(conn);
    e();
}

tamed void DBPool::erase(String key, event<> e) {
    tvars {
        PGconn* conn;
        String k, query;
        int32_t err;
    }

    twait { next_connection(make_event(conn)); }

    char buff[256];
    size_t sz;

    // todo: this does not handle strings with binary data!
    sz = PQescapeStringConn(conn, buff, key.data(), key.length(), &err);
    mandatory_assert(!err);
    k = String(buff, sz);

    query = "DELETE FROM cache WHERE key=\'" + k + "\'";

    err = PQsendQuery(conn, query.c_str());
    mandatory_assert(err == 1 && "Could not send query to DB.");

    do {
        twait { tamer::at_fd_read(PQsocket(conn), make_event()); }
        err = PQconsumeInput(conn);
        mandatory_assert(err == 1 && "Error reading data from DB.");
    } while(PQisBusy(conn));

    PGresult* result = PQgetResult(conn);
    if (PQresultStatus(result) != PGRES_COMMAND_OK)
        mandatory_assert(false && "Error getting result of DB query.");

    PQclear(result);
    result = PQgetResult(conn);
    mandatory_assert(!result && "Should only be one result for an erase!");

    replace_connection(conn);
    e();
}

void DBPool::next_connection(tamer::event<PGconn*> e) {
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

void DBPool::replace_connection(PGconn* conn) {
    if (!waiting_.empty()) {
        waiting_.front()(conn);
        waiting_.pop();
    }
    else
        pool_.push(conn);
}

PGconn* DBPool::connect_one() {
    String cs = "dbname=pequod host=" + host_ + " port=" + String(port_);
    PGconn* conn = PQconnectdb(cs.c_str());
    mandatory_assert(conn);
    mandatory_assert(PQstatus(conn) != CONNECTION_BAD);
    return conn;
}
#else

tamed void DBPool::insert(String key, String value, event<> e) {
    mandatory_assert(false && "Database not configured.");
}

tamed void DBPool::erase(String key, event<> e) {
    mandatory_assert(false && "Database not configured.");
}

#endif

}
