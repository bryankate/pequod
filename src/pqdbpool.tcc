#include "pqdbpool.hh"
#include "str.hh"
#include <sstream>

using namespace tamer;

namespace pq {

DBPoolParams::DBPoolParams()
    : dbname("pequod"), host("127.0.0.1"), port(10000),
      min(1), max(1), pipeline_depth(1), pipeline_timeout(2000) {
}

DBPool::DBPool(const String& host, uint32_t port) {
    params_.host = host;
    params_.port = port;
}

DBPool::DBPool(const DBPoolParams& params) : params_(params) {
}

DBPool::~DBPool() {
    clear();
}

void DBPool::connect() {
#if HAVE_LIBPQ
    for (uint32_t i = 0; i < params_.min; ++i) {
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
tamed void DBPool::execute(Str query, event<Json> e) {
    tvars {
       PGconn* conn;
    }

    if (query_buffer_.empty())
        oldest_ = tstamp();

    query_buffer_.push_back(query);
    event_buffer_.push_back(e);

    maybe_flush();
}

tamed void DBPool::flush() {
    tvars {
        PGconn* conn;
        query_pipe_t queries = this->query_buffer_;
        event_pipe_t events = this->event_buffer_;
    }

    if (query_buffer_.empty())
        return;

    query_buffer_.clear();
    event_buffer_.clear();
    oldest_ = 0;

    twait { next_connection(make_event(conn)); }
    twait { execute_pipeline(conn, queries, events, make_event()); }
    replace_connection(conn);
}

tamed void DBPool::execute_pipeline(PGconn* conn,
                                    const query_pipe_t& queries,
                                    event_pipe_t& events,
                                    event<> e) {
    tvars {
       int32_t err;
       PGresult* result;
       uint32_t r = 0;
       ExecStatusType status;
       Json ret;
    }

    {
        std::stringstream ss;
        for (auto& q : queries)
            ss << q << "; ";

        // might block. documentation says it is rare but we could use the
        // non-blocking write calls in libpq
        err = PQsendQuery(conn, ss.str().c_str());
        mandatory_assert(err == 1 && "Could not send query to DB.");
    }

    while(true) {
        while(PQisBusy(conn)) {
            twait { tamer::at_fd_read(PQsocket(conn), make_event()); }
            err = PQconsumeInput(conn);
            mandatory_assert(err == 1 && "Error reading data from DB.");
        }

        result = PQgetResult(conn);

        if (!result)
            break;

        status = PQresultStatus(result);
        ret.clear();

        switch(status) {
            case PGRES_COMMAND_OK:
                // command (e.g. insert, delete) returns no data
                break;
            case PGRES_TUPLES_OK: {
                int32_t nrows = PQntuples(result);
                int32_t ncols = PQnfields(result);

                for (int32_t r = 0; r < nrows; ++r) {
                    ret.push_back(Json::make_array_reserve(ncols));
                    for (int32_t c = 0; c < ncols; ++c) {
                        if (PQgetisnull(result, r, c))
                            ret[r][c] = Json::null_json;
                        else
                            ret[r][c] = Str(PQgetvalue(result, r, c),
                                            PQgetlength(result, r, c));
                    }
                }
                break;
            }
            default: {
                std::cerr << "Error getting result of DB query. " << std::endl
                        << "  Status:  " << PQresStatus(status) << std::endl
                        << "  Message: " << PQresultErrorMessage(result) << std::endl;
                mandatory_assert(false);
                break;
            }
        }

        events[r++](ret);
        PQclear(result);
    }

    e();
}

void DBPool::next_connection(tamer::event<PGconn*> e) {
    if (!pool_.empty()) {
        e(pool_.front());
        pool_.pop();
    }
    else if (conn_.size() < params_.max) {
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
    String cs = "dbname=" + params_.dbname + " host=" + params_.host + " port=" + String(params_.port);
    PGconn* conn = PQconnectdb(cs.c_str());
    mandatory_assert(conn);
    mandatory_assert(PQstatus(conn) != CONNECTION_BAD);
    return conn;
}

tamed void DBPool::add_prepared(const std::vector<String>& statements, tamer::event<> e) {
    tvars {
        std::vector<PGconn*> local_conns; 
        std::vector<PGconn*>::iterator c;
        std::vector<tamer::event<Json>> events;
        int32_t i, outstanding_count;
        PGconn* temp_conn;
        Json j;
    }

    if (!query_buffer_.empty())
        flush();

    // force max connections
    for ( i = params_.max - conn_.size(); i > 0; --i) {
        local_conns.push_back(connect_one());
        conn_.push_back(local_conns.back());
    }

    // wait for existing outstanding connections
    outstanding_count = params_.max - local_conns.size();
    for (i = 0; i < outstanding_count; ++i) {
        twait { next_connection(make_event(temp_conn)); }     
        local_conns.push_back(temp_conn);
    }

    // add the prepared statements to each connection
    for (c = local_conns.begin(); c != local_conns.end(); ++c ) {
        twait {
            events.clear();
            for (i = 0; i < (int32_t)statements.size(); ++i)
                events.push_back(make_event(j));
            execute_pipeline(*c, statements, events, make_event());
        }

        replace_connection(*c);
    }

    e();
}

#else

tamed void DBPool::execute(Str query, event<Json> e) {
    mandatory_assert(false && "Database not configured.");
}

tamed void DBPool::add_prepared(const std::vector<String>& statements, tamer::event<> e) {
    mandatory_assert(false && "Database not configured.");
}

tamed void DBPool::flush() {
    mandatory_assert(false && "Database not configured.");
}

#endif

}
