#include "pqdbpool.hh"
#include "str.hh"
#include "time.hh"
#include <sstream>

using namespace tamer;

namespace pq {

DBPool::DBPool(const String& host, uint32_t port)
    : host_(host), port_(port), min_(1), max_(10) {
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
tamed void DBPool::execute(String query, event<Json> e) {

    tvars {
       PGconn* conn;
       std::deque<event<Json>> do_events;
       std::stringstream queries(std::ios_base::app | std::ios_base::out);
    }

    if (query_buffer.size() == 0)
        first_insert = tstamp();


    query_buffer.push_back(query);
    event_buffer.push_back(e);

    if (query_buffer.size() < 250 && ((tstamp() - first_insert) < 200))
        return;
    else {
        for(auto i = 0; !query_buffer.empty() && i < 250; ++i) {
            queries << query_buffer.front() << ";";
            query_buffer.pop_front();
            do_events.push_back(event_buffer.front());
            event_buffer.pop_front();
        }
	    twait { next_connection(make_event(conn)); }
	    twait { do_execute(conn, queries.str(), do_events, make_event()); }
	    replace_connection(conn);
    }
}

tamed void DBPool::execute(PGconn* conn, String query, event<Json> e) {

    tvars { std::deque<event<Json>> wrapper; }
    wrapper.push_back(e);
    twait { do_execute(conn, query.c_str(), wrapper, make_event()); }
}

tamed void DBPool::do_execute(PGconn* conn, std::string q_set, std::deque<event<Json>> events, event<> e) {

    tvars {
       int32_t err;
       int32_t result_count = 0;
       PGresult* result;
       ExecStatusType status;
       Json ret;
    }

#if 0
    {
        // todo: this does not handle strings with binary data!
        char buff[1024];
        size_t sz = PQescapeStringConn(conn, buff, query.data(), query.length(), &err);
        String q(buff, sz);

        err = PQsendQuery(conn, q.c_str());
    }
#else
    err = PQsendQuery(conn, q_set.c_str());
#endif

    if (err != 1)
        std::cerr << "Err: " <<  PQerrorMessage(conn) << std::endl;
    mandatory_assert(err == 1 && "Could not send query to DB.");

    for(;;) {
	    err = PQconsumeInput(conn);
	    mandatory_assert(err == 1 && "Error reading data from DB.");
	    if (PQisBusy(conn)) 
	        twait { tamer::at_fd_read(PQsocket(conn), make_event()); }

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
	                    ret[r][c] = Str(PQgetvalue(result, r, c), PQgetlength(result, r, c));
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
        events.front()(ret);
        events.pop_front();
	    PQclear(result);
    };
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

tamed void DBPool::add_prepared_pool(std::vector<String> statements, tamer::event<> e) {
    tvars {
        std::vector<PGconn*> local_conns; 
        int32_t i, outstanding_count;
        PGconn* temp_conn;
        Json j;
        std::vector<PGconn*>::iterator c;
        std::vector<String>::iterator s;
    }

    // force max connections
    for ( i = max_ - conn_.size(); i > 0; --i) {
        local_conns.push_back(connect_one());
        conn_.push_back(local_conns.back());
    }

    // wait for existing outstanding connections
    outstanding_count = max_ - local_conns.size();
    for (i = 0; i < outstanding_count; ++i) {
        twait { next_connection(make_event(temp_conn)); }     
        local_conns.push_back(temp_conn);
    }

   // add the prepared statement to each connection
    for (c = local_conns.begin(); c != local_conns.end(); ++c ){
        for (s = statements.begin(); s != statements.end(); ++s) {
            twait { execute(*c, *s, make_event(j)); }
        }
        // replace the connection
        replace_connection(*c);
       }
   e();
}

#else

tamed void DBPool::execute(String query, event<Json> e) {
    mandatory_assert(false && "Database not configured.");
}

tamed void DBPool::add_prepared_pool(std::vector<String> statements, tamer::event<> e) {
    mandatory_assert(false && "Database not configured.");
}

#endif

}
