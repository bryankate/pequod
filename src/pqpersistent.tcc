#include "pqpersistent.hh"
#include "pqserver.hh"

namespace pq {

#if HAVE_LIBPQ

PostgresStore::PostgresStore(const DBPoolParams& params)
    : params_(params), pool_(nullptr), monitor_(nullptr) {
}

PostgresStore::~PostgresStore() {
    delete pool_;
    if (monitor_)
        PQfinish(monitor_);
}

void PostgresStore::connect() {
    std::vector<String> statements({
        "PREPARE kv_put(text,text) AS "
            "WITH upsert AS "
            "(UPDATE cache SET value=$2 WHERE key=$1 RETURNING cache.*) "
            "INSERT INTO cache "
            "SELECT * FROM (SELECT $1 k, $2 v) AS tmp_table "
            "WHERE CAST(tmp_table.k AS TEXT) NOT IN (SELECT key FROM upsert)",
        "PREPARE kv_erase(text) AS "
            "DELETE FROM cache WHERE key=$1",
        "PREPARE kv_get(text) AS "
            "SELECT value FROM cache WHERE key=$1",
        "PREPARE kv_scan(text,text) AS "
            "SELECT key, value FROM cache WHERE key >= $1 AND key < $2"});

    pool_ = new DBPool(params_);
    pool_->connect_all(statements);
}

tamed void PostgresStore::put(Str key, Str value, tamer::event<> done) {
    tvars {
        String q = "EXECUTE kv_put('" + key + "','" + value + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }
    done();
}

tamed void PostgresStore::erase(Str key, tamer::event<> done) {
    tvars {
        String q = "EXECUTE kv_erase('" + key + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }
    done();
}

tamed void PostgresStore::get(Str key, tamer::event<String> done) {
    tvars {
        String q = "EXECUTE kv_get('" + key + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }

    if (j.is_a() && j.size() && j[0].size())
        done(j[0][0].as_s());
    else
        // todo: distinguish between no value and empty value!
        done("");
}

tamed void PostgresStore::scan(Str first, Str last, tamer::event<ResultSet> done) {
    tvars {
        String q = "EXECUTE kv_scan('" + first + "','" + last + "')";
        Json j;
    }

    twait { pool_->execute(q, make_event(j)); }

    ResultSet& rs = done.result();
    for (auto it = j.abegin(); it < j.aend(); ++it )
        rs.push_back(Result((*it)[0].as_s(), (*it)[1].as_s()));

    done.unblocker().trigger();
}

void PostgresStore::flush() {
    pool_->flush();
}

void PostgresStore::run_monitor(Server& server) {
    String cs = "dbname=" + params_.dbname + " host=" + params_.host + " port=" + String(params_.port);
    monitor_ = PQconnectdb(cs.c_str());
    mandatory_assert(monitor_);
    mandatory_assert(PQstatus(monitor_) != CONNECTION_BAD);

    PGresult* res;
    res = PQexec(monitor_,
                 "CREATE OR REPLACE FUNCTION notify_upsert_listener() "
                 "RETURNS trigger AS "
                 "$BODY$ "
                 "BEGIN "
                 "PERFORM pg_notify('backend_queue', '{ \"op\":0, \"k\":\"' || CAST (NEW.key AS TEXT) || '\", \"v\":\"' || CAST (NEW.value AS TEXT) || '\" }'); "
                 "RETURN NULL; "
                 "END; "
                 "$BODY$ "
                 "LANGUAGE plpgsql VOLATILE "
                 "COST 100");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_,
                 "CREATE OR REPLACE FUNCTION notify_delete_listener() "
                 "RETURNS trigger AS "
                 "$BODY$ "
                 "BEGIN "
                 "PERFORM pg_notify('backend_queue', '{ \"op\":1, \"k\":\"' || CAST (OLD.key AS TEXT) || '\" }'); "
                 "RETURN NULL; "
                 "END; "
                 "$BODY$ "
                 "LANGUAGE plpgsql VOLATILE "
                 "COST 100");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_, "DROP TRIGGER IF EXISTS notify_upsert_cache ON cache");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_, "DROP TRIGGER IF EXISTS notify_delete_cache ON cache");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_,
                 "CREATE TRIGGER notify_upsert_cache "
                 "AFTER INSERT OR UPDATE ON cache "
                 "FOR EACH ROW "
                 "EXECUTE PROCEDURE notify_upsert_listener()");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_,
                 "CREATE TRIGGER notify_delete_cache "
                 "AFTER DELETE ON cache "
                 "FOR EACH ROW "
                 "EXECUTE PROCEDURE notify_delete_listener()");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    res = PQexec(monitor_, "LISTEN backend_queue");

    mandatory_assert(res);
    mandatory_assert(PQresultStatus(res) == PGRES_COMMAND_OK);
    PQclear(res);

    monitor_db(server);
}

tamed void PostgresStore::monitor_db(Server& server) {
    tvars {
        int32_t err;
        Json j;
    }

    while(true) {
        // XXX: we might need to yield once in a while...

        do {
            twait { tamer::at_fd_read(PQsocket(monitor_), make_event()); }
            err = PQconsumeInput(monitor_);
            mandatory_assert(err == 1 && "Error reading data from DB.");
        } while(PQisBusy(monitor_));

        // there should be no results on this connection
        mandatory_assert(!PQgetResult(monitor_));

        // process notifications
        PGnotify* n = PQnotifies(monitor_);
        while(n) {

            j.assign_parse(n->extra);
            assert(j && j.is_o());

            switch(j["op"].as_i()) {
                case pg_update:
                    server.insert(j["k"].as_s(), j["v"].as_s());
                    break;

                case pg_delete:
                    server.erase(j["k"].as_s());
                    break;

                default:
                    mandatory_assert(false && "Unknown DB operation.");
                    break;
            }

            PQfreemem(n);
            n = PQnotifies(monitor_);
        }
    }
}

#endif
}
