#include "pqpersistent.hh"
#include "pqserver.hh"
#include <boost/date_time.hpp>

namespace pq {

PersistentRead::PersistentRead(Str first, Str last, PersistentStore::ResultSet& rs)
    : rs_(rs), first_(first), last_(last) {
}

void PersistentRead::operator()(PersistentStore* ps){
    ps->scan(first_, last_, rs_);
    tev_();
}

void PersistentRead::set_trigger(tamer::event<> t) {
    tev_ = t;
}

PersistentWrite::PersistentWrite(Str key, Str value)
    : key_(key), value_(value) {
}

void PersistentWrite::operator()(PersistentStore* ps){
    ps->put(key_, value_);
    delete this;
}

PersistentStoreThread::PersistentStoreThread(PersistentStore* store)
    : store_(store), worker_(&PersistentStoreThread::run, this),
      pending_(1024), running_(true) {
}

PersistentStoreThread::~PersistentStoreThread() {
    running_ = false;
    cond_.notify_all();
    worker_.join();
    delete store_;
}

void PersistentStoreThread::enqueue(PersistentOp* op) {
    pending_.enqueue(op);
    cond_.notify_all();
}

void PersistentStoreThread::run() {
    PersistentOp *op;
    while(running_) {
        if (pending_.try_dequeue(op))
            (*op)(store_);
        else {
            boost::mutex::scoped_lock lock(mu_);
            cond_.timed_wait(lock, boost::posix_time::milliseconds(5));
        }
    }
}

}


#if HAVE_DB_CXX_H
#include <iostream>

BerkeleyDBStore::BerkeleyDBStore(std::string eH, std::string dbN, uint32_t e_flags, uint32_t d_flags)
    : env_home_(eH), db_name_(dbN), env_(new DbEnv(0)) {
    init(e_flags, d_flags);
}

BerkeleyDBStore::~BerkeleyDBStore() {
    try {
        if (dbh_ != NULL) {
            dbh_->close(0);
        }
        env_->close(0);
        delete dbh_;
        delete env_;
    } catch(DbException &e) {
        std::cerr << "Error closing database environment: "
                << env_home_
                << " or database "
                << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    } catch(std::exception &e) {
        std::cerr << "Error closing database environment: "
                << env_home_
                << " or database "
                << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    }
}

void BerkeleyDBStore::init(uint32_t env_flags, uint32_t db_flags) {
    try {
        env_->open(env_home_.c_str(), env_flags, 0);
        dbh_ = new Db(env_, 0);
        dbh_->open(NULL,
                   db_name_.c_str(),
                   NULL,
                   DB_BTREE,
                   db_flags,
                   0);
        uint32_t ndropped = 0;
        dbh_->truncate(NULL, &ndropped, 0);
    } catch(DbException &e) {
        std::cerr << "Error opening database or environment: "
                  << env_home_ << std::endl
                  << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    } catch(std::exception &e) {
        std::cerr << "Error opening database or environment: "
                  << env_home_ << std::endl
                  << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    }
}

int32_t BerkeleyDBStore::put(Str key, Str val){

    int32_t ret;

    Dbt k(key.mutable_data(), key.length());
    Dbt v(val.mutable_data(), val.length());

    ret = dbh_->put(NULL, &k, &v, 0);
    if (ret != 0) {
        env_->err(ret, "Database put failed.");
        return ret;
    }

    return ret;
}

String BerkeleyDBStore::get(Str k){

    Dbt key(k.mutable_data(), k.length());
    Dbt val;
    int32_t ret;

    val.set_flags(DB_DBT_MALLOC);

    ret = dbh_->get(NULL, &key, &val, 0);
    if (ret != 0) {
        env_->err(ret, "Database get failed.");
        std::cerr << "this is error..bad get." << std::endl;
    }

    return String((char*)val.get_data(), val.get_size());
}

void BerkeleyDBStore::scan(Str first, Str last, pq::PersistentStore::ResultSet& results){
    int ret, include_last_element;
    ret = include_last_element = 0;
    Dbc *db_cursor;
    // start a new cursor
    try{
        dbh_->cursor(NULL, &db_cursor, BerkeleyDBStore::cursor_flags_);
    } catch(DbException &e) {
        std::cerr << "Scan Error: opening database cursor failed: "
                  << env_home_ << std::endl
                  << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
    } catch(std::exception &e) {
        std::cerr << "Scan Error: opening database cursor failed: "
                  << env_home_ << std::endl
                  << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
    }

    Dbt key(first.mutable_data(), first.length());
    Dbt end_key(last.mutable_data(), last.length());
    Dbt value = Dbt();
    value.set_flags(DB_DBT_MALLOC);

    ret = db_cursor->get(&end_key, &value, DB_SET_RANGE);
    if (ret == DB_NOTFOUND) {
        // if `last' is DB_NOTFOUND, we will shift to the last element in the
        // database which should also be included in the while below
        include_last_element = 1;
        ret = db_cursor->get(&end_key, &value, DB_LAST);
        if (ret == DB_NOTFOUND)
            return;
        assert(ret == 0);
    }

    String end((char*) end_key.get_data(), end_key.get_size());

    db_cursor->get(&key, &value, DB_SET_RANGE); //over write the end value since we only care what the key is

    String k((char*) key.get_data(), key.get_size());
    String v((char*) value.get_data(), value.get_size());
    while (k < end || (k == end && include_last_element--)) {
        results.push_back(PersistentStore::Result(k, v));
        db_cursor->get(&key, &value, DB_NEXT);
        k = String((char*) key.get_data(), key.get_size());
        v = String((char*) value.get_data(), value.get_size());
    }
}

void BerkeleyDBStore::run_monitor(pq::Server&) {
    mandatory_assert(false && "DB monitoring not supported for BerkeleyDB.");
}

#endif

#if HAVE_PQXX_NOTIFICATION
#include <iostream>

PostgresStore::PostgresStore(String db, String host, uint32_t port)
    : dbname_(db), host_(host), port_(port),
      dbh_(new pqxx::connection(connection_string().c_str())), monitor_(nullptr) {
    init();
}

PostgresStore::~PostgresStore() {
    delete dbh_;
    delete monitor_;
}

void PostgresStore::init() {
    try{
        // start with an empty table, make it empty if needed
        pqxx::work txn(*dbh_);
        txn.exec(
            "DROP TABLE IF EXISTS cache"
        );
        txn.exec(
            "CREATE TABLE cache (key varchar, value varchar)"
        );
        txn.commit();
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        mandatory_assert(false);
    }
}

int32_t PostgresStore::put(Str key, Str val){
    try{
        pqxx::work txn(*dbh_);
        auto k = txn.quote(std::string(key.mutable_data(), key.length()));
        auto v = txn.quote(std::string(val.mutable_data(), val.length()));
        txn.exec(
            "WITH upsert AS " 
            "(UPDATE cache SET value=" + v +
            " WHERE key=" + k +
            " RETURNING cache.* ) " 
            "INSERT INTO cache " 
            "SELECT * FROM (SELECT " + k + " k, " + v + " v) AS tmp_table " 
            "WHERE CAST(tmp_table.k AS TEXT) NOT IN (SELECT key FROM upsert)"
        );
        txn.commit();
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}

String PostgresStore::get(Str k){
    try{
        pqxx::work txn(*dbh_);
        pqxx::result scan = txn.exec(
            "SELECT key, value FROM cache "
            "WHERE key = " +
            txn.quote(std::string(k.mutable_data(), k.length()))
        );
        return String(scan.begin()["value"].as<std::string>());
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        mandatory_assert(false);
    }
}

void PostgresStore::scan(Str first, Str last, pq::PersistentStore::ResultSet& results){
    try{
        pqxx::work txn(*dbh_);
        pqxx::result scan = txn.exec(
            "SELECT key, value FROM cache "
            "WHERE key >= " +
            txn.quote(std::string(first.mutable_data(), first.length())) +
            " AND key < " +
            txn.quote(std::string(last.mutable_data(), last.length()))
        );

        for (auto row = scan.begin(); row != scan.end(); ++row) {
            results.push_back(PersistentStore::Result(
                String(row["key"].as<std::string>()),
                String(row["value"].as<std::string>())
            ));
        }
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        mandatory_assert(false);
    }
}

String PostgresStore::connection_string() const {
    return "dbname=" + dbname_ + " host=" + host_ + " port=" + port_;
}

void PostgresStore::run_monitor(pq::Server& server) {
    monitor_ = new pqxx::connection(connection_string().c_str());

    try {
        pqxx::work txn(*monitor_);

        // create the notify trigger function
        txn.exec("CREATE OR REPLACE FUNCTION notify_pequod_listener() "
                 "RETURNS trigger AS "
                 "$BODY$ "
                 "BEGIN "
                 "PERFORM pg_notify('backend_queue', CAST (NEW.key AS text)); "
                 "RETURN NULL; "
                 "END; "
                 "$BODY$ "
                 "LANGUAGE plpgsql VOLATILE "
                 "COST 100"
        );
        // drop and create the trigger
        txn.exec("DROP TRIGGER IF EXISTS notify_cache ON cache");
        txn.exec("CREATE TRIGGER notify_cache "
                 "AFTER INSERT OR UPDATE ON cache "
                 "FOR EACH ROW "
                 "EXECUTE PROCEDURE notify_pequod_listener()"
        );
        txn.commit();
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        mandatory_assert(false);
    }

    monitor_db(server);
}

tamed void PostgresStore::monitor_db(pq::Server& server) {
    tvars {
        int32_t ret;
        PostgresListener listener(*(this->monitor_), "backend_queue", server);
    }

    while(true) {
        twait { tamer::at_fd_read(monitor_->sock(), make_event(ret)); }

        if (ret == 0) {
            // todo: i don't think this will block.
            // also, this will fire all notifications, which might take a while
            monitor_->get_notifs();
        }
        else
            mandatory_assert(false);
    }
}

PostgresListener::PostgresListener(pqxx::connection_base& conn, const std::string& channel,
                                   pq::Server& server)
    : pqxx::notification_receiver(conn, channel), server_(server) {
}

void PostgresListener::operator()(const std::string& payload, int32_t) {
    std::cerr << "Notification: " << payload << std::endl;

    Json j;
    j.parse(payload);
    assert(j && j.is_o());

    int32_t op = j["op"].as_i();
    switch(op) {
      case pg_update:
          server_.insert(j["key"].as_s(), j["value"].as_s());
          break;

      case pg_delete:
          server_.erase(j["key"].as_s());
          break;

      default:
          mandatory_assert(false && "Unknown DB operation.");
          break;
    }
}

#endif
