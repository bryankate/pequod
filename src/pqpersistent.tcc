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

PersistentErase::PersistentErase(Str key) : key_(key) {
}

void PersistentErase::operator()(PersistentStore* ps) {
    ps->erase(key_);
    delete this;
}

PersistentFlush::PersistentFlush(std::atomic<bool>& waiting, boost::condition_variable& cond)
    : waiting_(waiting), cond_(cond) {
}

void PersistentFlush::operator()(PersistentStore*) {
    waiting_ = false;
    cond_.notify_all();
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

void PersistentStoreThread::flush() {
    std::atomic<bool> waiting(true);
    boost::mutex mu;
    boost::condition_variable cond;

    pending_.enqueue(new PersistentFlush(waiting, cond));

    while(waiting) {
        boost::mutex::scoped_lock lock(mu);
        cond.timed_wait(lock, boost::posix_time::milliseconds(5));
    }
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


#if HAVE_PQXX_PQXX
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

void PostgresStore::erase(Str key) {
    try{
        pqxx::work txn(*dbh_);
        auto k = txn.quote(std::string(key.mutable_data(), key.length()));
        txn.exec("DELETE FROM cache WHERE key=" + k);
        txn.commit();
    } catch (const std::exception &e){
        std::cerr << e.what() << std::endl;
        mandatory_assert(false);
    }
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
    return "dbname=" + dbname_ + " host=" + host_ + " port=" + String(port_);
}

void PostgresStore::run_monitor(pq::Server& server) {
    monitor_ = new pqxx::connection(connection_string().c_str());

    try {
        pqxx::work txn(*monitor_);

        // create the notify trigger functions
        txn.exec(
        "CREATE OR REPLACE FUNCTION notify_upsert_listener() "
        "RETURNS trigger AS "
        "$BODY$ "
        "BEGIN "
        "PERFORM pg_notify('backend_queue', '{ \"op\":0, \"key\":\"' || CAST (NEW.key AS TEXT) || '\", \"value\":\"' || CAST (NEW.value AS TEXT) || '\" }'); "
        "RETURN NULL; "
        "END; "
        "$BODY$ "
        "LANGUAGE plpgsql VOLATILE "
        "COST 100"
        );

        txn.exec(
        "CREATE OR REPLACE FUNCTION notify_delete_listener() "
        "RETURNS trigger AS "
        "$BODY$ "
        "BEGIN "
        "PERFORM pg_notify('backend_queue', '{ \"op\":1, \"key\":\"' || CAST (OLD.key AS TEXT) || '\" }'); "
        "RETURN NULL; "
        "END; "
        "$BODY$ "
        "LANGUAGE plpgsql VOLATILE "
        "COST 100"
        );

        // drop and create the triggers
        txn.exec("DROP TRIGGER IF EXISTS notify_upsert_cache ON cache");
        txn.exec("DROP TRIGGER IF EXISTS notify_delete_cache ON cache");

        txn.exec(
        "CREATE TRIGGER notify_upsert_cache "
        "AFTER INSERT OR UPDATE ON cache "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE notify_upsert_listener()"
        );

        txn.exec(
        "CREATE TRIGGER notify_delete_cache "
        "AFTER DELETE ON cache "
        "FOR EACH ROW "
        "EXECUTE PROCEDURE notify_delete_listener()"
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
    //std::cerr << "Notification: " << payload << std::endl;

    Json j;
    j.assign_parse(payload);
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
