#include "pqpersistent.hh"

namespace pq {

PersistentRead::PersistentRead(Str first, Str last,
                               PersistentStore::ResultSet& rs, tamer::event<> ev)
    : rs_(rs), tev_(ev), first_(first), last_(last) {
}

void PersistentRead::operator()(PersistentStore* ps){
    ps->scan(first_, last_, rs_);
    tev_();
}

PersistentWrite::PersistentWrite(Str key, Str value)
    : key_(key), value_(value) {
}

void PersistentWrite::operator()(PersistentStore* ps){
    ps->put(key_, value_);
    delete this;
}

PersistentStoreThread::PersistentStoreThread(PersistentStore* store)
    : store_(store), worker_(&PersistentStoreThread::run, this), running_(true) {
}

PersistentStoreThread::~PersistentStoreThread() {
    boost::mutex::scoped_lock lock(mu_);
    running_ = false;
    cond_.notify_all();
    delete store_;
}

void PersistentStoreThread::enqueue(PersistentOp* op) {
    boost::mutex::scoped_lock lock(mu_);
    pending_.push(op);
    cond_.notify_one();
}

void PersistentStoreThread::run() {
    PersistentOp *op;
    while(running_) {
        while (!pending_.empty()){
            {
                boost::mutex::scoped_lock lock(mu_);
                op = pending_.front();
                (*op)(store_);
                pending_.pop();
            } // scope this lock so the writer gets a
              // chance to write even if there is work left on the queue
        }
        boost::mutex::scoped_lock lock(mu_);
        if (running_)
            cond_.wait(lock);
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

    Dbt k(key.mutable_data(), key.length() + 1);
    Dbt v(val.mutable_data(), val.length() + 1);

    ret = dbh_->put(NULL, &k, &v, 0);
    if (ret != 0) {
        env_->err(ret, "Database put failed.");
        return ret;
    }

    return ret;
}

String BerkeleyDBStore::get(Str k){

    Dbt key, val;
    int32_t ret;

    key.set_data(k.mutable_data());
    key.set_size(k.length() + 1);

    val.set_flags(DB_DBT_MALLOC);

    ret = dbh_->get(NULL, &key, &val, 0);
    if (ret != 0) {
        env_->err(ret, "Database get failed.");
        std::cerr << "this is error..bad get." << std::endl;
    }

    return String((char*)val.get_data(), val.get_size());
}

void BerkeleyDBStore::scan(Str first, Str last, pq::PersistentStore::ResultSet& results){
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

    db_cursor->get(&end_key, &value, DB_SET_RANGE);
    String end((char*) end_key.get_data(), end_key.get_size());
    db_cursor->get(&key, &value, DB_SET_RANGE); //over write the end value since we only care what the key is

    String k((char*) key.get_data(), key.get_size());
    String v((char*) value.get_data(), value.get_size());
    while (k < end) {
        results.push_back(PersistentStore::Result(k, v));
        db_cursor->get(&key, &value, DB_NEXT);
        k = String((char*) key.get_data(), key.get_size());
        v = String((char*) value.get_data(), value.get_size());
    }
}

#endif
