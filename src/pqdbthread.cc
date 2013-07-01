#include "pqdb.hh"
#include "pqdbthread.hh"


namespace pq {

ReadOperation::ReadOperation(Str first, Str last, ResultSet& rs, tamer::event<> ev)
    : rs_(rs), tev_(ev), first_key_(first), last_key_(last) {
}

void ReadOperation::operator()(PersistentStore* ps){
    ps->scan(first_key_, last_key_, rs_);
    tev_();
}

WriteOperation::WriteOperation(Str key, Str value)
    : key_(key), value_(value) {
}

void WriteOperation::operator()(PersistentStore* ps){
    ps->put(key_,value_);
    delete this;
}

void ResultSet::add(StringPair sp) {
    results_.push_back(sp);
}

void ResultSet::add(String k, String v) {
    add(std::make_pair(k,v));
}

BackendDatabaseThread::BackendDatabaseThread(PersistentStore* store)
    : dbh_(store), dbworker_(&BackendDatabaseThread::run, this), running_(true) {
}

BackendDatabaseThread::~BackendDatabaseThread() {
    boost::mutex::scoped_lock lock(mu_);
    running_ = false;
    its_time_to_.notify_all();
    delete dbh_;
}

void BackendDatabaseThread::enqueue(DatabaseOperation* dbo) {
    boost::mutex::scoped_lock lock( mu_ ); 
    pending_operations_.push(dbo);
    its_time_to_.notify_one();
}

void BackendDatabaseThread::run() {
    DatabaseOperation *dbo;
    while(running_) {
        while (!pending_operations_.empty()){
            {
                boost::mutex::scoped_lock lock(mu_);
                dbo = pending_operations_.front();
                (*dbo)(dbh_);
                pending_operations_.pop();
            } // scope this lock so the writer gets a 
              // chance to write even if there is work left on the queue 
        }
        boost::mutex::scoped_lock lock(mu_);
        if (running_)
            its_time_to_.wait(lock);
    }
}

}
