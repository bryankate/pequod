#include "pqdb.hh"
#include "pqdbthread.hh"


namespace pq {

void ReadOperation::operator()(PersistentStore* ps){
    PersistentStore::iterator begin = *(ps->lower_bound(first_key_));
    PersistentStore::iterator end = *(ps->lower_bound(last_key_));
    while (begin != end){
        rs_->add(*begin);
        ++begin;
    }
    tev_();
}

void WriteOperation::operator()(PersistentStore* ps){
    ps->put(key_,value_);
}

void ResultSet::add(StringPair sp) {
    results_->push_back(sp);
}

void ResultSet::add(String k, String v) {
    StringPair sp;
    sp.key = k;
    sp.value = v;
    add(sp);
}

void BackendDatabaseThread::enqueue(DatabaseOperation dbo) {
    boost::mutex::scoped_lock lock( mu_ ); 
    pending_operations_.push_back(dbo);
    its_time_to_.notify_one();
}

void BackendDatabaseThread::run() {
    DatabaseOperation dbo;
    for(;;){
        while (!pending_operations_.empty()){
            {
                boost::scoped_lock lock(mu_);
                dbo = pending_operations_.pop_front();
                dbo();
            } // scope this lock so the writer gets a 
              // chance to write even if there is work left on the queue 
        }
        boost::scoped_lock lock(mu_);
        its_time_to_.wait(lock);
    }
}

}
