#include "pqdb.hh"
#include "pqdbthread.hh"


namespace pq {

void ReadOperation::operator()(PersistentStore* ps){
    ps->scan(first_key_, last_key_, rs_);
    tev_();
}

void WriteOperation::operator()(PersistentStore* ps){
    ps->put(key_,value_);
}

void ResultSet::add(StringPair sp) {
    results_.push_back(sp);
}

void ResultSet::add(String k, String v) {
    add(std::make_pair(k,v));
}

void BackendDatabaseThread::enqueue(DatabaseOperation dbo) {
    boost::mutex::scoped_lock lock( mu_ ); 
    pending_operations_.push(dbo);
    its_time_to_.notify_one();
}

void BackendDatabaseThread::run() {
    for(;;){
        while (!pending_operations_.empty()){
            {
                boost::mutex::scoped_lock lock(mu_);
                pending_operations_.front()(dbh_);
                pending_operations_.pop();
            } // scope this lock so the writer gets a 
              // chance to write even if there is work left on the queue 
        }
        boost::mutex::scoped_lock lock(mu_);
        its_time_to_.wait(lock);
    }
}

}
