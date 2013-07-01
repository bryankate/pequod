#ifndef PEQUOD_DATABASE_THREAD_HH
#define PEQUOD_DATABASE_THREAD_HH

#include <tamer/tamer.hh>
#include <boost/thread.hpp>
#include <queue>
#include "string.hh"

class PersistentStore;

namespace pq {

typedef std::pair<String,String> StringPair;

class ResultSet {
  public:
    void add(String k, String v);
    void add(StringPair sp);
    inline std::vector<StringPair> results();
  private:
    std::vector<StringPair> results_;
};

class DatabaseOperation {
  public:
    DatabaseOperation() {}
    virtual ~DatabaseOperation() {}

    virtual void operator()(PersistentStore*) = 0;
};

class ReadOperation : public pq::DatabaseOperation {
  public:
    ReadOperation(ResultSet& rsr) : DatabaseOperation(), rs_(rsr) {};
    virtual void operator()(PersistentStore*);

  private:
    ResultSet& rs_;
    tamer::event<> tev_;
    String first_key_;
    String last_key_;
};

class WriteOperation : public pq::DatabaseOperation {
  public:
    WriteOperation() : DatabaseOperation() {}
    virtual void operator()(PersistentStore*);

  private:
    String key_;
    String value_;
};

class BackendDatabaseThread {
  public:
    void enqueue(DatabaseOperation* dbo);
    void run();

  private:
    boost::thread dbworker_;
    boost::mutex mu_;
    boost::condition_variable its_time_to_;
    std::queue<DatabaseOperation*> pending_operations_;
    PersistentStore* dbh_;
};

inline std::vector<StringPair> ResultSet::results(){
    return results_;
}

}
#endif
