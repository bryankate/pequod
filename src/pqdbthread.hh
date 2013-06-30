#ifndef PEQUOD_DATABASE_THREAD_HH
#define PEQUOD_DATABASE_THREAD_HH

#include <tamer/tamer.hh>
#include <boost/thread.hpp>
#include <queue>
#include "string.hh"

class PersistentStore;

namespace pq {

struct StringPair;

class ResultSet {
  public:
    int add(String k, String v);
    int add(StringPair sp);
    inline std::vector<StringPair> results();
  private:
    std::vector<StringPair> results_;
};

class DatabaseOperation {
  public:
    virtual ~DatabaseOperation();

    virtual void operator()(PersistentStore*);
};

class ReadOperation : public pq::DatabaseOperation {
  public:
    void operator()(PersistentStore*);
    ReadOperation(ResultSet& rsr) : rs_(rsr) {};

  private:
    ResultSet& rs_;
    tamer::event<> tev_;
    String first_key_;
    String last_key_;
};

class WriteOperation : public pq::DatabaseOperation {
  public:
    void operator()(PersistentStore*);

  private:
    String key_;
    String value_;
};

class BackendDatabaseThread {
  public:
    void enqueue(DatabaseOperation dbo);
    void run();

  private:
    boost::thread dbworker_;
    boost::mutex mu_;
    boost::condition_variable its_time_to_;
    std::queue<DatabaseOperation> pending_operations_;
    PersistentStore* dbh_;
};

inline std::vector<StringPair> ResultSet::results(){
    return results_;
}

}
#endif
