#ifndef PQ_PERSISTENT_HH
#define PQ_PERSISTENT_HH
#include "str.hh"
#include "string.hh"
#include <tamer/tamer.hh>
#include <boost/thread.hpp>
#include <queue>

namespace pq {

typedef std::pair<String,String> StringPair;

class ResultSet {
  public:
    inline void add(Str k, Str v);
    inline const std::vector<StringPair>& results() const;
  private:
    std::vector<StringPair> results_;
};

class PersistentStore {
  public:
    virtual ~PersistentStore() { }

    virtual void scan(Str first, Str last, ResultSet& results) = 0;
    virtual int32_t put(Str key, Str value) = 0;
    virtual String get(Str key) = 0;
};

class PersistentOp {
  public:
    virtual ~PersistentOp() { }
    virtual void operator()(PersistentStore*) = 0;
};

class PersistentRead : public PersistentOp {
  public:
    PersistentRead(Str first, Str last, ResultSet& rs, tamer::event<> ev);
    virtual void operator()(PersistentStore*);

  private:
    ResultSet& rs_;
    tamer::event<> tev_;
    String first_;
    String last_;
};

class PersistentWrite : public PersistentOp {
  public:
    PersistentWrite(Str key, Str value);
    virtual void operator()(PersistentStore*);

  private:
    String key_;
    String value_;
};

class PersistentStoreThread {
  public:
    PersistentStoreThread(PersistentStore* store);
    ~PersistentStoreThread();

    void enqueue(PersistentOp* op);
    void run();

  private:
    PersistentStore* store_;
    boost::thread worker_;
    boost::mutex mu_;
    boost::condition_variable cond_;
    std::queue<PersistentOp*> pending_;
    bool running_;
};

}

#if HAVE_DB_CXX_H
#include <db_cxx.h>

class Pqdb : public pq::PersistentStore {

  public:
    Pqdb(std::string eH = "./db/localEnv",
         std::string dbN = "pequod.db",
         uint32_t e_flags = Pqdb::env_flags_,
         uint32_t d_flags = Pqdb::db_flags_);
    ~Pqdb();

    virtual void scan(Str, Str, pq::ResultSet&);
    void init(uint32_t, uint32_t);
    virtual int32_t put(Str, Str);
    virtual String get(Str);
    bool strings_equal(String, String);

  private:
    static const uint32_t env_flags_ = DB_CREATE | DB_INIT_MPOOL;
    static const uint32_t db_flags_ = DB_CREATE;
    static const uint32_t cursor_flags_ = DB_CURSOR_BULK;
    std::string env_home_, db_name_;
    DbEnv *pqdb_env_;
    Db *dbh_;
};

#endif

#endif
