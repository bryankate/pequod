#ifndef PEQUOD_DATABASE_HH
#define PEQUOD_DATABASE_HH
#include "str.hh"
#include "string.hh"

namespace pq {

class PersistentStore {
  public:
    virtual ~PersistentStore() { }

    virtual int32_t put(Str key, String value) = 0;
    virtual String get(Str key) = 0;
};

}


#if HAVE_DB_CXX_H
#include <db_cxx.h>
#include <iostream>

class Pqdb : public pq::PersistentStore {

  public:

    Pqdb(std::string eH = "./db/localEnv",
         std::string dbN = "pequod.db",
         uint32_t e_flags = Pqdb::env_flags_, 
         uint32_t d_flags = Pqdb::db_flags_)
    : env_home_(eH), db_name_(dbN), pqdb_env_(new DbEnv(0))
    {
        init(e_flags, d_flags);
    }

    ~Pqdb() {
        try {
            if (dbh_ != NULL) {
                dbh_->close(0);
            }
            pqdb_env_->close(0);
            delete dbh_;
            delete pqdb_env_;
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

    void init(uint32_t, uint32_t);
    virtual int32_t put(Str, String);
    virtual String get(Str);

  private:
    static const uint32_t env_flags_ = DB_CREATE | DB_INIT_TXN | DB_INIT_MPOOL;
    static const uint32_t db_flags_ = DB_CREATE | DB_AUTO_COMMIT;
    std::string env_home_, db_name_;
    DbEnv *pqdb_env_;
    Db *dbh_;
};
#endif

#endif
