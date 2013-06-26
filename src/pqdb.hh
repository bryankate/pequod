#ifndef PEQUOD_DATABASE_HH
#define PEQUOD_DATABASE_HH
#include "str.hh"
#include <db_cxx.h>
#include <iostream>

class Pqdb {

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
  
    inline void init(uint32_t, uint32_t);
    inline int put(Str, Str);
    inline Str get(Str);

  private:
		static const uint32_t env_flags_ = DB_CREATE | DB_INIT_TXN | DB_INIT_MPOOL;
		static const uint32_t db_flags_ = DB_CREATE | DB_AUTO_COMMIT;
    std::string env_home_, db_name_;
    DbEnv *pqdb_env_;
    Db *dbh_;
};

void Pqdb::init(uint32_t env_flags, uint32_t db_flags){
  try {
    pqdb_env_->open(env_home_.c_str(), env_flags, 0);
    dbh_ = new Db(pqdb_env_, 0);
    dbh_->open(NULL,
              db_name_.c_str(),
              NULL,
              DB_BTREE,
              db_flags,
              0);
  } catch(DbException &e) {
    std::cerr << "Error opening database or environment: "
              << env_home_ << std::endl;
    std::cerr << e.what() << std::endl;
    exit( -1 );
  } catch(std::exception &e) {
    std::cerr << "Error opening database or environment: "
              << env_home_ << std::endl;
    std::cerr << e.what() << std::endl;
    exit( -1 );
  } 
}

int Pqdb::put(Str key, Str val){

  DbTxn *txn = nullptr;
  int ret;

  Dbt k(key.mutable_data(), key.length() + 1);
  Dbt v(val.mutable_data(), val.length() + 1);

  ret = pqdb_env_->txn_begin(NULL, &txn, 0);
  if (ret != 0) {
    pqdb_env_->err(ret, "Transaction begin failed.");
    return ret;
  }

  ret = dbh_->put(txn, &k, &v, 0);
  if (ret != 0) {
    pqdb_env_->err(ret, "Database put failed.");
    txn->abort();
    return ret;
  }

  ret = txn->commit(0);
  if (ret != 0) {
    pqdb_env_->err(ret, "Transaction commit failed.");
  }

  return ret;

}

Str Pqdb::get(Str k){

  // since this is always a point get we shouldn't need a transaction
  Dbt key, val;

  key.set_data(k.mutable_data());
  key.set_size(k.length() + 1);

  val.set_flags(DB_DBT_MALLOC);

  dbh_->get(NULL, &key, &val, 0);

  return Str((char*)val.get_data());

}
#endif
