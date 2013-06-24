#include "str.hh"
#include <db_cxx.h>
#include <iostream>

class Pqdb {

  public:

    Pqdb(uint32_t env_flags = DB_CREATE |
                              DB_INIT_TXN  | // Initialize transactions
                              DB_INIT_MPOOL, // maybe we should be the in memory cache?
         uint32_t db_flags = DB_CREATE |
                             DB_AUTO_COMMIT,
         std::string eH = "./db/localEnv",
         std::string dbN = "pequod.db")
         : envHome(eH), dbName(dbN), pqdbEnv(new DbEnv(0))
    {
      try {
        pqdbEnv->open(envHome.c_str(), env_flags, 0);
        dbh = new Db(pqdbEnv, 0);
        dbh->open(NULL,
                  dbName.c_str(),
                  NULL,
                  DB_BTREE,
                  db_flags,
                  0);
      } catch(DbException &e) {
        std::cerr << "Error opening database or environment: "
                  << envHome << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
      } catch(std::exception &e) {
        std::cerr << "Error opening database or environment: "
                  << envHome << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
      } 
    }

    ~Pqdb() {
      try {
        if (dbh != NULL) {
          dbh->close(0);
        }
        pqdbEnv->close(0);
        delete dbh;
        delete pqdbEnv;
      } catch(DbException &e) {
        std::cerr << "Error closing database environment: "
                  << envHome 
                  << " or database "
                  << dbName << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
      } catch(std::exception &e) {
        std::cerr << "Error closing database environment: "
                  << envHome 
                  << " or database "
                  << dbName << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
      } 
    }
  
    int put(Str, Str);
    Str get(Str);

  private:
    std::string envHome, dbName;
    DbEnv *pqdbEnv;
    Db *dbh;
};

int Pqdb::put(Str key, Str val){

  DbTxn *txn = nullptr;
  int ret;

  Dbt k(key.mutable_data(), key.length() + 1);
  Dbt v(val.mutable_data(), val.length() + 1);

  ret = pqdbEnv->txn_begin(NULL, &txn, 0);
  if (ret != 0) {
    pqdbEnv->err(ret, "Transaction begin failed.");
    return ret;
  }

  ret = dbh->put(txn, &k, &v, 0);
  if (ret != 0) {
    pqdbEnv->err(ret, "Database put failed.");
    txn->abort();
    return ret;
  }

  ret = txn->commit(0);
  if (ret != 0) {
    pqdbEnv->err(ret, "Transaction commit failed.");
  }

  return ret;

}

Str Pqdb::get(Str k){

  // since this is always a point get we shouldn't need a transaction
  Dbt key, val;

  key.set_data(k.mutable_data());
  key.set_size(k.length() + 1);

  val.set_flags(DB_DBT_MALLOC);

  dbh->get(NULL, &key, &val, 0);

  return Str((char*)val.get_data());

}
