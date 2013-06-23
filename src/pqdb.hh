#include "string.hh"
#include <db_cxx.h>
#include <iostream>

#define VALUE_SIZE 199 // TODO: something less lame here...

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
	
		int put(String, String);
		String get(String);

	private:
		std::string envHome, dbName;
		DbEnv *pqdbEnv;
		Db *dbh;
};

int Pqdb::put(String key, String val){
  	
	DbTxn *txn = nullptr;
	int ret;
	char *key_str = new char[key.length()+1];
	char *val_str = new char[val.length()+1];
	strcpy(key_str, key.c_str());
	strcpy(val_str, val.c_str());

	Dbt k(key_str, strlen(key_str) + 1);
	Dbt v(val_str, strlen(val_str) + 1);

	ret = pqdbEnv->txn_begin(NULL, &txn, 0);
  if (ret != 0) {
  	pqdbEnv->err(ret, "Transaction begin failed.");
		goto done;
	}

  ret = dbh->put(txn, &k, &v, 0);
  if (ret != 0) {
  	pqdbEnv->err(ret, "Database put failed.");
  	txn->abort();
		goto done;
  }

  ret = txn->commit(0);
  if (ret != 0) {
  	pqdbEnv->err(ret, "Transaction commit failed.");
  }

done:
	delete[] key_str;
	delete[] val_str;
	return ret;

}

String Pqdb::get(String k){

	// since this is always a point get we shouldn't need a transaction
	char *key_str = new char[k.length()+1];
	strcpy(key_str, k.c_str());
//	char *value;

	Dbt key, val;

	key.set_data(key_str);
	key.set_size(strlen(key_str) + 1);

//	val.set_data(value);
	val.set_flags(DB_DBT_MALLOC);

	dbh->get(NULL, &key, &val, 0);

	delete[] key_str;
	return String( (char *) val.get_data());

}
