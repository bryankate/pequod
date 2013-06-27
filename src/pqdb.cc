#include "pqdb.hh"

#if HAVE_DB_CXX_H
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

int32_t Pqdb::put(Str key, String val){

    DbTxn *txn = nullptr;
    int32_t ret;

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

String Pqdb::get(Str k){

    // since this is always a point get we shouldn't need a transaction
    Dbt key, val;

    key.set_data(k.mutable_data());
    key.set_size(k.length() + 1);

    val.set_flags(DB_DBT_MALLOC);

    dbh_->get(NULL, &key, &val, 0);

    return String((char*)val.get_data());
}
#endif
