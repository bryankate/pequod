#include "pqdb.hh"
#include "pqdbthread.hh"

#if HAVE_DB_CXX_H
void Pqdb::init(uint32_t env_flags, uint32_t db_flags) {
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
                << env_home_ << std::endl
                << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    } catch(std::exception &e) {
        std::cerr << "Error opening database or environment: "
                << env_home_ << std::endl
                << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
        exit( -1 );
    }
}

int32_t Pqdb::put(Str key, Str val){

    int32_t ret;

    Dbt k(key.mutable_data(), key.length() + 1);
    Dbt v(val.mutable_data(), val.length() + 1);

    ret = dbh_->put(NULL, &k, &v, 0);
    if (ret != 0) {
        pqdb_env_->err(ret, "Database put failed.");
        return ret;
    }

    return ret;
}

String Pqdb::get(Str k){

    Dbt key, val;
    int32_t ret;

    key.set_data(k.mutable_data());
    key.set_size(k.length() + 1);

    val.set_flags(DB_DBT_MALLOC);

    ret = dbh_->get(NULL, &key, &val, 0);
    if (ret != 0) {
        pqdb_env_->err(ret, "Database get failed.");
        std::cerr << "this is error..bad get." << std::endl;
    }

    return String((char*)val.get_data(), val.get_size());
}

void Pqdb::scan(Str first, Str last, pq::ResultSet& results){
    Dbc *db_cursor;
    // start a new cursor 
    try{  
        dbh_->cursor(NULL, &db_cursor, Pqdb::cursor_flags_);
    } catch(DbException &e) {
        std::cerr << "Scan Error: opening database cursor failed: "
                << env_home_ << std::endl
                << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
    } catch(std::exception &e) {
        std::cerr << "Scan Error: opening database cursor failed: "
                << env_home_ << std::endl
                << db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
    }
    
    Dbt key(first.mutable_data(), first.length());
    Dbt end_key(last.mutable_data(), last.length());
    Dbt value = Dbt();
    value.set_flags(DB_DBT_MALLOC);

    db_cursor->get(&end_key, &value, DB_SET_RANGE);
    String end((char*) end_key.get_data(), end_key.get_size());
    db_cursor->get(&key, &value, DB_SET_RANGE); //over write the end value since we only care what the key is

    String k((char*) key.get_data(), key.get_size());
    String v((char*) value.get_data(), value.get_size());
    while (k < end) {
        results.add(k, v);
        db_cursor->get(&key, &value, DB_NEXT);
        k = String((char*) key.get_data(), key.get_size());
        v = String((char*) value.get_data(), value.get_size());
    }
}

#endif

