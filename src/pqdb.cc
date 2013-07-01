#include "pqdb.hh"

namespace pq {

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

Pqdb::iterator& Pqdb::lower_bound(Str start){
    return new Pqdb::iterator(this, start);
}


void Pqdb::scan(Str first, Str last, ResultSet& results){
    Pqdb::iterator begin = lower_bound(first);
    Pqdb::iterator end = lower_bound(last);

    while (*begin != *end){
        results.add(*begin);        
        ++begin;
    }

}

#endif

}
