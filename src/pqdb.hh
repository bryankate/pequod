#ifndef PEQUOD_DATABASE_HH
#define PEQUOD_DATABASE_HH
#include "str.hh"
#include "string.hh"
#include <iterator>


namespace pq {

struct StringPair {
    String key;
    String value;
};

class PersistentStore {
  public:
    class iterator;
    virtual ~PersistentStore() { };

    virtual iterator& lower_bound(Str start) = 0; // how do i return any iterator
    virtual int32_t put(Str key, Str value) = 0;
    virtual String get(Str key) = 0;
};

class PersistentStore::iterator {

};

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

    class iterator;
    iterator& lower_bound(Str start);
    void init(uint32_t, uint32_t);
    virtual int32_t put(Str key, Str value);
    virtual String get(Str);

  private:
    static const uint32_t env_flags_ = DB_CREATE | DB_INIT_MPOOL;
    static const uint32_t db_flags_ = DB_CREATE;
    std::string env_home_, db_name_;
    DbEnv *pqdb_env_;
    Db *dbh_;

    friend class iterator;

};


class Pqdb::iterator : public std::iterator<std::forward_iterator_tag, Dbt>, public PersistentStore::iterator {
  public:
    inline iterator() = default;
    inline iterator(Pqdb* pqdb, Str start);

    inline StringPair operator*() const;

    inline bool operator==(const iterator& x) const;
    inline bool operator!=(const iterator& x) const;

    inline void operator++();

  private:
    Dbc* db_cursor_;
    Dbt* key_;
    Dbt* value_;
    Pqdb* pqdb_handle_;
    static const uint32_t cursor_flags = DB_CURSOR_BULK;

};

inline Pqdb::iterator::iterator(Pqdb* pqdb, Str start){
    pqdb_handle_ = pqdb;
    try{  
        pqdb_handle_->dbh_->cursor(NULL, &db_cursor_, Pqdb::iterator::cursor_flags);
    } catch(DbException &e) {
        std::cerr << "Error opening database cursor: "
                << pqdb_handle_->env_home_ << std::endl
                << pqdb_handle_->db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
    } catch(std::exception &e) {
        std::cerr << "Error opening database cursor: "
                << pqdb_handle_->env_home_ << std::endl
                << pqdb_handle_->db_name_ << std::endl;
        std::cerr << e.what() << std::endl;
    }
    
    key_ = new Dbt(start.mutable_data(), start.length());
    value_ = new Dbt();
    value_->set_flags(DB_DBT_MALLOC);

    db_cursor_->get(key_, value_, DB_SET_RANGE);

}

inline StringPair Pqdb::iterator::operator*() const {
    struct StringPair sp;
    sp.key = String((char*) key_->get_data(), key_->get_size());
    sp.value = String((char*)value_->get_data(), value_->get_size());
    return sp;
}

inline bool Pqdb::iterator::operator==(const iterator& x) const {
    if (this->key_->get_size() != x.key_->get_size())
        return false;
    char* mydata = (char*) this->key_->get_data();
    char* xdata = (char*) x.key_->get_data();
    int len = this->key_->get_size();
    while (--len >= 0)
        if (mydata[len] != xdata[len])
            return false;
    return  true;
}

inline bool Pqdb::iterator::operator!=(const iterator& x) const {
    return !(*this == x);
}

inline void Pqdb::iterator::operator++() {
    db_cursor_->get(key_, value_, DB_NEXT);
}

#endif

}

#endif
