#ifndef PGCLIENT_HH
#define PGCLIENT_HH
#if HAVE_POSTGRESQL_LIBPQ_FE_H
#include <postgresql/libpq-fe.h>
#include <boost/random.hpp>
#include "str.hh"
#include "string.hh"

namespace pq {
class PostgresClient {
  public:
    PostgresClient() {
        // XXX: Parametrize for twitter, facebook, etc.
        conn_ = PQconnectdb("dbname=hn port=5477");
        mandatory_assert(conn_);
        mandatory_assert(PQstatus(conn_) != CONNECTION_BAD);
    }

    ~PostgresClient() {
        PQfinish(conn_);
    }

    PGresult *query(const char* query) {
        PGresult* res = PQexec(conn_, query);        
        if (PQresultStatus(res) != PGRES_TUPLES_OK) {
            printf("No data\n");
            std::cout << PQresultErrorMessage(res) << "\n";
            exit(0);
        }
        return res;
    }

    PGresult *insert(const char* query) {
        PGresult* res = PQexec(conn_, query);        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            printf("Problem with insert!\n");
            std::cout << PQresultErrorMessage(res) << "\n";
            exit(0);
        }
        return res;
    }

    void bench(uint32_t n) {
        boost::mt19937 gen;
        boost::random_number_generator<boost::mt19937> rng(gen);
        for (uint32_t i = 0; i < n; i++) {
            char buf[128];
            uint32_t cid = rng(1000000);
            sprintf(buf,"SELECT * FROM comments WHERE cid = %d", cid); 
            PGresult* res = query(buf);
            for (int i = 0; i < PQntuples(res); i++) {
                for (int j = 0; j < PQnfields(res); j++) {
                    //   std::cout << PQgetvalue(res, i, j) << "\t";
                //std::cout << "\n";
                }
            }            
        }
    }

    void done(PGresult *res) {
        PQclear(res);
    }

  private:
    PGconn* conn_;
};
}
#endif
#endif
