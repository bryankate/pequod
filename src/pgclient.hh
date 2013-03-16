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

    void bench_text(uint32_t n) {
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

    void bench_params(uint32_t n) {
        boost::mt19937 gen;
        boost::random_number_generator<boost::mt19937> rng(gen);
        const char *paramValues[1];
        int paramLengths[1];
        int paramFormats[1];
        paramFormats[0] = 1;
        for (uint32_t i = 0; i < n; i++) {
            uint32_t cid = htonl(rng(1000000));
            paramValues[0] = (char *)&cid;
            paramLengths[0] = sizeof(cid);
            PGresult* res = PQexecParams(conn_, "SELECT * FROM comments WHERE cid = $1::int4", 
                                         1, NULL, 
                                         paramValues, 
                                         paramLengths, 
                                         paramFormats, 
                                         1);  // binary or not
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::cout << "Problem: " << PQresultErrorMessage(res) << "\n";
                exit(1);
            }
            for (int i = 0; i < PQntuples(res); i++) {
                for (int j = 0; j < PQnfields(res); j++) {
                    //std::cout << PQgetvalue(res, i, j) << "\t";
                }
                //std::cout << "\n";
            }            
        }
    }

    void bench_prepared(uint32_t n) {
        boost::mt19937 gen;
        boost::random_number_generator<boost::mt19937> rng(gen);
        const char *paramValues[1];
        int paramLengths[1];
        int paramFormats[1];
        paramFormats[0] = 1;
        PGresult* res = PQprepare(conn_, "bench", 
                                  "SELECT * FROM comments WHERE cid = $1::int4", 
                                  1,
                                  NULL);
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cout << "Problem preparing: " << PQresultErrorMessage(res) << "\n";
            exit(1);
        }
        for (uint32_t i = 0; i < n; i++) {
            uint32_t cid = htonl(rng(1000000));
            paramValues[0] = (char *)&cid;
            paramLengths[0] = sizeof(cid);
            res = PQexecPrepared(conn_, "bench",
                                 1,
                                 paramValues,
                                 paramLengths, 
                                 paramFormats, 
                                 1);  // binary or not
            if (PQresultStatus(res) != PGRES_TUPLES_OK) {
                std::cout << "Problem: " << PQresultErrorMessage(res) << "\n";
                exit(1);
            }
            for (int i = 0; i < PQntuples(res); i++) {
                for (int j = 0; j < PQnfields(res); j++) {
                    //std::cout << PQgetvalue(res, i, j) << "\t";
                }
                //std::cout << "\n";
            }
            PQclear(res);
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
