#ifndef PGCLIENT_HH
#define PGCLIENT_HH
#include <postgresql/libpq-fe.h>
#include "str.hh"
#include "string.hh"

namespace pq {
class PostgresClient {
  public:
    PostgresClient() {
        // XXX: Parametrize for twitter, facebook, etc.
        conn_ = PQconnectdb("dbname=hn");
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
            exit(0);
        }
        return res;
    }

    void insert(const char* query) {
        PGresult* res = PQexec(conn_, query);        
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            printf("Problem with insert!\n");
            std::cout << PQresultErrorMessage(res) << "\n";
            exit(0);
        }
        PQclear(res);
    }

    void done(PGresult *res) {
        PQclear(res);
    }

  private:
    PGconn* conn_;
};
}
#endif
