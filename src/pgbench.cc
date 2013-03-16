#include "pgclient.hh"
#include "time.hh"

using namespace std;

int main(int argv, char** argc) {
    (void)argv;
    (void)argc;
#if HAVE_POSTGRESQL_LIBPQ_FE_H
    pq::PostgresClient pg;
    uint32_t n = 200000;
    double start = tstamp();
    pg.bench_prepared(n);
    double tm = (tstamp() - start) / 1000000;
    cout << "Prepared statement: " << tm << " Queries: " << n << " QPS: " << n/tm << endl;
    return 0;
    start = tstamp();
    pg.bench_params(n);
    tm = (tstamp() - start) / 1000000;
    cout << "Parametrized statement: " << tm << " Queries: " << n << " QPS: " << n/tm << endl;

    start = tstamp();
    pg.bench_text(n);
    tm = (tstamp() - start) / 1000000;
    cout << "Text: " << tm << " Queries: " << n << " QPS: " << n/tm << endl;
#endif
    return 0;
}

