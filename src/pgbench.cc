#include "pgclient.hh"
#include "time.hh"

using namespace std;

int main(int argv, char** argc) {
    (void)argv;
    (void)argc;
#if HAVE_POSTGRESQL_LIBPQ_FE_H
    pq::PostgresClient pg;
    double start = tstamp();
    uint32_t n = 200000;
    pg.bench(n);
    double tm = (tstamp() - start) / 1000000;
    cout << "Time: " << tm << " Queries: " << n << " QPS: " << n/tm << endl;
#endif
    return 0;
}

