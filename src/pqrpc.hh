#ifndef PEQUOD_PQRPC_HH
#define PEQUOD_PQRPC_HH

enum {
    pq_get = 1,
    pq_insert = 2,
    pq_erase = 3,
    pq_count = 4,
    pq_scan = 5,
    pq_subscribe = 6,
    pq_unsubscribe = 7,
    pq_invalidate = 8,
    pq_add_join = 9,
    // order matters
    pq_stats = 10,
    pq_control = 11
};

enum {
    pq_ok = 0,
    pq_fail = -1
};

#endif
