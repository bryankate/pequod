#ifndef PEQUOD_PQRPC_HH
#define PEQUOD_PQRPC_HH

// order matters!
enum {
    // single key operations
    pq_get = 1,
    pq_insert = 2,
    pq_erase = 3,
    pq_notify_insert = 4,
    pq_notify_erase = 5,
    pq_count = 6,

    // range operations
    pq_scan = 7,
    pq_subscribe = 8,
    pq_unsubscribe = 9,
    pq_invalidate = 10,

    // other
    pq_add_join = 11,
    pq_stats = 12,
    pq_control = 13
};

enum {
    pq_ok = 0,
    pq_fail = -1
};

#endif
