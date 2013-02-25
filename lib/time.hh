#ifndef PEQUOD_TIME_HH
#define PEQUOD_TIME_HH
#include <sys/time.h>

inline double to_real(timeval tv) {
    return tv.tv_sec + tv.tv_usec / (double) 1e6;
}

inline timeval operator+(timeval a, timeval b) {
    timeval c;
    timeradd(&a, &b, &c);
    return c;
}

inline timeval operator-(timeval a, timeval b) {
    timeval c;
    timersub(&a, &b, &c);
    return c;
}

#endif
