#ifndef PEQUOD_TIME_HH
#define PEQUOD_TIME_HH
#include <sys/time.h>
#include "compiler.hh"

extern uint64_t tstamp_adjustment;

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

inline uint64_t tous(double seconds) {
    mandatory_assert(seconds >= 0);
    return (uint64_t) (seconds * 1000000);
}

inline double fromus(uint64_t t) {
    return (double) t / 1000000.;
}

inline uint64_t tv2us(const timeval& tv) {
    return (uint64_t) tv.tv_sec * 1000000 + tv.tv_usec;
}

inline uint64_t tstamp() {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return tv2us(tv);
}

#endif
