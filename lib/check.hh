#ifndef TEST_UTIL_HH_
#define TEST_UTIL_HH_

#include <iostream>
#include <assert.h>
#include <string>
#include "compiler.hh"

template <typename T>
inline void CHECK_TRUE(const T yes, const std::string &msg = "") {
    if (!yes) {
        std::cerr <<   "Assertion failed" << std::endl;
        std::cerr << "Message:\n" << msg << std::endl;
        mandatory_assert(0);
    }
}

template <typename T>
inline void CHECK_FALSE(const T yes, const std::string &msg = "") {
    if (yes) {
        std::cerr <<   "Assertion failed" << std::endl;
        std::cerr << "Message:\n" << msg << std::endl;
        mandatory_assert(0);
    }
}

template <typename T1, typename T2>
inline void CHECK_EQ(const T1 &expected, const T2 &actual, const std::string &msg = "") {
    if (expected != actual) {
        std::cerr <<   "\tActual:   " << actual
                  << "\n\tExpected: " << expected << std::endl;
        std::cerr << "Message: " << msg << std::endl;
        mandatory_assert(0);
    }
}

template <typename T1, typename T2>
inline void CHECK_GT(const T1 &actual, const T2 &comp) {
    if (actual <= comp) {
        std::cerr <<   "\tActual:     " << actual
                  << "\n\tExpected: > " << comp << std::endl;
        mandatory_assert(0);
    }
}

#endif

