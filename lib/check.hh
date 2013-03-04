#ifndef TEST_UTIL_HH_
#define TEST_UTIL_HH_

#include <iostream>
#include <assert.h>
#include <string>
#include "compiler.hh"

template <typename T>
inline void check_true(const char* file, int line,
                       T test, const std::string& msg = "") {
    if (!test) {
        std::cerr << file << ":" << line << ": Check failed\n";
        if (!msg.empty())
            std::cerr << "\t" << msg << std::endl;
        mandatory_assert(0);
    }
}

template <typename T1, typename T2>
inline void check_eq(const char* file, int line,
                     const T1& actual, const T2& expected,
                     const std::string& msg = "") {
    if (!(expected == actual)) {
        std::cerr << file << ":" << line << ": Check failed\n";
        if (!msg.empty())
            std::cerr << "\t" << msg << std::endl;
        std::cerr <<   "\tActual:   " << actual
                  << "\n\tExpected: " << expected << std::endl;
        mandatory_assert(0);
    }
}

#define CHECK_TRUE(test) check_true(__FILE__, __LINE__, (test), #test)
#define CHECK_EQ(actual, expected, ...) check_eq(__FILE__, __LINE__, (actual), (expected), #actual " == " #expected __VA_ARGS__)

#endif
