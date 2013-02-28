#ifndef PEQUOD_BASE_HH
#define PEQUOD_BASE_HH
#include <string.h>
#include "str.hh"
namespace pq {

template <typename T>
inline T table_name(const String_base<T>& key) {
    const char* x = (const char*) memchr(key.data(), '|', key.length());
    if (!x) {
        if (key.length() && key.back() == '}')
            x = key.end() - 1;
        else
            x = key.begin();
    }
    return static_cast<const T&>(key).fast_substring(key.data(), x);
}

template <typename T>
inline T table_name(const String_base<T>& key, const String_base<T>& key2) {
    String_base<T> t = table_name(key);
    if (t.length() && key2.length() > t.length()
        && memcmp(key2.data(), t.data(), t.length()) == 0
        && (key2[t.length()] | 1) == '}' /* matches | or } */)
        return t;
    else
        return static_cast<const T&>(key).fast_substring(key.data(), key.data());
}

} // namespace
#endif
