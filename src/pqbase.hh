#ifndef PEQUOD_BASE_HH
#define PEQUOD_BASE_HH
#include <string.h>
#include "string.hh"
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
    T t = table_name(key);
    if (t.length() && key2.length() > t.length()
        && memcmp(key2.data(), t.data(), t.length()) == 0
        && (key2[t.length()] | 1) == '}' /* matches | or } */)
        return t;
    else
        return static_cast<const T&>(key).fast_substring(key.data(), key.data());
}

extern const char unchanged_marker_data[];
extern const char erase_marker_data[];

inline String unchanged_marker() {
    return String::make_stable(unchanged_marker_data, 1);
}

inline bool is_unchanged_marker(const String& str) {
    return str.data() == unchanged_marker_data;
}

inline String erase_marker() {
    return String::make_stable(erase_marker_data, 1);
}

inline bool is_erase_marker(const String& str) {
    return str.data() == erase_marker_data;
}

} // namespace
#endif
