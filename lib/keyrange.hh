#ifndef KEYRANGE_HH
#define KEYRANGE_HH 1
#include "string.hh"
#include <string>

namespace pq {

class keyrange {
  public:
    String key;
    int owner;

    keyrange(const String &key, int owner)
	: key(key), owner(owner) {
    }
    template <typename T>
    keyrange(const String_base<T> &key, int owner)
        : key(key), owner(owner) {
    }
};

inline String next_key(String key) {
    key += '\0';
    return key;
}

inline std::string next_key(std::string key) {
    key.append(1, '\0');
    return key;
}

}
#endif
