#ifndef BLOOM_HH
#define BLOOM_HH

#include "compiler.hh"
#include "MurmurHash3.h"
#include <vector>
#include <utility>


class BloomFilter {

  public:
    inline BloomFilter(uint32_t m, double err);
    inline BloomFilter(uint32_t m, uint32_t k);

    inline bool check(const char* buff, size_t len) const;
    inline bool add(const char* buff, size_t len);

  private:
    typedef std::pair<size_t, size_t> hash_t;

    uint32_t k_;
    std::vector<bool> bits_;

    inline hash_t hash(const char* buff, size_t len) const;
    inline bool check(const hash_t hash) const;
    inline bool add(const hash_t hash);
};


inline BloomFilter::BloomFilter(uint32_t m, uint32_t k) : k_(k), bits_(m) {
}

inline BloomFilter::BloomFilter(uint32_t m, double err) {

    if (m < 1 || !err)
        mandatory_assert(false && "Invalid BloomFilter params.");

    double ln2 = log(2);
    double bpe = -(log(err) / (ln2 * ln2));
    k_ = (int)ceil(bpe * ln2);
    bits_ = std::vector<bool>((int)bpe * m);
}

inline bool BloomFilter::check(const char* buff, size_t len) const {
    return check(hash(buff, len));
}

inline bool BloomFilter::check(const hash_t hash) const {
    uint32_t hits = 0;

    for (uint32_t i = 0; i < k_; ++i)
        if (bits_[(hash.first + i * hash.second) % bits_.size()])
            ++hits;

    return (hits == k_);
}

inline bool BloomFilter::add(const char* buff, size_t len) {
    return add(hash(buff, len));
}

inline bool BloomFilter::add(const hash_t hash) {
    uint32_t hits = 0;

    for (uint32_t i = 0; i < k_; ++i) {
        auto bit = bits_[(hash.first + i * hash.second) % bits_.size()];

        if (bit)
            ++hits;
        else
            bit = true;
    }

    return (hits != k_);
}

inline BloomFilter::hash_t BloomFilter::hash(const char* buff, size_t len) const {
    hash_t hash(0,0);

    MurmurHash3_x86_32(buff, len, 112181, &hash.first);
    MurmurHash3_x86_32(buff, len, hash.first, &hash.second);
    return hash;
}

#endif
