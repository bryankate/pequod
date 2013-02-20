#ifndef PQTWITTER_HH
#define PQTWITTER_HH 1
#include <boost/random.hpp>
#include <utility>
#include <vector>
#include <iostream>
#include <stdint.h>
#include "json.hh"
namespace pq {

class TwitterPopulator {
  public:
    typedef boost::mt19937 generator_type;
    typedef boost::random_number_generator<boost::mt19937> rng_type;

    TwitterPopulator(const Json& param);

    inline uint32_t nusers() const;
    inline void set_nusers(uint32_t n);

    void create_subscriptions(generator_type& gen);
    void print_subscription_statistics(std::ostream& stream) const;
    inline const std::vector<std::pair<uint32_t, uint32_t> >& subscriptions() const;

    inline std::pair<uint64_t, Str> random_post(generator_type& gen) const;

  private:
    uint32_t nusers_;
    uint32_t min_followers_;
    uint32_t min_subs_;
    uint32_t max_subs_;
    uint32_t max_followers_;
    double shape_;

    std::vector<std::pair<uint32_t, uint32_t> > subs_;

    static const char tweet_data[];

    TwitterPopulator(const TwitterPopulator&) = delete;
    TwitterPopulator& operator=(const TwitterPopulator&) = delete;

    uint32_t* subscribe_probabilities(generator_type& gen);
};

inline uint32_t TwitterPopulator::nusers() const {
    return nusers_;
}

inline void TwitterPopulator::set_nusers(uint32_t n) {
    nusers_ = n;
}

inline const std::vector<std::pair<uint32_t, uint32_t> >& TwitterPopulator::subscriptions() const {
    return subs_;
}

inline std::pair<uint64_t, Str> TwitterPopulator::random_post(generator_type& gen) const {
    return std::make_pair(1000000000ULL - gen() % 65536, Str(tweet_data, 140 - gen() % 140));
}

} // namespace pq
#endif
