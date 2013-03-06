#ifndef PQFACEBOOK_HH
#define PQFACEBOOK_HH 1
#include "pqserver.hh"
#include <boost/random.hpp>
#include <boost/unordered_map.hpp>
#include <utility>
#include <vector>
#include <iostream>
#include <stdint.h>
#include "json.hh"

#define ID_WIDTH 7

namespace pq {

class FacebookPopulator {
  public:
    typedef boost::mt19937 generator_type;
    typedef boost::random_number_generator<boost::mt19937> rng_type;

    FacebookPopulator(const Json& param);

    inline uint32_t nusers() const;
    inline uint32_t npages() const;
    inline void set_nusers(uint32_t n);
    inline bool push() const;

    void populate_likes(generator_type);
    void generate_friends(generator_type);
    void report_counts(std::ostream&);

    inline boost::unordered_map<String, uint32_t> get_base_data();
    uint32_t* get_visit_list();

  private:
    uint32_t nusers_;
    uint32_t npages_;
    bool push_;
    uint32_t min_friends_;
    uint32_t max_friends_;
    uint32_t min_page_likes_;
    uint32_t max_page_likes_;
    double shape_;
    boost::unordered_map<String, uint32_t> localstore_;

    FacebookPopulator(const FacebookPopulator&) = delete;
    FacebookPopulator& operator=(const FacebookPopulator&) = delete;

    uint32_t* subscribe_probabilities(generator_type& gen);
};

inline boost::unordered_map<String, uint32_t> FacebookPopulator::get_base_data(){
    return localstore_;
}

inline uint32_t FacebookPopulator::npages() const {
    return npages_;
}

inline uint32_t FacebookPopulator::nusers() const {
    return nusers_;
}

inline void FacebookPopulator::set_nusers(uint32_t n) {
    nusers_ = n;
}

inline bool FacebookPopulator::push() const {
    return push_;
}


void facebook_like(Server& server, FacebookPopulator& fp,
                  uint32_t u, uint32_t p, Str value);
void facebook_populate(Server& server, FacebookPopulator& fp);
void facebook_run(Server& server, FacebookPopulator& fp);

} // namespace pq



#endif
