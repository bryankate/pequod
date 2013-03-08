#ifndef HNPOPULATOR_HH
#define HNPOPULATOR_HH
#include <utility>
#include <vector>
#include <map>
#include <set>
#include <iostream>
#include <stdint.h>
#include "json.hh"

namespace pq {

class HackernewsPopulator {
  public:
    HackernewsPopulator(const Json& param);

    inline void post_article(uint32_t author, uint32_t article);
    inline bool vote(uint32_t author, uint32_t user);
    inline uint32_t next_aid();
    inline uint32_t next_comment();

    inline void set_nusers(uint32_t);
    inline uint32_t nusers() const;
    inline uint32_t narticles() const;
    inline void set_narticles(uint32_t n);
    inline uint32_t karma(uint32_t author) const;
    inline void set_log(bool val);
    inline bool log() const;
    inline const std::vector<uint32_t>& articles() const;
    inline const std::vector<uint32_t>& karmas() const;
    inline uint32_t pre() const;
    inline uint32_t nops() const;
    inline uint32_t vote_rate() const;
    inline uint32_t comment_rate() const;
    inline uint32_t post_rate() const;
    inline bool m() const;

  private:
    Json param_;
    bool log_;
    uint32_t nusers_;
    // author -> karma
    std::vector<uint32_t> karma_;
    // article -> author
    std::vector<uint32_t> articles_;
    // article -> users
    std::map<uint32_t, std::set<uint32_t> > votes_;
    uint32_t pre_;
    uint32_t narticles_;
    uint32_t ncomments_;
    bool materialize_inline_;
};

HackernewsPopulator::HackernewsPopulator(const Json& param)
    : param_(param), log_(param["log"].as_b(false)), nusers_(param["nusers"].as_i(500)),
      karma_(param["nusers"].as_i(500)),
      articles_(1000000),
      pre_(param["narticles"].as_i(1000)),
      narticles_(0), ncomments_(0), 
      materialize_inline_(param["materialize"].as_b(false)) {
}

inline uint32_t HackernewsPopulator::nusers() const {
    return nusers_;
}
    
inline void HackernewsPopulator::post_article(uint32_t author, uint32_t article) {
    auto it = votes_.find(article);
    mandatory_assert(it == votes_.end());
    articles_[article] = author;
    auto s = std::set<uint32_t>();
    s.insert(author);
    votes_.insert(std::pair<uint32_t, std::set<uint32_t> >(article, s));
    ++narticles_;
    ++karma_[author];  // one vote
}

inline bool HackernewsPopulator::vote(uint32_t article, uint32_t user) {
    auto it = votes_.find(article);
    mandatory_assert(it != votes_.end());    
    if (it->second.find(user) != it->second.end())
        return false;
    it->second.insert(user);
    ++karma_[articles_[article]];
    return true;
}

inline uint32_t HackernewsPopulator::next_aid() {
    mandatory_assert(narticles_ < articles_.size());
    return narticles_++;
}

inline uint32_t HackernewsPopulator::next_comment() {
    mandatory_assert(ncomments_ < 10000000);
    return ncomments_++;
}

inline uint32_t HackernewsPopulator::narticles() const {
    return narticles_;
}

inline uint32_t HackernewsPopulator::pre() const {
    return pre_;
}

inline void HackernewsPopulator::set_narticles(uint32_t n) {
    narticles_ = n;
}

inline const std::vector<uint32_t>& HackernewsPopulator::articles() const {
    return articles_;
}

inline const std::vector<uint32_t>& HackernewsPopulator::karmas() const {
    return karma_;
}

inline uint32_t HackernewsPopulator::karma(uint32_t author) const {
    return karma_[author];
}

inline bool HackernewsPopulator::log() const {
    return log_;
}

inline void HackernewsPopulator::set_log(bool val) {
    log_ = val;
}

inline uint32_t HackernewsPopulator::nops() const {
    return param_["nops"].as_i(10000);
}

inline uint32_t HackernewsPopulator::vote_rate() const {
    return param_["vote_rate"].as_i(1);
}

inline uint32_t HackernewsPopulator::comment_rate() const {
    return param_["comment_rate"].as_i(1);
}

inline uint32_t HackernewsPopulator::post_rate() const {
    return param_["post_rate"].as_i(0);
}

inline bool HackernewsPopulator::m() const {
    return materialize_inline_;
}

};

#endif
