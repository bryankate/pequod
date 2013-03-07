#include "pqhackernews.hh"
#include "pqjoin.hh"
#include <sys/resource.h>
#include "json.hh"
#include "time.hh"
#include "sp_key.hh"

namespace pq {

HackernewsPopulator::HackernewsPopulator(const Json& param)
    : param_(param), log_(false), nusers_(param["nusers"].as_i(500)),
      karma_(param["nusers"].as_i(500)),
      articles_(1000000),
      pre_(param["narticles"].as_i(1000)),
      narticles_(0), ncomments_(0), 
      materialize_inline_(param["materialize"].as_b(false)) {
}

void HackernewsPopulator::post_article(uint32_t author, uint32_t article) {
    auto it = votes_.find(article);
    mandatory_assert(it == votes_.end());
    articles_[article] = author;
    auto s = std::set<uint32_t>();
    s.insert(author);
    votes_.insert(std::pair<uint32_t, std::set<uint32_t> >(article, s));
    ++narticles_;
    ++karma_[author];  // one vote
}

bool HackernewsPopulator::vote(uint32_t article, uint32_t user) {
    auto it = votes_.find(article);
    mandatory_assert(it != votes_.end());    
    if (it->second.find(user) != it->second.end())
        return false;
    it->second.insert(user);
    ++karma_[articles_[article]];
    return true;
}


}
