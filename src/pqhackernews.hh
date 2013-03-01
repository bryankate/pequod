#ifndef PQHACKERNEWS_HH
#define PQHACKERNEWS_HH
#include <boost/random.hpp>
#include <utility>
#include <vector>
#include <map>
#include <iostream>
#include <stdint.h>
#include "json.hh"
#include "pqserver.hh"

namespace pq {

class HackernewsPopulator {
  public:
    HackernewsPopulator(const Json& param);

    inline void post_article(uint32_t author, uint32_t article);
    inline void vote(uint32_t author);
    inline uint32_t next_aid();

    inline void set_nusers(uint32_t);
    inline uint32_t nusers() const;
    inline uint32_t narticles() const;
    inline void set_narticles(uint32_t n);
    inline uint32_t karma(uint32_t author) const;
    inline bool log() const;

    inline const std::vector<std::pair<uint32_t, uint32_t> >& articles() const;

  private:
    bool log_;
    // article -> <author, ncomment>
    std::vector<std::pair<uint32_t, uint32_t> > articles_;
    // author -> karma
    uint32_t nusers_;
    std::vector<uint32_t> karma_;
    uint32_t narticles_;
};

class HackernewsRunner {
  public:
    HackernewsRunner(Server& server, HackernewsPopulator& hp);
    
    void populate();
    void run();
    
  private:
    Server& server_;
    HackernewsPopulator& hp_;

    void post(uint32_t u, uint32_t a);
};

inline uint32_t HackernewsPopulator::nusers() const {
    return nusers_;
}
    
inline void HackernewsPopulator::post_article(uint32_t author, uint32_t article) {
    articles_[article].first = author;
    articles_[article].second = 0;
}

inline void HackernewsPopulator::vote(uint32_t article) {
    ++karma_[articles_[article].first];
}

inline uint32_t HackernewsPopulator::next_aid() {
    mandatory_assert(narticles_ < articles_.size());
    return narticles_++;
}

inline uint32_t HackernewsPopulator::narticles() const {
    return narticles_;
}

inline void HackernewsPopulator::set_narticles(uint32_t n) {
    narticles_ = n;
}

inline const std::vector<std::pair<uint32_t, uint32_t> >& HackernewsPopulator::articles() const {
    return articles_;
}

inline uint32_t HackernewsPopulator::karma(uint32_t author) const {
    return karma_[author];
}

inline bool HackernewsPopulator::log() const {
    return log_;
}

inline HackernewsRunner::HackernewsRunner(Server& server, HackernewsPopulator& hp)
    : server_(server), hp_(hp) {
}

};

#endif
