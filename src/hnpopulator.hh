#ifndef HNPOPULATOR_HH
#define HNPOPULATOR_HH
#include <utility>
#include <vector>
#include <map>
#include <set>
#include <iostream>
#include <stdint.h>
#include "json.hh"
#include <fstream>
#include <sstream>
#include <boost/algorithm/string.hpp>
#include "check.hh"

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
    inline bool pg() const;
    inline void populate_from_files(uint32_t* nv, uint32_t* nc);

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
    bool large_;
};

HackernewsPopulator::HackernewsPopulator(const Json& param)
    : param_(param), log_(param["log"].as_b(false)), nusers_(param["hnusers"].as_i(10)),
      karma_(1000000),
      articles_(1000000),
      pre_(param["narticles"].as_i(10)),
      narticles_(0), ncomments_(0), 
      materialize_inline_(param["materialize"].as_b(false)),
      large_(param["large"].as_b(false)) {
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
    return param_["nops"].as_i(10);
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

inline bool HackernewsPopulator::pg() const {
    return param_["pg"].as_b(false);
}

inline void HackernewsPopulator::populate_from_files(uint32_t* nv, uint32_t* nc) {
    using std::string;
    using std::ifstream;
    using std::istringstream;

    String fn;
    String prefix;

    CHECK_EQ(system("psql -p 5477 hn < db/hn/clear.sql > /dev/null"), 0);
    if (large_ && m()) {
        prefix = "db/hn/hn.data.large.mv";
        CHECK_EQ(system("psql -p 5477 hn < db/hn/pg.dump.large.mv > /dev/null"), 0);
    } else if (large_ && !m()) {
        prefix = "db/hn/hn.data.large";
        CHECK_EQ(system("psql -p 5477 hn < db/hn/pg.dump.large > /dev/null"), 0);
    } else if (!large_ && m())  {
        prefix = "db/hn/hn.data.small.mv";
        CHECK_EQ(system("psql -p 5477 hn < db/hn/pg.dump.small.mv > /dev/null"), 0);
    } else if (!large_ && !m()) {
        prefix = "db/hn/hn.data.small";
        CHECK_EQ(system("psql -p 5477 hn < db/hn/pg.dump.small > /dev/null"), 0);
    }
    

    fn = prefix + String(".articles");
    ifstream infile(fn.c_str(), ifstream::in);
    string line;
    uint32_t aid;
    uint32_t author;
    
    pre_ = 0;
    narticles_ = 0;
    nusers_ = 0;
    ncomments_ = 0;

    while(infile) {
        if (!getline(infile, line))
            break;
        boost::trim(line);
        if ((line.empty()) || (line[0] == '#'))
            continue;
        istringstream iss(line);            
        iss >> aid >> author;
        articles_[aid] = author;
        if (author > nusers_) 
            nusers_ = author;
        auto s = std::set<uint32_t>();
        s.insert(author);
        votes_.insert(std::pair<uint32_t, std::set<uint32_t> >(aid, s));
        ++narticles_;
    }

    fn = prefix + String(".votes");
    ifstream infile2(fn.c_str(), ifstream::in);
    uint32_t voter;
    while(infile2) {
        if (!getline(infile2, line))
            break;
        boost::trim(line);
        if ((line.empty()) || (line[0] == '#'))
            continue;
        istringstream iss(line);            
        iss >> aid >> voter;
        auto it = votes_.find(aid);
        mandatory_assert(it != votes_.end());
        it->second.insert(voter); 
        if (voter > nusers_) 
            nusers_ = voter;
        ++karma_[articles_[aid]];  // one vote
        (*nv)++;
    }

    fn = prefix + String(".comments");
    ifstream infile3(fn.c_str(), ifstream::in); // dumb
    while(infile3) {
        uint32_t commentor;
        uint32_t aid;
        uint32_t cid;
        if (!getline(infile3, line))
            break;
        boost::trim(line);
        if ((line.empty()) || (line[0] == '#'))
            continue;
        istringstream iss(line);            
        iss >> cid >> aid >> commentor;
        if (author > nusers_) 
            nusers_ = author;

        ncomments_++;
        (*nc)++;
    }

    if (log_) {
        for (uint32_t i = 0; i < nusers_; i++) {
            std::cout << "Karma: " << i << " " << karma_[i] << "\n";
        }
    }
}

};

#endif
