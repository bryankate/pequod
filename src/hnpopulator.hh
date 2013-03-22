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
    inline bool push() const;
    inline const std::vector<uint32_t>& articles() const;
    inline const std::vector<uint32_t>& karmas() const;
    inline uint32_t pre() const;
    inline uint32_t nops() const;
    inline uint32_t vote_rate() const;
    inline uint32_t comment_rate() const;
    inline uint32_t post_rate() const;
    inline bool mk() const;
    inline bool ma() const;
    inline bool pg() const;
    inline bool populate_only() const;
    inline bool run_only() const;
    inline void fill_db();
    inline void populate_from_files(uint32_t* nv, uint32_t* nc);
    inline void set_defaults();

    uint32_t ncomments;
    uint32_t nvotes;

  private:
    Json param_;
    bool log_;
    bool push_;
    uint32_t nusers_;
    // author -> karma
    std::vector<uint32_t> karma_;
    // article -> author
    std::vector<uint32_t> articles_;
    // article -> users
    std::map<uint32_t, std::set<uint32_t> > votes_;
    uint32_t pre_;
    uint32_t narticles_;
    bool pull_;
    bool materialize_articles_;
    bool large_;
    bool run_only_;
};

inline HackernewsPopulator::HackernewsPopulator(const Json& param)
    :  ncomments(0), nvotes(0), param_(param), log_(param["log"].as_b(false)), 
       push_(param["push"].as_b(false)), 
      nusers_(param["hnusers"].as_i(10)),
      karma_(1000000),
      articles_(1000000),
      pre_(param["narticles"].as_i(100)),
      narticles_(0),
      pull_(param["pull"].as_b(false)),
      materialize_articles_(param["super_materialize"].as_b(false)),
      large_(param["large"].as_b(false)),
      run_only_(param["run_only"].as_b(false)) {
    if (push_) 
        // I know this is weird, but this turns materialization off.
        pull_ = true;
        
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
    if (run_only_) {
        nvotes++;
        return true;
    }
    auto it = votes_.find(article);
    uint32_t author = articles_[article];
    mandatory_assert(it != votes_.end());    
    if (it->second.find(user) != it->second.end())
        return false;
    it->second.insert(user);
    ++karma_[author];
    nvotes++;
    return true;
}

inline uint32_t HackernewsPopulator::next_aid() {
    mandatory_assert(narticles_ < articles_.size());
    return narticles_++;
}

inline uint32_t HackernewsPopulator::next_comment() {
    mandatory_assert(ncomments < 10000000);
    return ncomments++;
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

inline bool HackernewsPopulator::push() const {
    return push_;
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

inline bool HackernewsPopulator::mk() const {
    return !pull_;
}

inline bool HackernewsPopulator::ma() const {
    return materialize_articles_;
}

inline bool HackernewsPopulator::pg() const {
    return param_["pg"].as_b(false);
}

inline bool HackernewsPopulator::populate_only() const {
    return param_["populate_only"].as_b(false);
}

inline bool HackernewsPopulator::run_only() const {
    return run_only_;
}

inline void HackernewsPopulator::set_defaults() {
    if (param_["large"].as_b(false)) {
        ncomments = 949245;
        narticles_ = 100000;
        nusers_ = 50000;
        nvotes = 2444184;
    } else {
        ncomments = 100;
        narticles_ = 100;
        nusers_ = 10;
        nvotes = 100;
    }
    for (uint32_t i = 0; i < narticles_; i++) {
        articles_[i] = i % nusers_;
    }
}

inline void HackernewsPopulator::fill_db() {
    mandatory_assert(pg());
    CHECK_EQ(system("psql -p 5477 hn < db/hn/clear.sql > /dev/null"), 0);
    char sz[128];
    char mat[128];
    char p[128];

    if (large_)
        sprintf(sz, "large");
    else
        sprintf(sz, "small");
    if (mk())
        sprintf(mat, ".mv");
    else
        sprintf(mat, ".nomv");
    if (push_)
        sprintf(p, ".push");
    else
        sprintf(p, ".nopush");

    char prefix[128];
    sprintf(prefix, "db/hn/pg.dump.%s%s%s", sz, mat, p);
    char cmd[128];
    sprintf(cmd, "psql -p 5477 hn < %s > /dev/null", prefix);
    CHECK_EQ(system(cmd), 0);
}

inline void HackernewsPopulator::populate_from_files(uint32_t* nv, uint32_t* nc) {
    mandatory_assert(pg());

    using std::string;
    using std::ifstream;
    using std::istringstream;

    String fn;

    char dataprefix[128];
    char sz[128];
    char mat[128];
    char p[128];

    if (large_)
        sprintf(sz, "large");
    else
        sprintf(sz, "small");
    if (mk())
        sprintf(mat, ".mv");
    else
        sprintf(mat, ".nomv");
    if (push_)
        sprintf(p, ".push");
    else
        sprintf(p, ".nopush");

    sprintf(dataprefix, "db/hn/hn.data.%s%s%s", sz, mat, p);
    fn = dataprefix + String(".articles");
    ifstream infile(fn.c_str(), ifstream::in);
    string line;
    uint32_t aid;
    uint32_t author;
    
    pre_ = 0;
    narticles_ = 0;
    nusers_ = 0;
    ncomments = 0;
    nvotes = 0;

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

    
    fn = dataprefix + String(".votes");
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
        if (it == votes_.end()) {
            printf("All articles should have one vote: %d %d\n", aid, voter);
            mandatory_assert(false);
        }
        it->second.insert(voter); 
        if (voter > nusers_) 
            nusers_ = voter;
        ++karma_[articles_[aid]];  // one vote
        (*nv)++;
    }

    fn = dataprefix + String(".comments");
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

        ncomments++;
        (*nc)++;
    }

    if (push_) {
        fn = dataprefix + String(".karma");
        ifstream infile4(fn.c_str(), ifstream::in); // dumb
        while(infile4) {
            uint32_t author;
            uint32_t karma;
            if (!getline(infile4, line))
                break;
            boost::trim(line);
            if ((line.empty()) || (line[0] == '#'))
                continue;
            istringstream iss(line);            
            iss >> author >> karma;
            if (author > nusers_) 
                nusers_ = author;
            mandatory_assert(karma_[author] == karma);
        }
    }

    if (log_) {
        for (uint32_t i = 0; i < nusers_; i++) {
            std::cout << "Karma: " << i << " " << karma_[i] << "\n";
        }
    }
}

};

#endif
