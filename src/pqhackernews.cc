#include "pqhackernews.hh"

namespace pq {

HackernewsPopulator::HackernewsPopulator(const Json& param)
    : karma_(param["nusers"].as_i(5000)),
      articles_(param["narticles"].as_i(1000000)), narticles_(0) {
}

void HackernewsPopulator::create_data() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    for (uint32_t i = 0; i < narticles_; ++i) {
        const uint32_t u = rng(nusers_);
        const uint32_t aid = next_aid();
        post_article(u, aid);
    }
}

void HackernewsRunner::populate() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    hp_.create_data();
    char buf[128];

    // author, aid, 0
    for (uint32_t aid = 0; aid < hp_.narticles(); aid++) {
        auto &art = hp_.articles()[aid];
        sprintf(buf, "a|%05d|%05d|%05d", art.first, aid, 0);
        server_.insert(Str(buf, 19), Str("lalalalala", 10), true);
        if (hp_.log()) {
            printf("article %.19s\n", buf);
        }

        // author, aid, commenter, j
        const uint32_t ncomment = rng(50);
        for (uint32_t j = 0; j < ncomment; ++j) {
            const uint32_t commentor = rng(hp_.nusers());
            sprintf(buf, "a|%05d|%05d|%05d|%05d", art.first, aid, commentor, j);
            server_.insert(Str(buf, 25), Str("lalalalala", 10), true);
            if (hp_.log()) {
                printf("comment %.25s\n", buf);
            }
        }

        // author, aid, voter
        const uint32_t nvote = rng(50);
        for (uint32_t j = 0; j < nvote; ++j) {
            hp_.vote(art.first); // karma
            const uint32_t voter = rng(hp_.nusers());
            sprintf(buf, "v|%05d|%05d|%05d", art.first, aid, voter);
            server_.insert(Str(buf, 19), Str("1", 1), true);
            if (hp_.log()) {
                printf("vote %.19s\n", buf);
            }
        }
    }

    // create karma table
    // define the join between a and v table
}

void HackernewsRunner::run() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    const uint32_t nops = 1000000000;
    const uint32_t nusers = hp_.nusers();
    
    exit(1);
    for (uint32_t i = 0; i < nops; ++i) {
        uint32_t a = rng(100);
        uint32_t u = rng(nusers);
        if (a < 3) {
            // post a page
            uint32_t aid = hp_.next_aid();
            hp_.post_article(u, aid);
            // insert the article into the server as h|<aid>|0/<article content>
            // insert a|<author>|<aid>
        } else {
            // read a page
            // with some probablity, vote a page
            // with some probablity, post a comment
        }
    }
}

}
