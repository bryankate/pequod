#include "pqhackernews.hh"
#include "pqjoin.hh"

namespace pq {

HackernewsPopulator::HackernewsPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5)),
      karma_(param["nusers"].as_i(5)*50*10),
      articles_(1000000),
      pre_(param["narticles"].as_i(10)),
      narticles_(0) {
}

void HackernewsRunner::post_article(uint32_t author, uint32_t aid) {
    char buf[128];
    hp_.post_article(author, aid);
    sprintf(buf, "a|%05d%05d|%05d|%05d", author, aid, 0, author);
    server_.insert(Str(buf, 24), Str("lalalalala", 10));
    if (hp_.log()) {
        printf("article %.24s\n", buf);
    }
}

void HackernewsRunner::post_comment(uint32_t commentor, uint32_t author, uint32_t aid) {
    char buf[128];
    sprintf(buf, "a|%05d%05d|%05d|%05d", author, aid, hp_.next_comment(), commentor);
    server_.insert(Str(buf, 25), Str("lalalalala", 10));
    if (hp_.log()) {
        printf("  %.24s\n", buf);
    }
}

bool HackernewsRunner::vote(uint32_t voter, uint32_t author, uint32_t aid) {
    char buf[128];
    if (hp_.vote(aid, voter)) {
        sprintf(buf, "v|%05d%05d|%05d", author, aid, voter);
        server_.insert(Str(buf, 18), Str("1", 1));
        if (hp_.log()) {
            printf("vote %.18s\n", buf);
        }
        return true;
    }
    return false;
}

void HackernewsRunner::read_article(uint32_t aid, uint32_t* author) {
    char buf[128];
    mandatory_assert(aid < hp_.narticles());
    *author = hp_.articles()[aid];
    sprintf(buf, "a|%05d%05d|%05d|%05d", *author, aid, 0, *author);
    auto it = server_.find(buf);
}

void HackernewsRunner::populate() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    char buf[128];
    uint32_t nv = 0;
    uint32_t nc = 0;
    
    for (uint32_t aid = 0; aid < hp_.pre(); aid++) {
        const uint32_t author = rng(hp_.nusers());
        post_article(author, aid);
        const uint32_t ncomment = rng(10);
        for (uint32_t j = 1; j <= ncomment; ++j) {
            nc++;
            const uint32_t commentor = rng(hp_.nusers());
            post_comment(commentor, author, aid);
        }
        const uint32_t nvote = rng(10);
        for (uint32_t j = 0; j < nvote; ++j) {
            const uint32_t voter = rng(hp_.nusers());
            if (vote(voter, author, aid))
                nv++;
        }
    }
    pq::Join* j = new pq::Join;
    bool valid = j->assign_parse("k|<author:5> "
                                 "a|<aid:10>|00000|<author> "
                                 "v|<aid>|<voter:5>");
    mandatory_assert(valid && "Invalid join");
    j->set_jvt(jvt_count_match);
    server_.add_join("k|", "k}", j);
    std::cout << "Added " << hp_.nusers() << " users, " << hp_.narticles() 
              << " articles, " << nv << " votes, " << nc << " comments." << std::endl;
}

void HackernewsRunner::run() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    const uint32_t nops = 10;
    const uint32_t nusers = hp_.nusers();
    const uint32_t narticles = hp_.narticles();

    char buf1[128], buf2[128];
    sprintf(buf1, "k|");
    sprintf(buf2, "k}");
    server_.validate(Str(buf1, 2), Str(buf2, 2));

    std::cout << ": scan [" << buf1 << "," << buf2 << ")\n";
    auto bit = server_.lower_bound(Str(buf1, 2)),
        eit = server_.lower_bound(Str(buf2, 2));
    for (; bit != eit; ++bit)
        std::cout << "  " << bit->key() << ": " << bit->value() << "\n";

    for (uint32_t i = 0; i < nops; ++i) {
        uint32_t a = rng(100);
        uint32_t u = rng(nusers);
        if (a < 3) {
            post_article(u, hp_.next_aid());
        } else {
            uint32_t aid = rng(narticles);
            uint32_t author;
            read_article(aid, &author);
            if (a < 10)
                vote(u, author, aid);
            if (a < 12)
                post_comment(u, author, aid);
        }
    }
}

}
