#include "pqhackernews.hh"
#include "pqjoin.hh"

namespace pq {

HackernewsPopulator::HackernewsPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5)),
      karma_(param["nusers"].as_i(5)*50*10),
      articles_(param["narticles"].as_i(10)),
      narticles_(0) {
}

void HackernewsRunner::populate() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    char buf[128];
    uint32_t nv = 0;
    uint32_t nc = 0;
    
    // author:aid, 0, author
    for (uint32_t aid = 0; aid < hp_.articles().size(); aid++) {
        const uint32_t author = rng(hp_.nusers());
        hp_.post_article(author, aid);

        sprintf(buf, "a|%05d%05d|%05d|%05d", author, aid, 0, author);
        server_.insert(Str(buf, 24), Str("lalalalala", 10));
        if (hp_.log()) {
            printf("article %.24s\n", buf);
        }

        // author:aid, j, commenter
        const uint32_t ncomment = rng(10);
        for (uint32_t j = 1; j <= ncomment; ++j) {
            nc++;
            const uint32_t commentor = rng(hp_.nusers());
            sprintf(buf, "a|%05d%05d|%05d|%05d", author, aid, j, commentor);
            server_.insert(Str(buf, 25), Str("lalalalala", 10));
            if (hp_.log()) {
                printf("  %.24s\n", buf);
            }
        }

        // author:aid, voter.
        const uint32_t nvote = rng(10);
        for (uint32_t j = 0; j < nvote; ++j) {
            const uint32_t voter = rng(hp_.nusers());
            if (hp_.vote(aid, voter)) {
                nv++;
                sprintf(buf, "v|%05d%05d|%05d", author, aid, voter);
                server_.insert(Str(buf, 18), Str("1", 1));
                if (hp_.log()) {
                    printf("vote %.18s\n", buf);
                }
            }
        }
    }
    hp_.set_narticles(hp_.articles().size());
    pq::Join* j = new pq::Join;
    //    bool valid = j->assign_parse("k|<author:5>|<aid:10>|<voter:5> "
    //                                 "a|<aid>|<idx:5>|<author:5> "
    //                                 "v|<aid>|<voter>");
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

    char buf1[128], buf2[128];
    sprintf(buf1, "k|");
    sprintf(buf2, "k}");
    server_.validate(Str(buf1, 2), Str(buf2, 2));

    std::cout << ": scan [" << buf1 << "," << buf2 << ")\n";
    auto bit = server_.lower_bound(Str(buf1, 2)),
        eit = server_.lower_bound(Str(buf2, 2));
    for (; bit != eit; ++bit)
        std::cout << "  " << bit->key() << ": " << bit->value() << "\n";

    std::cout << "DONE" << std::endl;
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
