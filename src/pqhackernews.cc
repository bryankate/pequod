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

void HackernewsRunner::post_article(uint32_t author, uint32_t aid) {
    char buf[128];
    hp_.post_article(author, aid);
    sprintf(buf, "a|%05d%05d", author, aid);
    server_.insert(Str(buf, 12), Str("lalalalala", 10));
    if (hp_.log()) {
        printf("post %.12s\n", buf);
    }
    sprintf(buf, "v|%05d%05d|%05d", author, aid, author);
    server_.insert(Str(buf, 18), Str("1", 1));
    if (hp_.log()) {
        printf("vote %.18s\n", buf);
    }
}

void HackernewsRunner::post_comment(uint32_t commentor, uint32_t aid) {
    char buf[128];
    uint32_t author = hp_.articles()[aid];
    sprintf(buf, "c|%05d%05d|%05d|%05d", author, aid, hp_.next_comment(), commentor);
    server_.insert(Str(buf, 24), Str("calalalala", 10));
    if (hp_.log()) {
        printf("comment  %.24s\n", buf);
    }
}

bool HackernewsRunner::vote(uint32_t voter, uint32_t aid) {
    char buf[128];
    uint32_t author = hp_.articles()[aid];
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

void HackernewsRunner::get_karma(String user) {
    char buf3[128];
    sprintf(buf3, "k|%s", user.c_str());
    auto kbit = server_.find(Str(buf3, 7));
    uint32_t karma = 0;
    if (kbit != NULL) {
        karma = atoi(kbit->value().c_str());
        uint32_t my_karma = hp_.karma(atoi(user.c_str()));
        mandatory_assert(karma == my_karma && "Karma mismatch");
        if (hp_.log())
            std::cout << "  k " << ":" << karma << "\n";
    }
}

void HackernewsRunner::read_article(uint32_t aid) {
    char buf1[128], buf2[128];
    mandatory_assert(aid < hp_.narticles());
    uint32_t author = hp_.articles()[aid];
    sprintf(buf1, "ma|%05d%05d|", author, aid);
    sprintf(buf2, "ma|%05d%05d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 14)),
        eit = server_.lower_bound(Str(buf2, 14));
    for (; bit != eit; ++bit) {
        String field = extract_spkey(2, bit->key());
        if (hp_.log()) {
            if (field == "a")
                std::cout << "read " << bit->key() << ": " << bit->value() << "\n";
            else
                std::cout << "  " << field << " " << bit->key() << ": " << bit->value() << "\n";
        }
        if (!hp_.m() && field == "c")
            get_karma(extract_spkey(4, bit->key()));
    }
}

void HackernewsRunner::populate() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    uint32_t nv = 0;
    uint32_t nc = 0;
    
    for (uint32_t aid = 0; aid < hp_.pre(); aid++) {
        const uint32_t author = rng(hp_.nusers());
        post_article(author, aid);
        const uint32_t ncomment = rng(10);
        for (uint32_t j = 1; j <= ncomment; ++j) {
            nc++;
            const uint32_t commentor = rng(hp_.nusers());
            post_comment(commentor, aid);
        }
        const uint32_t nvote = rng(10);
        for (uint32_t j = 0; j < nvote; ++j) {
            const uint32_t voter = rng(hp_.nusers());
            if (vote(voter, aid))
                nv++;
        }
    }

    // Materialize karma between articles and votes
    pq::Join* j = new pq::Join;
    String join_str = "k|<author:5> "
        "a|<author:5><seqid:5> "
        "v|<author><seqid>|<voter:5>";
    String start = "k|";
    String end = "k}";
    bool valid = j->assign_parse(join_str);
    mandatory_assert(valid && "Invalid karma join");
    j->set_jvt(jvt_count_match);
    server_.add_join(start, end, j);

    start = "ma|";
    end = "ma}";

    // Materialize articles
    join_str = "ma|<author:5><seqid:5>|a "
        "a|<author><seqid1> ";
    j = new pq::Join;
    valid = j->assign_parse(join_str);
    mandatory_assert(valid && "Invalid ma|article join");
    server_.add_join(start, end, j);
    
    // Materialize votes
    join_str = "ma|<author:5><seqid:5>|v "
        "v|<author><seqid>|<voter:5> ";
    j = new pq::Join;
    valid = j->assign_parse(join_str);
    mandatory_assert(valid && "Invalid ma|votes join");
    j->set_jvt(jvt_count_match);
    server_.add_join(start, end, j);

    // Materialize comments
    join_str = "ma|<author:5><seqid:5>|c|<commenter:5><cid:5> "
        "c|<author><seqid>|<cid>|<commenter> ";
    j = new pq::Join;
    valid = j->assign_parse(join_str);
    mandatory_assert(valid && "Invalid ma|comments join");
    server_.add_join(start, end, j);
    
    if (hp_.m()) {
        // Materialize karma inline
        join_str = "ma|<author:5><seqid:5>|k|<commenter:5> "
            "c|<author><seqid>|<cid:5>|<commenter> "
            "k|<commenter>";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|karma join");
        server_.add_join(start, end, j);
    }

    std::cout << "Added " << hp_.nusers() << " users, " << hp_.narticles() 
              << " articles, " << nv << " votes, " << nc << " comments." << std::endl;
}

void HackernewsRunner::run() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    const uint32_t nops = hp_.nops();
    const uint32_t nusers = hp_.nusers();
    struct rusage ru[2];
    uint32_t nread = 0, npost = 0, ncomment = 0, nvote = 0;

    char buf1[128], buf2[128];
    if (hp_.m()) {
        std::cout << "Materializing karma inline with articles.\n";
        sprintf(buf1, "a|");
        sprintf(buf2, "a}");
    } else {
        std::cout << "Materializing separate karma table.\n";
        sprintf(buf1, "k|");
        sprintf(buf2, "k}");
    }
    server_.validate(Str(buf1, 2), Str(buf2, 2));
    std::cout << "Finished validate.\n";
    if (hp_.log()) {
        std::cout << ": karma scan [" << buf1 << "," << buf2 << ")\n";
        auto bit = server_.lower_bound(Str(buf1, 2)),
            eit = server_.lower_bound(Str(buf2, 2));
        for (; bit != eit; ++bit)
            std::cout << "  " << bit->key() << ": " << bit->value() << "\n";
        std::cout << ": end karma scan [" << buf1 << "," << buf2 << ")\n";
    }

    getrusage(RUSAGE_SELF, &ru[0]);
    for (uint32_t i = 0; i < nops; ++i) {
        uint32_t p = rng(100);
        uint32_t user = rng(nusers);
        if (p < hp_.post_rate()) {
            post_article(user, hp_.next_aid());
            npost++;
        } else {
            uint32_t aid = rng(hp_.narticles());
            read_article(aid);
            nread++;
            if (p < hp_.vote_rate()) {
                vote(user, aid);
                nvote++;
            }
            if (p < hp_.comment_rate()) {
                post_comment(user, aid);
                ncomment++;
            }
        }
    }
    getrusage(RUSAGE_SELF, &ru[1]);
    Json stats = Json().set("nread", nread).set("npost", npost)
	.set("ncomment", ncomment).set("nvote", nvote)
	.set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    stats.merge(server_.stats());
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";

}

}
