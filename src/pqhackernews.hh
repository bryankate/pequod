#include "hnpopulator.hh"
#include "pqjoin.hh"
#include <sys/resource.h>
#include "json.hh"
#include "time.hh"
#include "sp_key.hh"
#include "pqserver.hh"

namespace pq {

class HackernewsRunner {
  public:
    HackernewsRunner(Server& server, HackernewsPopulator& hp);
    
    void populate();
    void run();
    
  private:
    Server& server_;
    HackernewsPopulator& hp_;

    void post_article(uint32_t author, uint32_t aid);
    void post_comment(uint32_t commentor, uint32_t aid);
    bool vote(uint32_t voter, uint32_t aid);
    uint32_t get_karma(String user);
    void read_article(uint32_t aid);
    void read_materialized(uint32_t aid);
    void read_tables(uint32_t aid);
};


inline HackernewsRunner::HackernewsRunner(Server& server, HackernewsPopulator& hp)
    : server_(server), hp_(hp) {
}

void HackernewsRunner::post_article(uint32_t author, uint32_t aid) {
    char buf[128];
    hp_.post_article(author, aid);
    sprintf(buf, "a|%07d%07d", author, aid);
    server_.insert(Str(buf, 16), Str("lalalalala", 10));
    if (hp_.log()) {
        printf("post %.16s\n", buf);
    }
    sprintf(buf, "v|%07d%07d|%07d", author, aid, author);
    server_.insert(Str(buf, 24), Str("1", 1));
    if (hp_.log()) {
        printf("vote %.24s\n", buf);
    }
}

void HackernewsRunner::post_comment(uint32_t commentor, uint32_t aid) {
    char buf[128];
    uint32_t author = hp_.articles()[aid];
    sprintf(buf, "c|%07d%07d|%07d|%07d", author, aid, hp_.next_comment(), commentor);
    server_.insert(Str(buf, 32), Str("calalalala", 10));
    if (hp_.log()) {
        printf("comment  %.32s\n", buf);
    }
}

bool HackernewsRunner::vote(uint32_t voter, uint32_t aid) {
    char buf[128];
    uint32_t author = hp_.articles()[aid];
    if (hp_.vote(aid, voter)) {
        sprintf(buf, "v|%07d%07d|%07d", author, aid, voter);
        server_.insert(Str(buf, 24), Str("1", 1));
        if (hp_.log()) {
            printf("vote %.24s\n", buf);
        }
        return true;
    }
    return false;
}

uint32_t HackernewsRunner::get_karma(String user) {
    char buf3[128];
    sprintf(buf3, "k|%s", user.c_str());
    auto kbit = server_.find(Str(buf3, 9));
    uint32_t karma = 0;
    if (kbit != NULL) {
        karma = atoi(kbit->value().c_str());
        uint32_t my_karma = hp_.karma(atoi(user.c_str()));
        mandatory_assert(karma == my_karma && "Karma mismatch");
    }
    return karma;
}

void HackernewsRunner::read_article(uint32_t aid) {
    if (hp_.m()) 
        read_materialized(aid);
    else
        read_tables(aid);
}

void HackernewsRunner::read_materialized(uint32_t aid) {
    char buf1[128], buf2[128];
    mandatory_assert(aid < hp_.narticles());
    uint32_t author = hp_.articles()[aid];
    sprintf(buf1, "ma|%07d%07d|", author, aid);
    sprintf(buf2, "ma|%07d%07d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 18)),
        eit = server_.lower_bound(Str(buf2, 18));
    for (; bit != eit; ++bit) {
        String field = extract_spkey(2, bit->key());
        if (hp_.log()) {
            if (field == "a")
                std::cout << "read " << bit->key() << ": " << bit->value() << "\n";
            else
                std::cout << "  " << field << " " << bit->key() << ": " << bit->value() << "\n";
        }
    }
}

void HackernewsRunner::read_tables(uint32_t aid) {
    char buf1[128], buf2[128];
    mandatory_assert(aid < hp_.narticles());
    uint32_t author = hp_.articles()[aid];

    // Article
    sprintf(buf1, "a|%07d%07d", author, aid);
    auto ait = server_.find(Str(buf1, 16));
    mandatory_assert(ait != NULL);
    if (hp_.log())
        std::cout << "read " << ait->key() << ":" << ait->value() << "\n";

    // Comments and Karma
    sprintf(buf1, "c|%07d%07d|", author, aid);
    sprintf(buf2, "c|%07d%07d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 17)),
        eit = server_.lower_bound(Str(buf2, 17));
    for (; bit != eit; ++bit) {
        uint32_t k = get_karma(extract_spkey(3, bit->key()));
        if (hp_.log()) {
            std::cout << "  c " << bit->key() << ": " << bit->value() 
                      << "  k " << k << "\n";
        }
    }

    // Votes
    sprintf(buf1, "v|%07d%07d|", author, aid);
    sprintf(buf2, "v|%07d%07d}", author, aid);
    auto cit = server_.lower_bound(Str(buf1, 17)),
        ceit = server_.lower_bound(Str(buf2, 17));
    uint32_t votect = 0;
    for (; cit != ceit; ++cit) {
        votect++;
        if (hp_.log())
            std::cout << "  v " << cit->key() << ": " << cit->value() << "\n";
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

    String start, end, join_str;
    bool valid;
    pq::Join* j;

    if (hp_.m()) {
        start = "ma|";
        end = "ma}";
        // Materialize articles
        join_str = "ma|<author:7><seqid:7>|a "
            "a|<author><seqid> ";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|article join");
        server_.add_join(start, end, j);
        
        // Materialize votes
        join_str = "ma|<author:7><seqid:7>|v "
            "v|<author><seqid>|<voter:7> ";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|votes join");
        j->set_jvt(jvt_count_match);
        server_.add_join(start, end, j);

        // Materialize comments
        join_str = "ma|<author:7><seqid:7>|c|<cid:7>|<commenter:7> "
            "c|<author><seqid>|<cid>|<commenter> ";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|comments join");
        server_.add_join(start, end, j);
        
        // Materialize karma inline
        std::cout << "Materializing karma inline with articles.\n";
        join_str = "ma|<aid:14>|k|<cid:7>|<commenter:7> "
            "c|<aid>|<cid>|<commenter> "
            "v|<commenter><seq:7>|<voter:7>";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|karma join");
        j->set_jvt(jvt_count_match);
        start = "ma|";
        end = "ma}";
        server_.add_join(start, end, j);
    } else {
        // Materialize karma in a separate table
        j = new pq::Join;
        join_str = "k|<author:7> "
            "a|<author:7><seqid:7> "
            "v|<author><seqid>|<voter:7>";
        start = "k|";
        end = "k}";
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid karma join");
        j->set_jvt(jvt_count_match);
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
    sprintf(buf1, "k|");
    sprintf(buf2, "k}");
    server_.validate(Str(buf1, 2), Str(buf2, 2));
    if (hp_.log()) {
        std::cout << ": karma scan [" << buf1 << "," << buf2 << ")\n";
        auto bit = server_.lower_bound(Str(buf1, 2)),
            eit = server_.lower_bound(Str(buf2, 2));
        for (; bit != eit; ++bit)
            std::cout << "  " << bit->key() << ": " << bit->value() << "\n";
        std::cout << ": end karma scan [" << buf1 << "," << buf2 << ")\n";
        std::cout << ": my karma table:\n";
        for (uint32_t i = 0; i < hp_.nusers(); i++)
            std::cout << " k " << i << ": " << hp_.karma(i) << "\n";
        std::cout << ": end my karma table\n";
    }

    sprintf(buf1, "ma|");
    sprintf(buf2, "ma}");
    server_.validate(Str(buf1, 3), Str(buf2, 3));

    if (hp_.log()) {
        std::cout << ": ma scan [" << buf1 << "," << buf2 << ")\n";
        auto bit = server_.lower_bound(Str(buf1, 3)),
            eit = server_.lower_bound(Str(buf2, 3));
        for (; bit != eit; ++bit)
            std::cout << "  " << bit->key() << ": " << bit->value() << "\n";
        std::cout << ": end ma scan [" << buf1 << "," << buf2 << ")\n";
    }

    std::cout << "Finished validate.\n";
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
