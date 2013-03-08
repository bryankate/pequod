#include "str.hh"
#include "string.hh"
#include <sys/resource.h>
#include "json.hh"
#include "time.hh"
#include "pgclient.hh"
#include <boost/random.hpp>
#include "hnpopulator.hh"

namespace pq {

class PGHackernewsRunner {
  public:
    PGHackernewsRunner(PostgresClient& pg, HackernewsPopulator& hp) 
        : hp_(hp), pg_(pg) {
    }

    void populate() {
        mandatory_assert(system("psql -d hn < db/hn/schema.sql > /dev/null") == 0);
        if (hp_.m())
            mandatory_assert(system("psql -d hn < db/hn/views.sql > /dev/null") == 0);
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
        std::cout << "Finished populate.\n";
    }

    void post_article(uint32_t author, uint32_t aid) {
        char buf[128];
        hp_.post_article(author, aid);
        sprintf(buf, "INSERT INTO articles VALUES (%d, %d, 'linklinklink')", aid, author);
        pg_.insert(buf);
        if (hp_.log()) {
            printf("post %05d\n", aid);
        }
        sprintf(buf, "INSERT INTO votes VALUES (%d, %d)", aid, author);
        pg_.insert(buf);
        if (hp_.log()) {
            printf("vote\n");
        }
    }

    void post_comment(uint32_t commentor, uint32_t aid) {
        char buf[128];
        sprintf(buf, "INSERT INTO comments VALUES (%d, %d, %d, 'commenttext')", hp_.next_comment(), aid, commentor);
        pg_.insert(buf);
        if (hp_.log()) {
            printf("comment  %d %d\n", aid, commentor);
        }
    }

    bool vote(uint32_t voter, uint32_t aid) {
        char buf[128];
        if (hp_.vote(aid, voter)) {
            sprintf(buf, "INSERT INTO votes VALUES (%d, %d)", aid, voter);
            pg_.insert(buf);
            if (hp_.log()) {
                printf("vote %d %d\n", aid, voter);
            }
            return true;
        }
        return false;
    }

    void read_article(uint32_t aid) {
        mandatory_assert(aid < hp_.narticles());
        char buf[512];
        if (hp_.m()) {
            // Materialized karma table, query it
            sprintf(buf, "SELECT articles.aid,articles.author,articles.link,"
                    "comments.cid,comments.comment,karma_mv.karma,count(votes.aid) "
                    "as vote_count FROM articles,comments,votes,karma_mv "
                    "WHERE articles.aid = %d AND comments.aid = articles.aid "
                    "AND votes.aid = articles.aid AND karma_mv.author=comments.commenter "
                    "GROUP BY articles.aid,comments.cid,karma_mv.karma", aid);
        } else {
            // No karma_mv
            sprintf(buf, "SELECT articles.aid,articles.author,articles.link,"
                    "comments.cid,comments.comment,count(votes.aid) "
                    "as vote_count, karma.karma FROM articles,comments,votes, "
                    "(SELECT articles.author, count(*) as karma FROM articles, votes WHERE "
                    "articles.aid = votes.aid GROUP BY articles.author) AS karma "
                    "WHERE articles.aid = %d AND comments.aid = articles.aid "
                    "AND votes.aid = articles.aid AND karma.author=comments.commenter "
                    "GROUP BY articles.aid,comments.cid,karma.karma", aid);
        }
        PGresult* res = pg_.query(buf);
        
        // aid author link cid comment karma votes
        for (int i = 0; i < PQntuples(res); i++) {
            for (int j = 0; j < PQnfields(res); j++) {
                if (hp_.log()) {
                    std::cout << PQgetvalue(res, i, j) << " ";
                }
            }
            if (hp_.log()) 
                std::cout << "\n";
        }
        pg_.done(res);
    }

    void run() {
        mandatory_assert(hp_.narticles() > 0);
        boost::mt19937 gen;
        gen.seed(13918);
        boost::random_number_generator<boost::mt19937> rng(gen);
        const uint32_t nops = hp_.nops();
        const uint32_t nusers = hp_.nusers();
        struct rusage ru[2];
        uint32_t nread = 0, npost = 0, ncomment = 0, nvote = 0;
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
         std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
    }
    
  private:
    HackernewsPopulator& hp_;
    PostgresClient& pg_;
};

};
