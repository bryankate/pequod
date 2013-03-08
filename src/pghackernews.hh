#include "str.hh"
#include "string.hh"
#include <sys/resource.h>
#include "json.hh"
#include "time.hh"
#include "pgclient.hh"
#include <boost/random.hpp>
#include "hnpopulator.hh"
#include "check.hh"

namespace pq {

template <typename C>
class SQLHackernewsShim {
  public:
    SQLHackernewsShim(C& pg) : pg_(pg) {
    }
    template <typename R>
    void initialize(bool log, bool ma, preevent<R> e) {
        ma_ = ma;
        log_ = log;
        CHECK_EQ(system("psql -d hn < db/hn/schema.sql > /dev/null"), 0);
        if (ma_)
            CHECK_EQ(system("psql -d hn < db/hn/views.sql > /dev/null"), 0);
        e();
    }
    template <typename R>
    void post_article(uint32_t author, uint32_t aid, const String &v, preevent<R> e) {
        char buf[128];
        sprintf(buf, "INSERT INTO articles VALUES (%d, %d, '%s')", aid, author, v.data());
        pg_.insert(buf);
        if (log_)
            printf("post %05d\n", aid);
        sprintf(buf, "INSERT INTO votes VALUES (%d, %d)", aid, author);
        pg_.insert(buf);
        if (log_)
            printf("vote\n");
        e();
    }

    template <typename R>
    void post_comment(uint32_t commentor, uint32_t author, uint32_t aid, uint32_t cid,
                      const String &v, preevent<R> e) {
        (void)author;
        char buf[128];
        sprintf(buf, "INSERT INTO comments VALUES (%d, %d, %d, '%s')", cid, aid, commentor, v.data());
        pg_.insert(buf);
        if (log_)
            printf("comment  %d %d\n", aid, commentor);
        e();
    }

    template <typename R>
    void vote(uint32_t voter, uint32_t author, uint32_t aid, preevent<R> e) {
        (void)author;
        char buf[128];
        sprintf(buf, "INSERT INTO votes VALUES (%d, %d)", aid, voter);
        pg_.insert(buf);
        if (log_)
            printf("vote %d %d\n", aid, voter);
        e();
    }
    template <typename R>
    void read_article(uint32_t aid, uint32_t author, karmas_type& check_karmas, preevent<R> e) {
        (void)author;
        (void)check_karmas;
        char buf[512];
        if (ma_)
            // Materialized karma table, query it
            sprintf(buf, "SELECT articles.aid,articles.author,articles.link,"
                    "comments.cid,comments.comment,karma_mv.karma,count(votes.aid) "
                    "as vote_count FROM articles,comments,votes,karma_mv "
                    "WHERE articles.aid = %d AND comments.aid = articles.aid "
                    "AND votes.aid = articles.aid AND karma_mv.author=comments.commenter "
                    "GROUP BY articles.aid,comments.cid,karma_mv.karma", aid);
        else
            // No karma_mv
            sprintf(buf, "SELECT articles.aid,articles.author,articles.link,"
                    "comments.cid,comments.comment,count(votes.aid) "
                    "as vote_count, karma.karma FROM articles,comments,votes, "
                    "(SELECT articles.author, count(*) as karma FROM articles, votes WHERE "
                    "articles.aid = votes.aid GROUP BY articles.author) AS karma "
                    "WHERE articles.aid = %d AND comments.aid = articles.aid "
                    "AND votes.aid = articles.aid AND karma.author=comments.commenter "
                    "GROUP BY articles.aid,comments.cid,karma.karma", aid);

        PGresult* res = pg_.query(buf);
        // aid author link cid comment karma votes
        if (log_)
            for (int i = 0; i < PQntuples(res); i++) {
                for (int j = 0; j < PQnfields(res); j++)
                    std::cout << PQgetvalue(res, i, j) << " ";
                std::cout << "\n";
            }
        pg_.done(res);
        e();
    }
    template <typename R>
    void stats(preevent<R, Json> e) {
        e(Json());
    }

  private:
    C& pg_;
    bool log_;
    bool ma_;
};

};
