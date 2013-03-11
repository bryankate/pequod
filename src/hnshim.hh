#ifndef HN_CLIENT_HH
#define HN_CLIENT_HH
#include "check.hh"

namespace pq {

typedef const std::vector<uint32_t> karmas_type;

template <typename C>
class HashHackerNewsShim {
  public:
    HashHackerNewsShim(C &c) : c_(c) {
    }
    template <typename R>
    void initialize(bool, bool ma, preevent<R> e) {
        mandatory_assert(!ma, "unimplemented: materializing all");
        e();
    }
    template <typename R>
    void post_article(uint32_t author, uint32_t aid, const String &v, preevent<R> e) {
        // add h|aid/author|v
        c_.set(String("h|") + String(aid), String(author) + String("|") + v);
        // XXX: append aid to ah|author?
        e();
    }
    template <typename R>
    void post_comment(uint32_t commentor, uint32_t author, uint32_t aid,
                      uint32_t cid, const String &v, preevent<R> e) {
        (void)author;
        // add c|cid/v
        c_.set(String("c|") + String(cid), v);
        // append commentor|cid to ac|aid
        char buf[128];
        sprintf(buf, "%d|%d\255", commentor, aid);
        c_.append(String("ac|") + String(aid), Str(buf, strlen(buf)));
        e();
    }
    template <typename R>
    void vote(uint32_t voter, uint32_t author, uint32_t aid, preevent<R> e) {
        // append voter to v|aid?
        c_.append(String("v|") + String(aid), String(voter) + String(","));
        // increment k|author
        c_.increment(String("k|") + String(author));
        e();
    }
    template <typename R>
    void read_article(uint32_t aid, uint32_t author, karmas_type &check_karmas,
                      preevent<R> e) {
        (void)author;
        (void)check_karmas;
        // get h|aid/hv
        size_t value_length;
        const char *hv = c_.get(String("h|") + String(aid), 0, &value_length);
        // get ac|aid/clist
        const char *cl = c_.get(String("ac|") + String(aid), 0, &value_length);
        // for each commentor/cid in clist:
        Str clist(cl, value_length);
        ssize_t ep;
        for (ssize_t s = 0; (ep = clist.find_left('\255', s)) != -1; s = ep + 1) {
            ssize_t p = clist.find_left('|', s);
            mandatory_assert(p != -1);
            Str commentor(cl + s, cl + p);
            Str cid(cl + p + 1, cl + ep);
            // get c|cid/cv
            c_.done_get(c_.get(String("c|") + cid, 0, &value_length));
            // get k|commentor/kc
            c_.done_get(c_.get(String("k|") + commentor, 0, &value_length));
        }
        c_.done_get(hv);
        c_.done_get(cl);
        e();
    }
    template <typename R>
    void stats(preevent<R, Json> e) {
        e(Json());
    }
  private:
    C& c_;
};

template <typename S>
class PQHackerNewsShim {
  public:
    PQHackerNewsShim(S& server) : server_(server) {}
    template <typename R>
    void initialize(bool log, bool ma, preevent<R> e);
    template <typename R>
    void post_article(uint32_t author, uint32_t aid, const String &v, preevent<R> e) {
        char buf[128];
        sprintf(buf, "a|%05d%05d", author, aid);
        server_.insert(Str(buf, 12), v);
        if (log_)
            printf("post %.12s\n", buf);
        sprintf(buf, "v|%05d%05d|%05d", author, aid, author);
        server_.insert(Str(buf, 18), Str("1", 1));
        if (log_)
            printf("vote %.18s\n", buf);
        e();
    }
    template <typename R>
    void post_comment(uint32_t commentor, uint32_t author, uint32_t aid, uint32_t cid,
                      const String &v, preevent<R> e) {
        char buf[128];
        sprintf(buf, "c|%05d%05d|%05d|%05d", author, aid, cid, commentor);
        server_.insert(Str(buf, 24), v);
        if (log_)
            printf("comment  %.24s\n", buf);
        e();
    }
    template <typename R>
    void vote(uint32_t voter, uint32_t author, uint32_t aid, preevent<R> e) {
        char buf[128];
        sprintf(buf, "v|%05d%05d|%05d", author, aid, voter);
        server_.insert(Str(buf, 18), Str("", 0));
        if (log_)
            printf("vote %.18s\n", buf);
        e();
    }
    template <typename R>
    void read_article(uint32_t aid, uint32_t author, karmas_type& check_karmas,
                      preevent<R> e);
    template <typename R>
    void stats(preevent<R, Json> e) {
        e(server_.stats());
    }
  private:
    void read_materialized(uint32_t aid, uint32_t author, karmas_type& check_karmas);
    void read_tables(uint32_t aid, uint32_t author, karmas_type& check_karmas);
    void get_karma(String user, karmas_type& check_karmas);

    bool log_;
    bool ma_;
    S& server_;
};

template <typename S> template <typename R>
void PQHackerNewsShim<S>::initialize(bool log, bool ma, preevent<R> e) {
    log_ = log;
    ma_ = ma;
    String start, end, join_str;
    bool valid;
    pq::Join* j;
    if (ma) {
        start = "ma|";
        end = "ma}";
        // Materialize articles
        join_str = "ma|<author:5><seqid:5>|a "
            "a|<author><seqid> ";
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
        join_str = "ma|<author:5><seqid:5>|c|<cid:5>|<commenter:5> "
            "c|<author><seqid>|<cid>|<commenter> ";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|comments join");
        server_.add_join(start, end, j);
        
        // Materialize karma inline
        std::cout << "Materializing karma inline with articles.\n";
        join_str = "ma|<aid:10>|k|<cid:5>|<commenter:5> "
            "c|<aid>|<cid>|<commenter> "
            "v|<commenter><seq:5>|<voter:5>";
        j = new pq::Join;
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid ma|karma join");
        j->set_jvt(jvt_count_match);
        server_.add_join(start, end, j);
    } else {
        // Materialize karma in a separate table
        j = new pq::Join;
        join_str = "k|<author:5> "
            "a|<author:5><seqid:5> "
            "v|<author><seqid>|<voter:5>";
        start = "k|";
        end = "k}";
        valid = j->assign_parse(join_str);
        mandatory_assert(valid && "Invalid karma join");
        j->set_jvt(jvt_count_match);
        server_.add_join(start, end, j);
    }
    server_.validate(start, end);
    if (log_) {
        std::cout << ": print validated [" << start << "," << end << ")\n";
        auto bit = server_.lower_bound(start),
             eit = server_.lower_bound(end);
        for (; bit != eit; ++bit)
            std::cout << "  " << bit->key() << ": " << bit->value() << "\n";
        std::cout << ": end print validated [" << start << "," << end << ")\n";
        std::cout << "Finished validate.\n";
    }
    e();
}

template <typename S> template <typename R>
void PQHackerNewsShim<S>::read_article(uint32_t aid, uint32_t author, 
                                   karmas_type& check_karmas, preevent<R> e) {
    if (ma_)
        read_materialized(aid, author, check_karmas);
    else
        read_tables(aid, author, check_karmas);
    e();
}

template <typename S>
void PQHackerNewsShim<S>::read_materialized(uint32_t aid, uint32_t author, karmas_type&) {
    char buf1[128], buf2[128];
    sprintf(buf1, "ma|%05d%05d|", author, aid);
    sprintf(buf2, "ma|%05d%05d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 14)),
         eit = server_.lower_bound(Str(buf2, 14));
    for (; bit != eit; ++bit) {
        String field = extract_spkey(2, bit->key());
        if (log_) {
            if (field == "a")
                std::cout << "read " << bit->key() << ": " << bit->value() << "\n";
            else
                std::cout << "  " << field << " " << bit->key() << ": " << bit->value() << "\n";
        }
    }
}

template <typename S>
void PQHackerNewsShim<S>::read_tables(uint32_t aid, uint32_t author, karmas_type& check_karmas) {
    char buf1[128], buf2[128];
    // Article
    sprintf(buf1, "a|%05d%05d", author, aid);
    auto ait = server_.find(Str(buf1, 12));
    mandatory_assert(ait != NULL);
    if (log_)
        std::cout << "read " << ait->key() << ":" << ait->value() << "\n";

    // Comments and Karma
    sprintf(buf1, "c|%05d%05d|", author, aid);
    sprintf(buf2, "c|%05d%05d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 13)),
         eit = server_.lower_bound(Str(buf2, 13));
    for (; bit != eit; ++bit) {
        if (log_) {
            std::cout << "  c " << bit->key() << ": " << bit->value() << "\n";
            get_karma(extract_spkey(3, bit->key()), check_karmas);
        }
    }

    // Votes
    sprintf(buf1, "v|%05d%05d|", author, aid);
    sprintf(buf2, "v|%05d%05d}", author, aid);
    auto cit = server_.lower_bound(Str(buf1, 13)),
         ceit = server_.lower_bound(Str(buf2, 13));
    uint32_t votect = 0;
    for (; cit != ceit; ++cit) {
        votect++;
        if (log_)
            std::cout << "  v " << cit->key() << ": " << cit->value() << "\n";
    }
}

template <typename S>
void PQHackerNewsShim<S>::get_karma(String user, karmas_type& check_karmas) {
    char buf3[128];
    sprintf(buf3, "k|%s", user.c_str());
    auto kbit = server_.find(Str(buf3, 7));
    uint32_t karma = 0;
    if (kbit != NULL) {
        karma = atoi(kbit->value().c_str());
        uint32_t my_karma = check_karmas[atoi(user.c_str())];
        mandatory_assert(karma == my_karma && "Karma mismatch");
        if (log_)
            std::cout << "  k " << user << ":" << karma << "\n";
    }
}

#if HAVE_POSTGRESQL_LIBPQ_FE_H
#include <postgresql/libpq-fe.h>
template <typename C>
class SQLHackernewsShim {
  public:
    SQLHackernewsShim(C& pg) : pg_(pg) {
    }
    template <typename R>
    void initialize(bool log, bool ma, preevent<R> e) {
        ma_ = ma;
        log_ = log;
        CHECK_EQ(system("psql -p 5477 -d hn < db/hn/schema.sql > /dev/null"), 0);
        if (ma_)
            CHECK_EQ(system("psql -p 5477 -d hn < db/hn/views.sql > /dev/null"), 0);
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
        // XXX: should use more general return type
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
#endif

};

#endif
