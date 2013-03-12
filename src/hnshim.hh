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
    void initialize(bool, bool ma, bool, preevent<R> e) {
        mandatory_assert(!ma, "unimplemented: materializing all");
        e();
    }
    template <typename R>
    void post_populate(preevent<R> e) { (void)e; }
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
    void vote(uint32_t voter, uint32_t author, uint32_t aid, karmas_type& check_karmas, preevent<R> e) {
        // append voter to v|aid?
        (void) check_karmas;
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
    void initialize(bool log, bool ma, bool push, preevent<R> e);
    template <typename R>
    void post_populate(preevent<R> e) {
        String start = "k|";
        String end = "k}";
        
        if (ma_) {
            start = "ma|";
            end = "ma}";
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
    template <typename R>
    void post_article(uint32_t author, uint32_t aid, const String &v, preevent<R> e) {
        char buf[128];
        sprintf(buf, "a|%07d%07d", author, aid);
        server_.insert(Str(buf, 16), v);
        if (log_)
            printf("post %.16s\n", buf);
        sprintf(buf, "v|%07d%07d|%07d", author, aid, author);
        server_.insert(Str(buf, 24), Str("1", 1));
        if (log_)
            printf("vote %.24s\n", buf);
        e();
    }
    template <typename R>
    void post_comment(uint32_t commentor, uint32_t author, uint32_t aid, uint32_t cid,
                      const String &v, preevent<R> e) {
        char buf[128];
        sprintf(buf, "c|%07d%07d|%07d|%07d", author, aid, cid, commentor);
        server_.insert(Str(buf, 32), v);
        if (log_)
            printf("comment  %.32s\n", buf);
        e();
    }
    template <typename R>
    void vote(uint32_t voter, uint32_t author, uint32_t aid, karmas_type& check_karmas, preevent<R> e) {
        char buf[128];
        sprintf(buf, "v|%07d%07d|%07d", author, aid, voter);
        server_.insert(Str(buf, 24), Str("", 0));
        if (push_) {
            sprintf(buf, "k|%07d", author);
            char buf1[128];
            sprintf(buf1, "%07d", check_karmas[author]);
            server_.insert(Str(buf, 9), Str(buf1, 7));
        }
        if (log_)
            printf("vote %.24s\n", buf);
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
    bool push_;
    S& server_;
};

template <typename S> template <typename R>
void PQHackerNewsShim<S>::initialize(bool log, bool ma, bool push, preevent<R> e) {
    log_ = log;
    ma_ = ma;
    push_ = push;

    if (push) {
        e();
        return;
        // dont' set up any joins
        // on each vote, +1 the k range
    }

    String start, end, join_str;
    bool valid;
    pq::Join* j;
    if (ma) {
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
    sprintf(buf1, "ma|%07d%07d|", author, aid);
    sprintf(buf2, "ma|%07d%07d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 18)),
         eit = server_.lower_bound(Str(buf2, 18));
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
    sprintf(buf1, "a|%07d%07d", author, aid);
    auto ait = server_.find(Str(buf1, 16));
    mandatory_assert(ait != NULL);
    if (log_)
        std::cout << "read " << ait->key() << ":" << ait->value() << "\n";

    // Comments and Karma
    sprintf(buf1, "c|%07d%07d|", author, aid);
    sprintf(buf2, "c|%07d%07d}", author, aid);
    auto bit = server_.lower_bound(Str(buf1, 17)),
         eit = server_.lower_bound(Str(buf2, 17));
    for (; bit != eit; ++bit) {
        if (log_) {
            std::cout << "  c " << bit->key() << ": " << bit->value() << "\n";
            get_karma(extract_spkey(3, bit->key()), check_karmas);
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
        if (log_)
            std::cout << "  v " << cit->key() << ": " << cit->value() << "\n";
    }
}

template <typename S>
void PQHackerNewsShim<S>::get_karma(String user, karmas_type& check_karmas) {
    char buf3[128];
    sprintf(buf3, "k|%s", user.c_str());
    auto kbit = server_.find(Str(buf3, 9));
    uint32_t karma = 0;
    if (kbit == NULL)  {
        mandatory_assert(check_karmas[atoi(user.c_str())] == 0);
    } else if (kbit != NULL) {
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
    void initialize(bool log, bool ma, bool push, preevent<R> e) {
        ma_ = ma;
        log_ = log;
        push_ = push;
        e();
    }

    template <typename R>
    void post_populate(preevent<R> e) {
        e();
    }

    template <typename R>
    void post_article(uint32_t author, uint32_t aid, const String &v, preevent<R> e) {
        char buf[128];
        sprintf(buf, "INSERT INTO articles VALUES (%d, %d, '%s')", aid, author, v.data());
        pg_.insert(buf);
        if (log_)
            printf("post %07d\n", aid);
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
    void vote(uint32_t voter, uint32_t author, uint32_t aid, karmas_type& check_karmas, preevent<R> e) {
        char buf[128];
        (void) check_karmas;
        sprintf(buf, "INSERT INTO votes values (%d, %d)", aid, voter);
        PGresult* res = pg_.insert(buf);
        int affected = atoi(PQcmdTuples(res));
        mandatory_assert(affected == 1);
        if (log_)
            printf("vote %d %d authored by %d\n", aid, voter, author);
        e();
    }
    template <typename R>
    void read_article(uint32_t aid, uint32_t author, karmas_type& check_karmas, preevent<R> e) {
        (void)author;
        if (log_) 
            printf("Reading article %d\n", aid);
        char buf[512];
        if (ma_) {
            // Materialized karma table, query it
            sprintf(buf, "SELECT articles.aid,articles.author,articles.link,"
                                 "comments.cid,comments.commenter,comments.comment,"
                                 "karma_mv.karma,count(votes.aid) as vote_count "
                         "FROM articles "
                         "LEFT OUTER JOIN comments ON articles.aid=comments.aid "
                         "LEFT OUTER JOIN karma_mv ON comments.commenter=karma_mv.author "
                         "JOIN votes ON articles.aid=votes.aid "
                         "WHERE articles.aid = %d "
                         "GROUP BY articles.aid,comments.cid,karma_mv.karma", aid);
        } else {
            // No karma_mv
            sprintf(buf, "SELECT articles.aid,articles.author,articles.link,"
                                 "comments.cid,comments.commenter,comments.comment,"
                                 "karma.karma,count(votes.aid) as vote_count "
                         "FROM articles "
                         "LEFT JOIN comments ON articles.aid=comments.aid "
                         "LEFT JOIN "
                           "(SELECT articles.author, count(*) as karma FROM articles, votes WHERE "
                           "articles.aid = votes.aid GROUP BY articles.author) AS karma "
                           "ON comments.commenter=karma.author "
                         "JOIN votes ON articles.aid=votes.aid "
                         "WHERE articles.aid = %d "
                         "GROUP BY articles.aid,comments.cid,karma.karma", aid);
        }
        // XXX: should use more general return type
        PGresult* res = pg_.query(buf);
        if (log_) {
            printf("aid\tauthor\tlink\t\tcid\tuser\tcomment\tkarma\tvotes\n");
            for (int i = 0; i < PQntuples(res); i++) {
                for (int j = 0; j < PQnfields(res); j++)
                    std::cout << PQgetvalue(res, i, j) << "\t";
                std::cout << "\n";
            }
        }

        // Check karma
        for (int i = 0; i < PQntuples(res); i++) {
            uint32_t karma = atoi(PQgetvalue(res, i, 6));
            String user = PQgetvalue(res, i, 4);
            if (user == "")
                continue;
            uint32_t my_karma = check_karmas[atoi(user.c_str())];
            if (karma > my_karma + 2 || my_karma > karma + 2) {
                printf("Karma problem. mine: %d db's: %d user: %s\n", my_karma, karma, user.c_str());
                mandatory_assert(false && "Karma mismatch");
            }
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
    bool push_;
};
#endif

};

#endif
