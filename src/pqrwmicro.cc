#include "pqrwmicro.hh"
#include "pqserver.hh"
#include "time.hh"
#include <stdlib.h>
#include <sys/time.h>
#include <sys/resource.h>
#if DO_PERF
#include <sys/prctl.h>
#include <sys/wait.h>
#endif

namespace pq {

struct validate_args {
    Str first;
    Str last;
    Match match;
    Server* server;
};

static void pull(validate_args& va, int joinpos, Join* j) {
    uint8_t kf[128], kl[128];
    int kflen = j->expand_first(kf, j->source(joinpos),
                                va.first, va.last, va.match);
    int kllen = j->expand_last(kl, j->source(joinpos),
                               va.first, va.last, va.match);
    assert(Str(kf, kflen) <= Str(kl, kllen));

    SourceRange* r = 0;
    if (joinpos + 1 == j->nsource())
        r = j->make_source(*va.server, va.match,
                           Str(kf, kflen), Str(kl, kllen));

    auto it = va.server->lower_bound(Str(kf, kflen));
    auto ilast = va.server->lower_bound(Str(kl, kllen));

    Match::state mstate(va.match.save());
    const Pattern& pat = j->source(joinpos);

    for (; it != ilast; ++it) {
	if (it->key().length() != pat.key_length())
            continue;
        // XXX PERFORMANCE can prob figure out ahead of time whether this
        // match is simple/necessary
        if (pat.match(it->key(), va.match)) {
            if (r)
                r->notify(it.operator->(), String(), SourceRange::notify_insert);
            else
                pull(va, joinpos + 1, j);
        }
        va.match.restore(mstate);
    }
    delete r;
}

static int pull(Str first, Str last, Server& server, Join* j) {
    Str tname = table_name(first, last);
    assert(tname);
    Table& t = server.make_table(tname);
    validate_args va{first, last, Match(), &server};
    pull(va, 0, j);
    size_t count = t.size();
    t.clear();
    return count;
}

void RwMicro::populate() {
    char buf[128];
    srandom(1328);
    bool *b = new bool[nuser_];
    for (int u = 0; u < nuser_; ++u) {
        bzero(b, nuser_ * sizeof(*b));
        b[u] = true;
        for (int j = 0; j < nfollower_; ++j) {
            int follower;
            while (b[follower = (random() % nuser_)]);
            b[follower] = true;
            sprintf(buf, "s|%05u|%05u", follower, u);
            server_.insert(Str(buf, 13), String(""));
        }
    }
    delete[] b;
    j_ = new Join;
    bool ok = j_->assign_parse("t|<user_id:5>|<time:10>|<poster_id:5> = "
                               "using s|<user_id>|<poster_id> "
                               "copy p|<poster_id>|<time>");
    mandatory_assert(ok);
    if (push_)
        server_.add_join(Str("t|"), Str("t}"), j_);
}

void RwMicro::run() {
    char buf1[128], buf2[128];
    int time = 100, nread = 0, npost = 0, nrefresh = 0;
    int* loadtime = new int[nuser_];
    struct rusage ru[2];
    struct timeval tv[2];
    srandom(18181);
    bzero(loadtime, sizeof(*loadtime) * nuser_);
#if DO_PERF
    // perf profiling
    {
        String me(getpid());
        pid_t pid = fork();
        if (!pid) {
            prctl(PR_SET_PDEATHSIG, SIGINT);
            execlp("perf", "perf", "record", "-g", "-p", me.c_str(), NULL);
            exit(0);
        }
    }
#endif
    gettimeofday(&tv[0], NULL);
    getrusage(RUSAGE_SELF, &ru[0]);
    const int nu_active = nuser_ * pactive_ / 100;
    for (int i = 0; i < nops_; ++i) {
        if (i >= (nops_ - nu_active) || (random() % 100 < prefresh_)) {
            int u;
            if (i >= nops_ - nu_active)
                u = i - (nops_ - nu_active);
            else
                u = random() % nu_active;
            ++nrefresh;
            sprintf(buf1, "t|%05u|%010u", u, loadtime[u] + 1);
            sprintf(buf2, "t|%05u}", u);
            if (push_) {
                server_.validate(Str(buf1, 18), Str(buf2, 8));
                nread += server_.count(Str(buf1, 18), Str(buf2, 8));
            } else
                nread += pull(Str(buf1, 18), Str(buf2, 8), server_, j_);
            loadtime[u] = time;
        } else {
            int poster = random() % nuser_;
            sprintf(buf1, "p|%05u|%010u", poster, ++time);
            server_.insert(buf1, String("She likes movie moby"));
            ++npost;
        }
    }
    getrusage(RUSAGE_SELF, &ru[1]);
    gettimeofday(&tv[1], NULL);
    Json stats = Json()
        //.set("expected_post_read", pactive_)
        //.set("actual_post_read", nread * 100.0 / (std::max(npost, 1) * nfollower_))
        .set("expected_prefresh", prefresh_)
        .set("actual_prefresh", nrefresh * 100.0 / nops_)
        .set("nposts", npost)
        .set("total_ops", nops_)
	.set("nposts_read", nread)
        .set("nrefresh", nrefresh)
	.set("user_time", to_real(ru[1].ru_utime - ru[0].ru_utime))
        .set("system_time", to_real(ru[1].ru_stime - ru[0].ru_stime))
        .set("real_time", to_real(tv[1] - tv[0]))
        .merge(server_.stats());
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
    delete[] loadtime;
}

}
