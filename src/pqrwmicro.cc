#include "pqrwmicro.hh"
#include "pqserver.hh"
#include "time.hh"
#include <stdlib.h>
#include <sys/time.h>
#include <sys/resource.h>
#define DO_PERF 0
#if DO_PERF
#include <sys/prctl.h>
#include <sys/wait.h>
#endif

namespace pq {

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
    int time = 1;
    for (int u = 0; u < nuser_; ++u)
        for (int i = 0; i < 10; ++i) {
            sprintf(buf, "p|%05u|%010u", u, ++time);
            server_.insert(buf, String("She likes movie moby"));
        }
    delete[] b;
    j_ = new Join;
    String jtext = "t|<user_id:5>|<time:10>|<poster_id:5> = "
                   "using s|<user_id>|<poster_id> "
                   "copy p|<poster_id>|<time>";
    if (!push_)
        jtext.append(" pull");
    mandatory_assert(j_->assign_parse(jtext.c_str()));
    server_.add_join(Str("t|"), Str("t}"), j_);
    if (prerefresh_) {
        const int nu_active = nuser_ * pactive_ / 100;
        char buf1[128], buf2[128];
        for (int i = 0; i < nu_active; ++i) {
            int u = i;
            sprintf(buf1, "t|%05u|%010u", u, 0);
            sprintf(buf2, "t|%05u}", u);
            server_.validate(Str(buf1, 18), Str(buf2, 8));
            server_.count(Str(buf1, 18), Str(buf2, 8));
        }
    }
}

void RwMicro::run() {
    char buf1[128], buf2[128];
    int time = 100000000, nread = 0, npost = 0, nrefresh = 0;
    int* loadtime = new int[nuser_];
    double trefresh = 0;
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
    Table& t = server_.make_table("t");
    for (int i = 0; i < nops_; ++i) {
        if (random() % 100 < prefresh_) {
            struct timeval optv[2];
            gettimeofday(&optv[0], NULL);
            int u = random() % nu_active;
            ++nrefresh;
            sprintf(buf1, "t|%05u|%010u", u, loadtime[u] + 1);
            sprintf(buf2, "t|%05u}", u);
            server_.validate(Str(buf1, 18), Str(buf2, 8));
            if (push_)
                nread += server_.count(Str(buf1, 18), Str(buf2, 8));
            else {
                nread += t.size();
                t.flush();
            }
            loadtime[u] = time;
            gettimeofday(&optv[1], NULL);
            trefresh += to_real(optv[1] - optv[0]);
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
        .set("refresh_time", trefresh)
        .merge(server_.stats());
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
    delete[] loadtime;
}

}
