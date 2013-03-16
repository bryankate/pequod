#include "pqrwmicro.hh"
#include "pqserver.hh"
#include "time.hh"
#include <stdlib.h>
#include <sys/time.h>
#include <sys/resource.h>

namespace pq {

void RwMicro::populate() {
    Json j;
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
    pq::Join* join = new Join;
    bool ok = join->assign_parse("t|<user_id:5>|<time:10>|<poster_id:5> = "
                                 "using s|<user_id>|<poster_id> "
                                 "copy p|<poster_id>|<time>");
    mandatory_assert(ok);
    server_.add_join(Str("t|"), Str("t}"), join);
}

void RwMicro::run() {
    char buf1[128], buf2[128];
    int time = 100, nread = 0;
    const int nrefresh = nuser_ * pread_ / 100;
    int* loadtime = new int[nuser_];
    struct rusage ru[2];
    struct timeval tv[2];
    srandom(18181);
    bzero(loadtime, sizeof(*loadtime) * nuser_);
    gettimeofday(&tv[0], NULL);
    getrusage(RUSAGE_SELF, &ru[0]);
    for (int i = 0; i < d_; ++i) {
        for (int j = 0; j < pspost_; ++j) {
            int poster = random() % nuser_;
            sprintf(buf1, "p|%05u|%010u", poster, ++time);
            server_.insert(buf1, String("She likes movie moby"));
        }
        for (int j = 0; j < nrefresh; ++j) {
            sprintf(buf1, "t|%05u|%010u", j, loadtime[j] + 1);
            sprintf(buf2, "t|%05u}", j);
            server_.validate(Str(buf1, 18), Str(buf2, 8));
            nread += server_.count(Str(buf1, 18), Str(buf2, 8));
            loadtime[j] = time;
        }
    }
    getrusage(RUSAGE_SELF, &ru[1]);
    gettimeofday(&tv[1], NULL);
    Json stats = Json().set("expected_pread", pread_)
        .set("actual_pread", nread * 100.0 / (d_ * pspost_ * nfollower_))
        .set("nposts", d_ * pspost_)
        .set("ninserts", d_ * pspost_ * nfollower_)
        .set("nrefresh_per_second", nrefresh)
	.set("nposts_read", nread)
	.set("user_time", to_real(ru[1].ru_utime - ru[0].ru_utime))
        .set("system_time", to_real(ru[1].ru_stime - ru[0].ru_stime))
        .set("real_time", to_real(tv[1] - tv[0]))
        .merge(server_.stats());
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
    delete[] loadtime;
}

}
