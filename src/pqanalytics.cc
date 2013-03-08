#include "pqanalytics.hh"
#include "time.hh"
#include <sys/resource.h>

using std::cout;
using std::cerr;
using std::endl;

namespace pq {

// convert a base 10 timestamp (in seconds) to base 60 string
inline String to_base60(uint32_t base10, uint32_t width = 8) {
    std::string result;
    char buff[4];

    do {
        sprintf(buff, "%02u", base10 % 60);
        result.insert(0, buff, 2);
        base10 /= 60;
    } while(base10 > 0);

    if (result.size() < width)
        result.insert(0, width - result.size(), '0');

    assert(result.size() == width);
    return String(result);
}

AnalyticsRunner::AnalyticsRunner(Server& server, const Json& param)
    : server_(server), param_(param),
      rng_(gen_), log_(param["log"].as_b(false)),
      push_(param["push"].as_b(false)),
      proactive_(param["proactive"].as_b(false)),
      buffer_(param["buffer"].as_b(false)),
      popduration_(param["popduration"].as_i(7200)),
      duration_(param["duration"].as_i(1728000)),
      pread_(param["pread"].as_i(1)), bytes_(0) {

    gen_.seed(param["seed"].as_i(112181));
}

void AnalyticsRunner::populate() {

    for (uint32_t time = 0; time < popduration_; ++time)
        record_bps(time);

    if (push_)
        return;

    Join* Bpm = new Join();
    Bpm->assign_parse("Bpm|<hour:4><min:2> "
                      "Bps|<hour><min><sec:2>");
    Bpm->set_jvt(jvt_sum_match);
    Bpm->ref();

    server_.add_join("Bpm|", "Bpm}", Bpm);

    if (proactive_)
        server_.validate("Bpm|", "Bpm}");
}

void AnalyticsRunner::record_bps(uint32_t time) {
    uint32_t bytes = rng_(1048576);
    uint32_t flushbytes = 0;
    bool flush = false;
    String t60 = to_base60(time);
    String t60min(String(t60.data(), t60.data() + 6));

    if (time && time % 60 == 0) {
        flush = true;
        flushbytes = bytes_;
        bytes_ = 0;
    }

    server_.insert(String("Bps|") + t60, String(bytes));
    bytes_ += bytes;

    if (!buffer_) {
        if (log_)
            bpm_[String("Bpm|") + t60min] += bytes;

        if (push_)
            server_.insert(String("Bpm|") + t60min, String(bytes_));
    }
    else if (flush) {
        String t60prevmin = to_base60(time-1).substring(0,6);

        if (log_)
            bpm_[String("Bpm|") + t60prevmin] = flushbytes;

        if (push_)
            server_.insert(String("Bpm|") + t60prevmin, String(flushbytes));
    }
}

void AnalyticsRunner::run() {
    struct rusage ru[2];
    struct timeval tv[2];
    uint32_t nquery = 0;
    uint32_t nread = 0;

    getrusage(RUSAGE_SELF, &ru[0]);
    gettimeofday(&tv[0], 0);

    for (uint32_t time = popduration_; time != popduration_ + duration_; ++time) {
        record_bps(time);

        if (rng_(100) < pread_) {
            ++nquery;

            String kf = String("Bpm|") + to_base60(time - 1200);
            String kl = String("Bpm|") + to_base60(time);

            if (!push_)
                server_.validate(kf.substring(0, 8), kl);

            if (log_) {
                uint32_t returned = 0;
                auto i = server_.lower_bound(kf);
                auto iend = server_.lower_bound(kl);

                for (; i != iend; ++i) {
                    auto snap = bpm_.find(i->key());

                    if (snap == bpm_.end()) {
                        cerr << "Missing returned Bpm value in snapshot: " << i->key() << endl;
                        goto error;
                    }
                    else if (snap->second != i->value().to_i()) {
                        cerr << "Value for " << i->key() << " (" << i->value()
                             << ") does not match snapshot ("
                             << snap->second << ")" << endl;
                        goto error;
                    }

                    ++returned;
                }

                uint32_t dist = std::distance(bpm_.lower_bound(kf),
                                              bpm_.lower_bound(kl));
                if (returned != dist) {
                    cerr << "Number of Bpm results (" << returned
                         << ") does not match local snapshot ("
                         << dist << ")." << endl;
                    goto error;
                }

                nread += returned;
            }
            else {
                nread += server_.count(kf, kl);
            }
        }
    }

    {
        getrusage(RUSAGE_SELF, &ru[1]);
        gettimeofday(&tv[1], 0);
        Json stats = server_.stats().set("nquery", nquery)
                                        .set("npoints_read", nread)
                                        .set("system_time", to_real(ru[1].ru_stime - ru[0].ru_stime))
                                        .set("real_time", to_real(tv[1] - tv[0]));
        cout << stats.unparse(Json::indent_depth(4)) << endl;
        return;
    }

    error:
    server_.print(cerr);
    mandatory_assert(false);
}

}
