#include "pqtwitter.hh"
#include "json.hh"
#include "pqjoin.hh"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <math.h>
#include <sys/resource.h>
#include "time.hh"
namespace pq {
const char TwitterPopulator::tweet_data[] = "................................................................................................................................................................";

TwitterPopulator::TwitterPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5000)),
      push_(param["push"].as_b(false)),
      log_(param["log"].as_b(false)),
      min_followers_(param["min_followers"].as_i(10)),
      min_subs_(param["min_subscriptions"].as_i(20)),
      max_subs_(param["max_subscriptions"].as_i(200)),
      shape_(param["shape"].as_d(55)) {
}

uint32_t* TwitterPopulator::subscribe_probabilities(generator_type& gen) {
    uint32_t expected_subs = (min_subs_ + max_subs_) / 2;
    double* follow_shape = new double[nusers_];
    while (1) {
	double shape_effect = 0;
	for (uint32_t i = 0; i != nusers_; ++i) {
	    double x = pow((double) (i + 1) / nusers_, shape_);
	    follow_shape[i] = x;
	    shape_effect += x;
	}
        max_followers_ = (expected_subs - min_followers_) / (shape_effect / nusers_);
	if (max_followers_ <= nusers_ / 4)
            break;
        // change shape, try again
        shape_ = shape_ * 0.7;
        std::cerr << "RETRYING with shape " << shape_ << "...\n";
    }

    rng_type rng(gen);
    std::random_shuffle(follow_shape, follow_shape + nusers_, rng);

    uint32_t* sub_prob = new uint32_t[nusers_];
    assert(gen.min() == 0);
    double scale = gen.max() / (expected_subs * (double) nusers_);
    double pos = 0;
    for (uint32_t i = 0; i != nusers_; ++i) {
        pos += min_followers_ + follow_shape[i] * max_followers_;
        sub_prob[i] = (uint32_t) (pos * scale);
    }
    sub_prob[nusers_ - 1] = gen.max();

    delete[] follow_shape;
    return sub_prob;
}

void TwitterPopulator::create_subscriptions(generator_type& gen) {
    uint32_t* sub_prob = subscribe_probabilities(gen);
    uint32_t* subvec = new uint32_t[(nusers_ + 31) / 32];
    rng_type rng(gen);
    subs_.clear();
    std::vector<std::pair<uint32_t, uint32_t> > followers;

    for (uint32_t i = 0; i != nusers_; ++i) {
	memset(subvec, 0, sizeof(uint32_t) * ((nusers_ + 31) / 32));
	uint32_t nsubs = min_subs_ + rng(max_subs_ - min_subs_ + 1);
	for (uint32_t j = 0; j != nsubs; ++j) {
	    // pick follow
	    uint32_t other;
	    do {
		other = std::upper_bound(sub_prob, sub_prob + nusers_, gen()) - sub_prob;
	    } while (subvec[other / 32] & (1U << (other % 32)));
            subs_.push_back(std::make_pair(i, other));
            followers.push_back(std::make_pair(other, i));
            subvec[other / 32] |= 1U << (other % 32);
        }
    }

    delete[] sub_prob;
    delete[] subvec;

    followers_.clear();
    followers_.reserve(followers.size());
    follower_ptrs_.clear();
    follower_ptrs_.reserve(nusers_ + 1);

    std::sort(followers.begin(), followers.end());
    for (auto& sub : followers) {
        while (follower_ptrs_.size() <= sub.first)
            follower_ptrs_.push_back(followers_.size());
        followers_.push_back(sub.second);
    }
    while (follower_ptrs_.size() <= nusers_)
        follower_ptrs_.push_back(followers_.size());
}


void TwitterPopulator::print_subscription_statistics(std::ostream& stream) const {
    using namespace boost::accumulators;

    uint32_t* num_followers = new uint32_t[nusers_];
    memset(num_followers, 0, nusers_ * sizeof(uint32_t));
    for (auto& sub : subs_)
        ++num_followers[sub.second];
    std::sort(num_followers, num_followers + nusers_);

    accumulator_set<uint32_t, stats<tag::variance> > acc;
    for (uint32_t i = 0; i != nusers_; ++i)
        acc(num_followers[i]);

    stream << "USERS HAVE # SUBSCRIBERS:\n"
           << "  zero: " << (std::upper_bound(num_followers, num_followers + nusers_, 0) - num_followers)
           << "  mean: " << mean(acc)
           << "  sdev: " << sqrt(variance(acc)) << "\n"
           << "  min " << num_followers[0]
           << ", 1% " << num_followers[(int) (0.01 * nusers_)]
           << ", 10% " << num_followers[(int) (0.10 * nusers_)]
           << ", 25% " << num_followers[(int) (0.25 * nusers_)]
           << ", 50% " << num_followers[(int) (0.50 * nusers_)]
           << ", 75% " << num_followers[(int) (0.75 * nusers_)]
           << ", 90% " << num_followers[(int) (0.90 * nusers_)]
           << ", 99% " << num_followers[(int) (0.99 * nusers_)]
           << ", max " << num_followers[nusers_ - 1] << "\n";

    delete[] num_followers;
}


void TwitterRunner::post(uint32_t u, uint32_t time, Str value) {
    char buf[128];
    sprintf(buf, "p|%05d|%010u", u, time);
    server_.insert(Str(buf, 18), value, true);
    if (tp_.push())
        for (auto it = tp_.begin_followers(u);
             it != tp_.end_followers(u); ++it) {
            sprintf(buf, "t|%05u|%010u|%05u", *it, time, u);
            server_.insert(Str(buf, 24), value, false);
        }
}

void TwitterRunner::populate() {
    boost::mt19937 gen;
    gen.seed(0);

    tp_.create_subscriptions(gen);
    char buf[128];
    for (auto& x : tp_.subscriptions()) {
        sprintf(buf, "s|%05d|%05d", x.first, x.second);
        server_.insert(Str(buf, 13), Str("1", 1), true);
        if (tp_.log())
            printf("subscribe %.13s\n", buf);
    }

#if 0
    for (uint32_t u = 0; u != tp_.nusers(); ++u)
        for (int p = 0; p != 10; ++p) {
            auto post = tp_.random_post(gen);
            post(u, post.first, post.second);
            if (p == 9 && u % 1000 == 0)
                fprintf(stderr, "%u/%u ", u, tp_.nusers());
        }
#endif

    tp_.print_subscription_statistics(std::cout);

    if (!tp_.push()) {
        pq::Join* j = new pq::Join;
        j->assign_parse("t|<user_id:5>|<time:10>|<poster_id:5> "
                        "s|<user_id>|<poster_id> "
                        "p|<poster_id>|<time>");
        server_.add_join("t|", "t}", j);
    }
}

void TwitterRunner::run() {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    struct rusage ru[2];

    uint32_t time = 1000000000;
    uint32_t nusers = tp_.nusers();
    uint32_t post_end_time = time + nusers * 5;
    uint32_t end_time = post_end_time + 1000000;
    uint32_t* load_times = new uint32_t[nusers];
    for (uint32_t i = 0; i != nusers; ++i)
        load_times[i] = 0;
    char buf1[128], buf2[128];
    uint32_t npost = 0, nfull = 0, nupdate = 0;
    size_t nread = 0;
    getrusage(RUSAGE_SELF, &ru[0]);

    while (time != end_time) {
        uint32_t u = rng(nusers);
        uint32_t a = rng(100);
        if (time < post_end_time || a < 2) {
            if (tp_.log())
                printf("%d: post p|%05d|%010d\n", time, u, time);
            post(u, time, "?!?#*");
            ++npost;
        } else {
            uint32_t tx = load_times[u];
            if (!tx || a < 3) {
                tx = time - 32000;
                ++nfull;
            } else
                ++nupdate;
            sprintf(buf1, "t|%05d|%010d", u, tx);
            sprintf(buf2, "t|%05d}", u);
            server_.validate(Str(buf1, 18), Str(buf2, 8));
            if (tp_.log()) {
                std::cout << time << ": scan [" << buf1 << "," << buf2 << ")\n";
                auto bit = server_.lower_bound(Str(buf1, 18)),
                    eit = server_.lower_bound(Str(buf2, 8));
                for (; bit != eit; ++bit, ++nread)
                    std::cout << "  " << bit->key() << ": " << bit->value() << "\n";
            } else
                nread += server_.count(Str(buf1, 18), Str(buf2, 8));
            load_times[u] = time;
        }
        ++time;
    }

    getrusage(RUSAGE_SELF, &ru[1]);
    Json stats = Json().set("nposts", npost).set("nfull", nfull)
	.set("nupdate", nupdate).set("nposts_read", nread)
	.set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    stats.merge(server_.stats());
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
    delete[] load_times;
}

}
