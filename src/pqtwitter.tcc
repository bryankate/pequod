// -*- mode: c++ -*-
#include "pqtwitter.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "pqremoteclient.hh"
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
      synchronous_(param["synchronous"].as_b(false)),
      min_followers_(param["min_followers"].as_i(10)),
      min_subs_(param["min_subscriptions"].as_i(20)),
      max_subs_(param["max_subscriptions"].as_i(200)),
      shape_(param["shape"].as_d(55)),
      duration_(param["duration"].as_i(1000000)),
      ppost_(param["ppost"].as_i(2)),
      psubscribe_(param["psubscribe"].as_i(3)) {
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

    stream << nusers_ << " USERS HAVE SUBSCRIBERS:\n"
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

tamed void run_twitter_remote(TwitterPopulator& tp, int client_port) {
    tvars {
        tamer::fd fd;
        RemoteClient* rc;
        TwitterShim<RemoteClient>* shim;
        TwitterRunner<TwitterShim<RemoteClient> >* tr;
    }
    std::cerr << "connecting to port " << client_port << "\n";
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, client_port, make_event(fd)); }
    if (!fd) {
        std::cerr << "port " << client_port << ": "
                  << strerror(-fd.error()) << "\n";
        exit(1);
    }
    rc = new RemoteClient(fd);
    shim = new TwitterShim<RemoteClient>(*rc);
    tr = new TwitterRunner<TwitterShim<RemoteClient> >(*shim, tp);
    tr->populate();
    twait { tr->run(make_event()); }
    delete tr;
    delete shim;
    delete rc;
}

} // namespace pq
