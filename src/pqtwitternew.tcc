// -*- mode: c++ -*-
#include "pqtwitternew.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "pqremoteclient.hh"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <math.h>
#include <sys/resource.h>
#include <fstream>
#include "time.hh"
namespace pq {
const char TwitterNewPopulator::tweet_data[] = "................................................................................................................................................................";

TwitterNewPopulator::TwitterNewPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5000)),
      push_(param["push"].as_b(false)),
      pull_(param["pull"].as_b(false)),
      log_(param["log"].as_b(false)),
      synchronous_(param["synchronous"].as_b(false)),
      graph_file_(param["graph"].as_s("")),
      min_followers_(param["min_followers"].as_i(10)),
      min_subs_(param["min_subscriptions"].as_i(20)),
      max_subs_(param["max_subscriptions"].as_i(200)),
      shape_(param["shape"].as_d(55)),
      duration_(param["duration"].as_i(1000000)),
      ppost_(param["ppost"].as_i(2)),
      psubscribe_(param["psubscribe"].as_i(3)) {
}

uint32_t* TwitterNewPopulator::subscribe_probabilities(generator_type& gen) {
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

void TwitterNewPopulator::make_subscriptions(std::vector<std::pair<uint32_t, uint32_t> >& subs,
                                             generator_type& gen) {
    if (graph_file_)
        import_subscriptions(subs);
    else
        synthetic_subscriptions(subs, gen);
}

void TwitterNewPopulator::synthetic_subscriptions(std::vector<std::pair<uint32_t, uint32_t> >& subs,
                                               generator_type& gen) {
    uint32_t* sub_prob = subscribe_probabilities(gen);
    uint32_t* subvec = new uint32_t[(nusers_ + 31) / 32];
    rng_type rng(gen);
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
            subs.push_back(std::make_pair(i, other));
            followers.push_back(std::make_pair(other, i));
            subvec[other / 32] |= 1U << (other % 32);
        }
    }

    delete[] sub_prob;
    delete[] subvec;

    make_followers(subs, followers);
}


void TwitterNewPopulator::import_subscriptions(std::vector<std::pair<uint32_t, uint32_t> >& subs) {
    std::ifstream graph;
    graph.open(graph_file_.c_str());
    assert(graph.is_open() && "Could not open twitter social graph.");

    graph >> nusers_;

    uint32_t user;
    uint32_t follower;
    std::vector<std::pair<uint32_t, uint32_t> > followers;

    while(graph.good()) {
        graph >> user >> follower;
        subs.push_back(std::make_pair(follower, user));
        followers.push_back(std::make_pair(user, follower));
    }

    make_followers(subs, followers);
}

struct FollowersLess {
    bool operator() (const std::pair<uint32_t, uint32_t>& u, const uint32_t f) const {
        return u.first < f;
    }

    bool operator() (const uint32_t f, const std::pair<uint32_t, uint32_t>& u) const {
        return f < u.first;
    }
};


void TwitterNewPopulator::make_followers(std::vector<std::pair<uint32_t, uint32_t>>& subs,
                                         std::vector<std::pair<uint32_t, uint32_t>>& followers) {

    // sort users by the number of followers they have
    for (uint32_t u = 0; u < nusers_; ++u)
        nfollowers_.push_back(std::make_pair(0, u));

    for (auto& sub : subs)
        ++nfollowers_[sub.second].first;
    std::sort(nfollowers_.begin(), nfollowers_.end());

    std::vector<uint32_t> segs{1, 10, 100, 1000, 5000, 10000, 100000, 1000000};
    std::vector<double> npost{0.0001, 0.0001, 0.1, 0.15, 0.2, 0.25, 0.2, 0.15};
    std::vector<uint32_t> indices;

    for (uint32_t g = 0; g < segs.size(); ++g)
        indices.push_back(lower_bound(nfollowers_.begin(), nfollowers_.end(),
                                      segs[g], FollowersLess()) - nfollowers_.begin());

    // a top-level discrete distribution that chooses between segments
    graph_dist_ = graph_dist_type(indices.begin(), indices.end(), npost.begin());

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


void TwitterNewPopulator::print_subscription_statistics(std::ostream& stream) const {
    using namespace boost::accumulators;

    accumulator_set<uint32_t, stats<tag::variance> > acc;
    for (uint32_t i = 0; i != nusers_; ++i)
        acc(nfollowers_[i].first);

    stream << nusers_ << " USERS HAVE SUBSCRIBERS:\n"
           << "  zero: " << (std::upper_bound(nfollowers_.begin(),
                                              nfollowers_.end(),
                                              0, FollowersLess()) - nfollowers_.begin())
           << "  mean: " << mean(acc)
           << "  sdev: " << sqrt(variance(acc)) << "\n"
           << "  min " << nfollowers_[0].first
           << ", 1% " << nfollowers_[(int) (0.01 * nusers_)].first
           << ", 10% " << nfollowers_[(int) (0.10 * nusers_)].first
           << ", 25% " << nfollowers_[(int) (0.25 * nusers_)].first
           << ", 50% " << nfollowers_[(int) (0.50 * nusers_)].first
           << ", 75% " << nfollowers_[(int) (0.75 * nusers_)].first
           << ", 90% " << nfollowers_[(int) (0.90 * nusers_)].first
           << ", 99% " << nfollowers_[(int) (0.99 * nusers_)].first
           << ", max " << nfollowers_[nusers_ - 1].first << "\n";
}

tamed void run_twitter_new_remote(TwitterNewPopulator& tp, int client_port) {
    tvars {
        tamer::fd fd;
        RemoteClient* rc;
        TwitterNewShim<RemoteClient>* shim;
        TwitterNewRunner<TwitterNewShim<RemoteClient> >* tr;
    }
    std::cerr << "connecting to port " << client_port << "\n";
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, client_port, make_event(fd)); }
    if (!fd) {
        std::cerr << "port " << client_port << ": "
                  << strerror(-fd.error()) << "\n";
        exit(1);
    }
    rc = new RemoteClient(fd);
    shim = new TwitterNewShim<RemoteClient>(*rc);
    tr = new TwitterNewRunner<TwitterNewShim<RemoteClient> >(*shim, tp);
    tr->populate();
    twait { tr->run(make_event()); }
    delete tr;
    delete shim;
    delete rc;
}

} // namespace pq
