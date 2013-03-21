// -*- mode: c++ -*-
#include "pqtwitternew.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "pqremoteclient.hh"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <cmath>
#include <fstream>
#include <sys/resource.h>

using std::endl;
using std::vector;
using std::pair;
using std::ostream;
using std::ofstream;

namespace pq {
const char TwitterNewPopulator::tweet_data[] = "................................"
                                               "................................"
                                               "................................"
                                               "................................"
                                               "................................";

TwitterUser::TwitterUser()
    : nbackpost_(0), npost_(0), nsubscribe_(0), nlogout_(0),
      nlogin_(0), ncheck_(0), nread_(0), load_time_(0), loggedin_(false),
      uid_((uint32_t)-1), nfollowers_(0), celeb_(false) {
}

TwitterUser::Compare::Compare(CompareField field) : field_(field) {
}

bool TwitterUser::Compare::operator() (const TwitterUser&a, const TwitterUser& b) const {
    switch(field_) {
        case comp_uid:
            return a.uid_ < b.uid_;
        case comp_nfollowers:
            return a.nfollowers_ < b.nfollowers_;
        case comp_check:
            return a.ncheck_ < b.ncheck_;
        default:
            mandatory_assert(false && "Unknown user comparison field.");
    }

    return false; // never
}

bool TwitterUser::Compare::operator() (const uint32_t& a, const TwitterUser& b) const {
    switch(field_) {
        case comp_uid:
            return a < b.uid_;
        case comp_nfollowers:
            return a < b.nfollowers_;
        case comp_check:
            return a < b.ncheck_;
        default:
            mandatory_assert(false && "Unknown user comparison field.");
    }

    return false; // never
}


TwitterNewPopulator::TwitterNewPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5000)),
      duration_(param["duration"].as_i(100000)),
      postlimit_(param["postlimit"].as_i(0)),
      push_(param["push"].as_b(false)),
      pull_(param["pull"].as_b(false)),
      log_(param["log"].as_b(false)),
      synchronous_(param["synchronous"].as_b(false)),
      overhead_(param["overhead"].as_b(false)),
      visualize_(param["visualize"].as_b(false)),
      verbose_(param["verbose"].as_b(false)),
      celebthresh_(param["celebrity"].as_i(0)),
      pct_active_(param["pactive"].as_d(70)),
      graph_file_(param["graph"].as_s("")),
      min_followers_(param["min_followers"].as_i(10)),
      min_subs_(param["min_subscriptions"].as_i(20)),
      max_subs_(param["max_subscriptions"].as_i(200)),
      shape_(param["shape"].as_d(55)) {

    assert(!(push_ && pull_));

    vector<double> op_weight(n_op, 0);

    if (overhead_) {
        op_weight[op_post] = 100;
        pct_active_ = 100;
    }
    else {
        // these are not percentages, but weights, so it is
        // possible for them to sum != 100. the important thing
        // is to get the right ratio of weights for your experiment.
        // for example, the real twitter sees (according to the video i watched)
        // a check:post ratio between 50:1 and 100:1 on a normal day.
        // they also see 10x more social graph changes than posts
        op_weight[op_post] = param["ppost"].as_d(1);
        op_weight[op_subscribe] = param["psubscribe"].as_d(10);
        op_weight[op_login] = param["plogin"].as_d(5);
        op_weight[op_logout] = param["plogout"].as_d(5);
        op_weight[op_check] = param["pread"].as_d(60);
    }

    op_dist_ = op_dist_type(op_weight.begin(), op_weight.end());
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

void TwitterNewPopulator::make_subscriptions(vector<pair<uint32_t, uint32_t> >& subs,
                                             generator_type& gen) {
    if (graph_file_)
        import_subscriptions(subs, gen);
    else
        synthetic_subscriptions(subs, gen);
}

void TwitterNewPopulator::synthetic_subscriptions(vector<pair<uint32_t, uint32_t> >& subs,
                                                  generator_type& gen) {
    uint32_t* sub_prob = subscribe_probabilities(gen);
    uint32_t* subvec = new uint32_t[(nusers_ + 31) / 32];
    rng_type rng(gen);
    vector<pair<uint32_t, uint32_t> > followers;

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

    make_followers(subs, followers, gen);
}

void TwitterNewPopulator::import_subscriptions(vector<pair<uint32_t, uint32_t> >& subs,
                                               generator_type& gen) {
    std::ifstream graph;
    graph.open(graph_file_.c_str());
    assert(graph.is_open() && "Could not open twitter social graph.");

    graph >> nusers_;

    uint32_t user;
    uint32_t follower;
    vector<pair<uint32_t, uint32_t>> followers;

    while(graph.good()) {
        graph >> user >> follower;
        subs.push_back(std::make_pair(follower, user));
        followers.push_back(std::make_pair(user, follower));
    }

    make_followers(subs, followers, gen);
}

void TwitterNewPopulator::make_followers(vector<pair<uint32_t, uint32_t>>& subs,
                                         vector<pair<uint32_t, uint32_t>>& followers,
                                         generator_type& gen) {

    users_ = vector<TwitterUser>(nusers_);
    for (auto& sub : subs)
        ++users_[sub.second].nfollowers_;

    vector<double> wpost(nusers_, 0);
    for (uint32_t u = 0; u < nusers_; ++u) {
        users_[u].uid_ = u;

        if (celebthresh_ && users_[u].nfollowers_ > celebthresh_)
            users_[u].celeb_ = true;

        // assign individual post weights that correlate with the number
        // of followers a user has. increase variance as the weight grows
        if (!users_[u].nfollowers_)
            continue;
        else if (users_[u].nfollowers_ < 10)
            wpost[u] = 0.1;
        else
            wpost[u] = gen() % (uint32_t)(std::log10(users_[u].nfollowers_) * 2) + 1;
    }

    post_dist_ = post_dist_type(wpost.begin(), wpost.end());
    uni_dist_ = uni_dist_type(0, nusers_ - 1);

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


void TwitterNewPopulator::print_subscription_statistics(ostream& stream) {
    using namespace boost::accumulators;

    if (!verbose_)
        return;

    accumulator_set<uint32_t, stats<tag::variance> > acc;
    for (uint32_t i = 0; i != nusers_; ++i)
        acc(users_[i].nfollowers_);

    TwitterUser::Compare cmp(TwitterUser::comp_nfollowers);
    std::sort(users_.begin(), users_.end(), cmp);

    stream << nusers_ << " USERS HAVE SUBSCRIBERS:\n"
           << "  zero: " << (std::upper_bound(users_.begin(), users_.end(), 0, cmp) - users_.begin())
           << "  mean: " << mean(acc)
           << "  sdev: " << sqrt(variance(acc)) << "\n"
           << "  min " << users_[0].nfollowers_
           << ", 1% " << users_[(int) (0.01 * nusers_)].nfollowers_
           << ", 10% " << users_[(int) (0.10 * nusers_)].nfollowers_
           << ", 25% " << users_[(int) (0.25 * nusers_)].nfollowers_
           << ", 50% " << users_[(int) (0.50 * nusers_)].nfollowers_
           << ", 75% " << users_[(int) (0.75 * nusers_)].nfollowers_
           << ", 90% " << users_[(int) (0.90 * nusers_)].nfollowers_
           << ", 99% " << users_[(int) (0.99 * nusers_)].nfollowers_
           << ", max " << users_[nusers_ - 1].nfollowers_ << "\n";

    std::sort(users_.begin(), users_.end(), TwitterUser::Compare(TwitterUser::comp_uid));
}

void TwitterNewPopulator::print_visualization() {
    if (!visualize_)
        return;

    ofstream ppu("ppu.txt");
    ofstream cpu("cpu.txt");
    assert(ppu.good());
    assert(cpu.good());

    std::sort(users_.begin(), users_.end(), TwitterUser::Compare(TwitterUser::comp_nfollowers));
    for (uint32_t i = 0; i < nusers_; ++i)
        ppu << users_[i].nfollowers_ << " " << (users_[i].nbackpost_ +  users_[i].npost_) << endl;

    std::sort(users_.begin(), users_.end(), TwitterUser::Compare(TwitterUser::comp_check));
    for (uint32_t i = 0; i <= 99; ++i)
        cpu << i << " " << users_[(uint32_t)((double)i / 100 * nusers_)].ncheck_ << endl;
    cpu << 100 << " " << users_[nusers_ - 1].ncheck_ << endl;

    ppu.close();
    cpu.close();

    // todo: should sort back by uid? yes if this is not called at the end
    // of the application execution.
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
    twait { tr->populate(make_event()); }
    twait { tr->run(make_event()); }
    delete tr;
    delete shim;
    delete rc;
}

} // namespace pq
