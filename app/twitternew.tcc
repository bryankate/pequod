// -*- mode: c++ -*-
#include "twitternew.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "pqmulticlient.hh"
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

TwitterGraphNode::TwitterGraphNode(uint32_t uid)
    : uid_(uid), nfollowers_(0) {
}

TwitterUser::TwitterUser(uint32_t uid)
    : TwitterGraphNode(uid), last_read_(0) {
}

TwitterGraphNode::Compare::Compare(CompareField field) : field_(field) {
}

bool TwitterGraphNode::Compare::operator() (const TwitterGraphNode* a, const TwitterGraphNode* b) const {
    switch(field_) {
        case comp_uid:
            return a->uid_ < b->uid_;
        case comp_nfollowers:
            return a->nfollowers_ < b->nfollowers_;
        default:
            mandatory_assert(false && "Unknown user comparison field.");
    }

    return false; // never
}

bool TwitterGraphNode::Compare::operator() (const uint32_t& a, const TwitterGraphNode* b) const {
    switch(field_) {
        case comp_uid:
            return a < b->uid_;
        case comp_nfollowers:
            return a < b->nfollowers_;
        default:
            mandatory_assert(false && "Unknown user comparison field.");
    }

    return false; // never
}


TwitterNewPopulator::TwitterNewPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5000)),
      ngroups_(param["ngroups"].as_i(1)),
      groupid_(param["groupid"].as_i(0)),
      duration_(param["duration"].as_i(100000) / ngroups_),
      popduration_(param["popduration"].as_i(0)),
      postlimit_(param["postlimit"].as_i(0) / ngroups_),
      checklimit_(param["checklimit"].as_i(0) / ngroups_),
      initialize_(param["initialize"].as_b(true)),
      populate_(param["populate"].as_b(true)),
      execute_(param["execute"].as_b(true)),
      push_(param["push"].as_b(false)),
      pull_(param["pull"].as_b(false)),
      fetch_(param["fetch"].as_b(false)),
      prevalidate_(param["prevalidate"].as_b(true)),
      prevalidate_inactive_(param["prevalidate_inactive"].as_b(false)),
      prevalidate_before_sub_(param["prevalidate_before_sub"].as_b(false)),
      writearound_(param["writearound"].as_b(false)),
      log_(param["log"].as_b(false)),
      synchronous_(param["synchronous"].as_b(false)),
      visualize_(param["visualize"].as_b(false)),
      binary_(param["binary"].as_b(true)),
      verbose_(param["verbose"].as_b(false)),
      celebthresh_(param["celebrity"].as_i(0)),
      pct_active_(param["pactive"].as_d(70)),
      graph_file_(param["graph"].as_s("")),
      min_followers_(param["min_followers"].as_i(10)),
      min_subs_(param["min_subscriptions"].as_i(20)),
      max_subs_(param["max_subscriptions"].as_i(200)),
      shape_(param["shape"].as_d(55)) {

    mandatory_assert(!(push_ && pull_));

    if (pull_ || push_) {
        prevalidate_ = false;
        prevalidate_inactive_ = false;
    }

    if (prevalidate_before_sub_)
        mandatory_assert(prevalidate_ || prevalidate_inactive_);

    vector<double> op_weight(n_op, 0);

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

void TwitterNewPopulator::make_subscriptions(generator_type& gen,
                                             vector<pair<uint32_t, uint32_t>>& subs) {
    if (graph_file_)
        import_subscriptions(gen, subs);
    else
        synthetic_subscriptions(gen, subs);
}

void TwitterNewPopulator::synthetic_subscriptions(generator_type& gen,
                                                  vector<pair<uint32_t, uint32_t>>& subs) {
    make_users();

    uint32_t* sub_prob = subscribe_probabilities(gen);
    uint32_t* subvec = new uint32_t[(nusers_ + 31) / 32];
    rng_type rng(gen);

    for (uint32_t i = 0; i != nusers_; ++i) {
	memset(subvec, 0, sizeof(uint32_t) * ((nusers_ + 31) / 32));
	uint32_t nsubs = min_subs_ + rng(max_subs_ - min_subs_ + 1);
	for (uint32_t j = 0; j != nsubs; ++j) {
	    // pick follow
	    uint32_t other;
	    do {
		other = std::upper_bound(sub_prob, sub_prob + nusers_, gen()) - sub_prob;
	    } while (other == i || (subvec[other / 32] & (1U << (other % 32))));

            ++users_[other]->nfollowers_;
	    if (in_group(i))
                subs.push_back(std::make_pair(i, other));
            subvec[other / 32] |= 1U << (other % 32);

#if TIMELINE_SANITY_CHECK
            users_[other]->followers_.push_back(i);
#endif
        }
    }

    delete[] sub_prob;
    delete[] subvec;

    finish_make_subscriptions(gen);
}

void TwitterNewPopulator::import_subscriptions(generator_type& gen,
                                               vector<pair<uint32_t, uint32_t>>& subs) {
    uint32_t user;
    uint32_t follower;
    std::ifstream graph;
    graph.open(graph_file_.c_str());
    assert(graph.is_open() && "Could not open twitter social graph.");

    graph >> nusers_;
    make_users();

    while(graph.good()) {
        graph >> user >> follower;
        ++users_[user]->nfollowers_;
        if (in_group(follower))
            subs.push_back(std::make_pair(follower, user));

#if TIMELINE_SANITY_CHECK
        users_[user]->followers_.push_back(follower);
#endif
    }

    finish_make_subscriptions(gen);
}

void TwitterNewPopulator::finish_make_subscriptions(generator_type& gen) {

    vector<double> wpost(nusers_, 0);
    for (uint32_t u = 0; u < nusers_; ++u) {
        uint32_t nfollowers = users_[u]->nfollowers_;

        // assign individual post weights that correlate with the number
        // of followers a user has. increase variance as the weight grows
        if (!nfollowers)
            continue;
        else if (nfollowers < 10)
            wpost[u] = 0.1;
        else
            wpost[u] = gen() % (uint32_t)(std::log10(nfollowers) * 2) + 1;
    }

    post_dist_ = post_dist_type(wpost.begin(), wpost.end());
    uni_dist_ = uni_dist_type(0, nusers_ - 1);
}

void TwitterNewPopulator::make_users() {

    groupbegin_ = 0;
    groupend_ = nusers_;

    if (ngroups_ > 1) {
        uint32_t gsize = nusers_ / ngroups_;
        groupbegin_ = gsize * groupid_;
        groupend_ = (groupid_ == ngroups_ - 1) ? nusers_ : (groupbegin_ + gsize);
    }

    group_uni_dist_ = uni_dist_type(groupbegin_, groupend_ - 1);

    for (uint32_t u = 0; u < nusers_; ++u)
        if (in_group(u))
            users_.push_back(new TwitterUser(u));
        else
            users_.push_back(new TwitterGraphNode(u));
}

bool TwitterNewPopulator::in_group(uint32_t u) const {
    return (u >= groupbegin_ && u < groupend_);
}


void TwitterNewPopulator::print_subscription_statistics(ostream& stream) {
    using namespace boost::accumulators;

    if (!verbose_ || ngroups_ > 1)
        return;

    accumulator_set<uint32_t, stats<tag::variance> > acc;
    for (uint32_t i = 0; i != nusers_; ++i)
        acc(users_[i]->nfollowers());

    TwitterGraphNode::Compare cmp(TwitterGraphNode::comp_nfollowers);
    std::sort(users_.begin(), users_.end(), cmp);

    stream << nusers_ << " USERS HAVE SUBSCRIBERS:\n"
           << "  zero: " << (std::upper_bound(users_.begin(), users_.end(), 0, cmp) - users_.begin())
           << "  mean: " << mean(acc)
           << "  sdev: " << sqrt(variance(acc)) << "\n"
           << "  min " << users_[0]->nfollowers()
           << ", 1% " << users_[(int) (0.01 * nusers_)]->nfollowers()
           << ", 10% " << users_[(int) (0.10 * nusers_)]->nfollowers()
           << ", 25% " << users_[(int) (0.25 * nusers_)]->nfollowers()
           << ", 50% " << users_[(int) (0.50 * nusers_)]->nfollowers()
           << ", 75% " << users_[(int) (0.75 * nusers_)]->nfollowers()
           << ", 90% " << users_[(int) (0.90 * nusers_)]->nfollowers()
           << ", 99% " << users_[(int) (0.99 * nusers_)]->nfollowers()
           << ", max " << users_[nusers_ - 1]->nfollowers() << "\n";

    std::sort(users_.begin(), users_.end(), TwitterGraphNode::Compare(TwitterGraphNode::comp_uid));
}

typedef pq::TwitterNewShim<pq::MultiClient, pq::TwitterNewPopulator> remote_shim_type;

tamed void run_twitter_new_remote(TwitterNewPopulator& tp, int client_port,
                                  const Hosts* hosts, const Hosts* dbhosts,
                                  const Partitioner* part) {
    tvars {
        MultiClient* mc = new MultiClient(hosts, dbhosts, part, client_port);
        remote_shim_type* shim = new remote_shim_type(*mc, tp);
        TwitterNewRunner<remote_shim_type>* tr = new TwitterNewRunner<remote_shim_type>(*shim, tp);
    }
    twait { mc->connect(make_event()); }
    twait { tr->initialize(make_event()); }
    twait { tr->populate(make_event()); }
    twait { tr->run(make_event()); }
    delete tr;
    delete shim;
    delete mc;
}

typedef pq::TwitterNewDBShim<pq::DBPool, pq::TwitterNewPopulator> compare_shim_type;

tamed void run_twitter_new_compare(TwitterNewPopulator& tp, int32_t client_port,
                                   uint32_t pool_min, uint32_t pool_max) {
    tvars {
        DBPool* client = new DBPool("127.0.0.1", client_port, pool_min, pool_max);
        compare_shim_type* shim = new compare_shim_type(*client, tp);
        TwitterNewRunner<compare_shim_type>* tr = new TwitterNewRunner<compare_shim_type>(*shim, tp);
    }

    client->connect(); 
    twait { tr->initialize(make_event()); }
    twait { tr->populate(make_event()); }
    twait { tr->run(make_event()); }
    delete tr;
    delete shim;
    delete client;
}

} // namespace pq
