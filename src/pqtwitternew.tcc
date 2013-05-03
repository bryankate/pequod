// -*- mode: c++ -*-
#include "pqtwitternew.hh"
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

TwitterUser::TwitterUser(uint32_t uid)
    : nbackpost_(0), npost_(0), nsubscribe_(0), nlogout_(0),
      nlogin_(0), ncheck_(0), nread_(0), load_time_(0), loggedin_(false),
      uid_(uid), celeb_(false) {
}

TwitterUser::Compare::Compare(CompareField field) : field_(field) {
}

bool TwitterUser::Compare::operator() (const TwitterUser&a, const TwitterUser& b) const {
    switch(field_) {
        case comp_uid:
            return a.uid_ < b.uid_;
        case comp_nfollowers:
            return a.followers_.size() < b.followers_.size();
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
            return a < b.followers_.size();
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
      popduration_(param["popduration"].as_i(duration_ / 10)),
      postlimit_(param["postlimit"].as_i(0)),
      push_(param["push"].as_b(false)),
      pull_(param["pull"].as_b(false)),
      fetch_(param["fetch"].as_b(false)),
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

void TwitterNewPopulator::make_subscriptions(generator_type& gen) {
    if (graph_file_)
        import_subscriptions(gen);
    else
        synthetic_subscriptions(gen);
}

void TwitterNewPopulator::synthetic_subscriptions(generator_type& gen) {
    vector<pair<uint32_t, uint32_t> > subs;
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
            subvec[other / 32] |= 1U << (other % 32);
        }
    }

    delete[] sub_prob;
    delete[] subvec;

    make_users(subs, gen);
}

void TwitterNewPopulator::import_subscriptions(generator_type& gen) {
    std::ifstream graph;
    graph.open(graph_file_.c_str());
    assert(graph.is_open() && "Could not open twitter social graph.");

    graph >> nusers_;

    uint32_t user;
    uint32_t follower;
    vector<pair<uint32_t, uint32_t> > subs;

    while(graph.good()) {
        graph >> user >> follower;
        subs.push_back(std::make_pair(follower, user));
    }

    make_users(subs, gen);
}

void TwitterNewPopulator::make_users(vector<pair<uint32_t, uint32_t>>& subs,
                                     generator_type& gen) {

    for (uint32_t u = 0; u < nusers_; ++u)
        users_.push_back(TwitterUser(u));

    for (auto& sub : subs)
        users_[sub.second].add_follower(sub.first);

    vector<double> wpost(nusers_, 0);
    for (uint32_t u = 0; u < nusers_; ++u) {
        uint32_t nfollowers = users_[u].nfollowers();

        if (celebthresh_ && nfollowers > celebthresh_)
            users_[u].mark_celeb();

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


void TwitterNewPopulator::print_subscription_statistics(ostream& stream) {
    using namespace boost::accumulators;

    if (!verbose_)
        return;

    accumulator_set<uint32_t, stats<tag::variance> > acc;
    for (uint32_t i = 0; i != nusers_; ++i)
        acc(users_[i].nfollowers());

    TwitterUser::Compare cmp(TwitterUser::comp_nfollowers);
    std::sort(users_.begin(), users_.end(), cmp);

    stream << nusers_ << " USERS HAVE SUBSCRIBERS:\n"
           << "  zero: " << (std::upper_bound(users_.begin(), users_.end(), 0, cmp) - users_.begin())
           << "  mean: " << mean(acc)
           << "  sdev: " << sqrt(variance(acc)) << "\n"
           << "  min " << users_[0].nfollowers()
           << ", 1% " << users_[(int) (0.01 * nusers_)].nfollowers()
           << ", 10% " << users_[(int) (0.10 * nusers_)].nfollowers()
           << ", 25% " << users_[(int) (0.25 * nusers_)].nfollowers()
           << ", 50% " << users_[(int) (0.50 * nusers_)].nfollowers()
           << ", 75% " << users_[(int) (0.75 * nusers_)].nfollowers()
           << ", 90% " << users_[(int) (0.90 * nusers_)].nfollowers()
           << ", 99% " << users_[(int) (0.99 * nusers_)].nfollowers()
           << ", max " << users_[nusers_ - 1].nfollowers() << "\n";

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
        ppu << users_[i].nfollowers() << " " << (users_[i].nbackpost_ +  users_[i].npost_) << endl;

    std::sort(users_.begin(), users_.end(), TwitterUser::Compare(TwitterUser::comp_check));
    for (uint32_t i = 0; i <= 99; ++i)
        cpu << i << " " << users_[(uint32_t)((double)i / 100 * nusers_)].ncheck_ << endl;
    cpu << 100 << " " << users_[nusers_ - 1].ncheck_ << endl;

    ppu.close();
    cpu.close();

    // todo: should sort back by uid? yes if this is not called at the end
    // of the application execution.
}

tamed void run_twitter_new_remote(TwitterNewPopulator& tp, int client_port,
                                  const Hosts* hosts, const Partitioner* part) {
    tvars {
        MultiClient* mc = new MultiClient(hosts, part, client_port);
        TwitterNewShim<MultiClient>* shim = new TwitterNewShim<MultiClient>(*mc);
        TwitterNewRunner<TwitterNewShim<MultiClient>>* tr = new TwitterNewRunner<TwitterNewShim<MultiClient> >(*shim, tp);
    }
    twait { mc->connect(make_event()); }
    twait { tr->populate(make_event()); }
    twait { tr->run(make_event()); }
    delete tr;
    delete shim;
    delete mc;
}

} // namespace pq
