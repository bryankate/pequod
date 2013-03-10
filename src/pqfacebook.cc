#include "pqfacebook.hh"
#include "error.hh"
#include "time.hh"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <math.h>
#include <sys/resource.h>

using namespace std;

namespace pq {

FacebookPopulator::FacebookPopulator(const Json& param)
    : nusers_(param["nusers"].as_i(5000)),
      npages_(param["npages"].as_i(5000)),
      push_(param["push"].as_b(false)),
      min_friends_(param["minfriends"].as_i(1)),
      max_friends_(param["maxfriends"].as_i(10)),
      min_page_likes_(param["minlikes"].as_i(0)),
      max_page_likes_(param["maxlikes"].as_i(10)),
      shape_(param["shape"].as_d(5)) {
}

void FacebookPopulator::populate_likes(boost::mt19937 gen){
    // generate some likes; these should be skewed in some way; exponential-like in shape
    boost::random::uniform_int_distribution<> generatePageLike(0, npages_-1);
    boost::random::uniform_int_distribution<> generatePageLikeCount(min_page_likes_, max_page_likes_);
    boost::random::exponential_distribution<> generatePageWithDistribution(shape_);
    double cur, largest;
    largest = 1/shape_;
    char like_k[16];
    uint32_t page_n;

    for (uint32_t user_n(0); user_n != nusers_; ++user_n ){
        int user_likes_count = generatePageLikeCount(gen);
        for (int j = 0; j < user_likes_count; ++j){
            while((cur = generatePageWithDistribution(gen)) > largest){
              largest = cur;
            }
            page_n = floor((cur/largest)*npages_);
            assert(sprintf(like_k, "l|%06u|%06u", user_n, page_n) == 15);
#ifdef DEBUG
            cout << "We made this like bro -> " << like_k << endl;
#endif
            localstore_[like_k] = page_n; 
        }
    }
#ifdef DEBUG
    boost::unordered_map<String, uint32_t>::const_iterator i;

    cout << "The store looks like this now -> " << endl;
    for(i = localstore_.begin(); i != localstore_.end(); ++i){
        cout << i->first << ":" << i->second << endl;
    }
#endif
}

void FacebookPopulator::generate_friends(generator_type gen){
    // generate some likes; these should be skewed like the twitter follows
    boost::random::uniform_int_distribution<> generateFriend(0, nusers_-1);
    boost::random::uniform_int_distribution<> generateFriendCount(min_friends_, max_friends_);
    char friends_k[16];
    for (uint32_t user_n(0); user_n != nusers_; ++user_n ){
        int user_friend_count = generateFriendCount(gen);
        for (int j = 0; j < user_friend_count; ++j){
            uint32_t friend_n;
            while ( (friend_n = generateFriend(gen)) == user_n )
                ; // don't friend yourself
            assert(sprintf(friends_k, "f|%06u|%06u", user_n, friend_n) == 15);
            localstore_[friends_k] = friend_n; // add the friendship
            // reuse the buffer to add the reciprocal friendship
            assert(sprintf(friends_k, "f|%06u|%06u", friend_n, user_n) == 15);
            localstore_[friends_k] = user_n; // if a friends b; b should friend a

        }
    }
#ifdef DEBUG
    boost::unordered_map<String, uint32_t>::const_iterator i;

    cout << "The store looks like this now -> " << endl;
    for(i = localstore_.begin(); i != localstore_.end(); ++i){
        cout << i->first << ":" << i->second << endl;
    }
#endif
}
void FacebookPopulator::report_counts(std::ostream& stream){
    // do the stats and output what we have
    using namespace boost::accumulators;

    uint32_t* num_friends = new uint32_t[nusers_];
    uint32_t* num_page_likes = new uint32_t[npages_];
    memset(num_friends, 0, nusers_ * sizeof(uint32_t));
    memset(num_page_likes, 0, npages_ * sizeof(uint32_t));
    for (auto& keyval : localstore_) {
        if (*(keyval.first.data()) == 'f')
            ++num_friends[keyval.second];
        else if (*(keyval.first.data()) == 'l')
            ++num_page_likes[keyval.second];
    }
    std::sort(num_friends, num_friends + nusers_);
    std::sort(num_page_likes, num_page_likes + npages_);

    accumulator_set<uint32_t, stats<tag::variance> > acc;
    accumulator_set<uint32_t, stats<tag::variance> > p_acc;
    for (uint32_t i = 0; i != nusers_; ++i)
        acc(num_friends[i]);
    for (uint32_t i = 0; i != npages_; ++i)
        p_acc(num_page_likes[i]);

    stream << "USERS HAVE # FRIENDS:\n"
           << "  zero: " << (std::upper_bound(num_friends, num_friends + nusers_, 0) - num_friends)
           << "  mean: " << mean(acc)
           << "  sdev: " << sqrt(variance(acc)) << "\n"
           << "  min " << num_friends[0]
           << ", 1% " << num_friends[(int) (0.01 * nusers_)]
           << ", 10% " << num_friends[(int) (0.10 * nusers_)]
           << ", 25% " << num_friends[(int) (0.25 * nusers_)]
           << ", 50% " << num_friends[(int) (0.50 * nusers_)]
           << ", 75% " << num_friends[(int) (0.75 * nusers_)]
           << ", 90% " << num_friends[(int) (0.90 * nusers_)]
           << ", 99% " << num_friends[(int) (0.99 * nusers_)]
           << ", max " << num_friends[nusers_ - 1] << "\n";

    stream << "PAGES HAVE # LIKES:\n"
           << "  zero: " << (std::upper_bound(num_page_likes, num_page_likes + npages_, 0) - num_page_likes)
           << "  mean: " << mean(p_acc)
           << "  sdev: " << sqrt(variance(p_acc)) << "\n"
           << "  min " << num_page_likes[0]
           << ", 1% " << num_page_likes[(int) (0.01 * npages_)]
           << ", 10% " << num_page_likes[(int) (0.10 * npages_)]
           << ", 25% " << num_page_likes[(int) (0.25 * npages_)]
           << ", 50% " << num_page_likes[(int) (0.50 * npages_)]
           << ", 75% " << num_page_likes[(int) (0.75 * npages_)]
           << ", 90% " << num_page_likes[(int) (0.90 * npages_)]
           << ", 99% " << num_page_likes[(int) (0.99 * npages_)]
           << ", max " << num_page_likes[npages_ - 1] << "\n";

    delete[] num_friends;
    delete[] num_page_likes;
}

uint32_t* FacebookPopulator::get_visit_list(){ return (uint32_t*)1; }


void facebook_like(Server& server, FacebookPopulator& fp,
                  uint32_t u, uint32_t p, Str value) {
    char buf[128];
    sprintf(buf, "l|%06d|%06d", u, p);
    server.insert(Str(buf, 15), value);
    if (fp.push())
          std::cerr << "NOT IMPLEMENTED" << std::endl;
//        for (auto it = tp.begin_followers(u); it != tp.end_followers(u); ++it) {
//            sprintf(buf, "t|%05u|%010u|%05u", *it, time, u);
//            server.insert(Str(buf, 24), value);
//        }
}

void facebook_populate(Server& server, FacebookPopulator& fp) {
    boost::mt19937 gen;
    gen.seed(0);

    fp.populate_likes(gen);
    fp.generate_friends(gen);
    fp.nusers();
    for (auto& x : fp.get_base_data()) {
        server.insert(x.first, Str("1", 1));
    }
    fp.report_counts(std::cout);

    if (!fp.push()) {
        FileErrorHandler errh(stderr);
        pq::Join* j = new pq::Join;
        j->assign_parse("c|<user_id:6>|<page_id:6>|<fuser_id:6> "
                        "l|<fuser_id>|<page_id> "
                        "f|<user_id>|<fuser_id>", &errh);
        server.add_join("c|", "c}", j);
    }
}

void facebook_run(Server& server, FacebookPopulator& fp) {
    boost::mt19937 gen;
    gen.seed(13918);
    boost::random_number_generator<boost::mt19937> rng(gen);
    struct rusage ru[2];

    uint32_t time = 10000000;
    uint32_t nusers = fp.nusers();
    uint32_t npages = fp.npages();
    uint32_t post_end_time = time + nusers * 5;
    uint32_t end_time = post_end_time + 10000;
    uint32_t* load_times = new uint32_t[nusers];
    for (uint32_t i = 0; i != nusers; ++i)
        load_times[i] = 0;
    char buf1[128], buf2[128];
    uint32_t nlike = 0;
    size_t nvisit = 0;
    getrusage(RUSAGE_SELF, &ru[0]);

    while (time != end_time) {
        uint32_t u = rng(nusers);
        uint32_t p = rng(npages);
        uint32_t l = rng(100);
        // u should always visit p
        sprintf(buf1, "c|%06d|%06d", u, p);
        sprintf(buf2, "c|%06d|%06d}", u, p);
        server.validate(Str(buf1, 15), Str(buf2, 16));
        nvisit += server.count(Str(buf1, 15), Str(buf2, 16));
        load_times[u] = time;
        // 3% u should also like the page
        if (l < 3) {
           facebook_like(server, fp, u, p, "1");
           ++nlike;
        }
        ++time;
    }

    getrusage(RUSAGE_SELF, &ru[1]);
    Json stats = Json().set("nlikes", nlike)
        .set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    stats.merge(server.stats());
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
    delete[] load_times;
}

}
