#include "pqfacebook.hh"
#include <boost/accumulators/accumulators.hpp>
#include <boost/accumulators/statistics/stats.hpp>
#include <boost/accumulators/statistics/mean.hpp>
#include <boost/accumulators/statistics/variance.hpp>
#include <math.h>

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

}

