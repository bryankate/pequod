#include <boost/random/random_number_generator.hpp>
#include "pghackernews.hh"
#include "pgclient.hh"
#include "hnpopulator.hh"
#include "json.hh"

int main() {
    pq::PostgresClient pg;
    Json tp_param = Json().set("nusers", 5000);
    tp_param.set("narticles", 10);
    tp_param.set("vote_rate", 1);
    tp_param.set("comment_rate", 1);
    tp_param.set("nops", 10000);
    pq::HackernewsPopulator hp(tp_param);
    pq::PGHackernewsRunner hr(pg, hp);
    hr.populate();
    hr.run();
    return 0;
}
