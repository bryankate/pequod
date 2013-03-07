#include <boost/random/random_number_generator.hpp>
#include "pghackernews.hh"
#include "pgclient.hh"
#include "hnpopulator.hh"
#include "json.hh"
#include "clp.h"

static Clp_Option options[] = {
    { "push", 'p', 1000, 0, Clp_Negate },
    { "nusers", 'n', 1001, Clp_ValInt, 0 },
    { "log", 0, 1002, 0, Clp_Negate },
    { "narticles", 'a', 1003, Clp_ValInt, 0 },
    { "nops", 'o', 1004, Clp_ValInt, 0 },
    { "materialize", 'm', 1005, 0, Clp_Negate },
    { "vote_rate", 'v', 1006, Clp_ValInt, 0 },
    { "comment_rate", 'r', 1007, Clp_ValInt, 0 }
};


int main(int argc, char** argv) {
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    pq::PostgresClient pg;
    Json tp_param = Json().set("nusers", 5);
    while (Clp_Next(clp) != Clp_Done) {
	if (clp->option->long_name == String("push"))
	    tp_param.set("push", !clp->negated);
	else if (clp->option->long_name == String("nusers"))
	    tp_param.set("nusers", clp->val.i);
	else if (clp->option->long_name == String("narticles"))
	    tp_param.set("narticles", clp->val.i);
	else if (clp->option->long_name == String("vote_rate"))
	    tp_param.set("vote_rate", clp->val.i);
	else if (clp->option->long_name == String("comment_rate"))
	    tp_param.set("comment_rate", clp->val.i);
	else if (clp->option->long_name == String("nops"))
	    tp_param.set("nops", clp->val.i);
	else if (clp->option->long_name == String("materialize"))
	    tp_param.set("materialize", !clp->negated);
        else if (clp->option->long_name == String("log"))
            tp_param.set("log", !clp->negated);
    }
    pq::HackernewsPopulator hp(tp_param);
    pq::PGHackernewsRunner hr(pg, hp);
    hr.populate();
    std::cout << "Finished populate.\n";
    hr.run();
    return 0;
}
