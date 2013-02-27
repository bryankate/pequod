#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "pqtwitter.hh"
#include "pqfacebook.hh"
#include "clp.h"
#include <boost/random/random_number_generator.hpp>
#include <sys/resource.h>
#include "time.hh"

namespace pq {

// XXX check circular expansion

inline ServerRange::ServerRange(Str first, Str last, range_type type, Join* join)
    : ibegin_len_(first.length()), iend_len_(last.length()),
      subtree_iend_(keys_ + ibegin_len_, iend_len_), type_(type), join_(join) {
    memcpy(keys_, first.data(), first.length());
    memcpy(keys_ + ibegin_len_, last.data(), last.length());
}

ServerRange* ServerRange::make(Str first, Str last, range_type type, Join* join) {
    char* buf = new char[sizeof(ServerRange) + first.length() + last.length()];
    return new(buf) ServerRange(first, last, type, join);
}

void ServerRange::add_sink(const Match& m) {
    assert(join_ && type_ == copy);
    resultkeys_.push_back(String::make_uninitialized((*join_)[0].key_length()));
    (*join_)[0].expand(resultkeys_.back().mutable_udata(), m);
}

void ServerRange::notify(const Datum* d, int notifier, Server& server) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (type_ == copy && join_->back().match(d->key())) {
	for (auto& s : resultkeys_) {
	    join_->expand(s.mutable_udata(), d->key());
	    if (notifier >= 0)
		server.insert(s, d->value_, join_->recursive());
	    else
		server.erase(s, join_->recursive());
	}
    }
}

void ServerRange::validate(Str first, Str last, Server& server) {
    if (type_ == joinsink) {
        Match mf, ml;
        (*join_)[0].match(first, mf);
        (*join_)[0].match(last, ml);
        validate(mf, ml, 1, server);
        server.add_validjoin(first, last, join_);
    }
}

void ServerRange::validate(Match& mf, Match& ml, int joinpos, Server& server) {
    uint8_t kf[128], kl[128];
    int kflen = (*join_)[joinpos].first(kf, mf);
    int kllen = (*join_)[joinpos].last(kl, ml);

    // need to validate the source ranges in case they have not been
    // expanded yet.
    // XXX PERFORMANCE this is not always necessary
    server.validate(Str(kf, kflen), Str(kl, kllen));

    if (joinpos + 1 == join_->size())
        server.add_copy(Str(kf, kflen), Str(kl, kllen), join_, mf);

    auto it = server.lower_bound(Str(kf, kflen));
    auto ilast = server.lower_bound(Str(kl, kllen));

    Match mk(mf);
    mk &= ml;
    Match::state mfstate(mf.save()), mlstate(ml.save()), mkstate(mk.save());

    for (; it != ilast; ++it) {
	if (it->key().length() != (*join_)[joinpos].key_length())
            continue;
        // XXX PERFORMANCE can prob figure out ahead of time whether this
        // match is simple/necessary
        if ((*join_)[joinpos].match(it->key(), mk)) {
            if (joinpos + 1 == join_->size()) {
                kflen = (*join_)[0].first(kf, mk);
                // XXX PERFORMANCE can prob figure out ahead of time whether
                // this insert is simple (no notifies)
                server.insert(Str(kf, kflen), it->value_, join_->recursive());
            } else {
                (*join_)[joinpos].match(it->key(), mf);
                (*join_)[joinpos].match(it->key(), ml);
                validate(mf, ml, joinpos + 1, server);
                mf.restore(mfstate);
                ml.restore(mlstate);
            }
        }
        mk.restore(mkstate);
    }
}

std::ostream& operator<<(std::ostream& stream, const ServerRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << ")";
    if (r.type_ == ServerRange::copy) {
	stream << ": copy ->";
	for (auto s : r.resultkeys_)
	    stream << " " << s;
    }
    else if (r.type_ == ServerRange::joinsink)
        stream << ": joinsink @" << (void*) r.join_;
    else if (r.type_ == ServerRange::joinsource)
            stream << ": joinsource @" << (void*) r.join_;
    else if (r.type_ == ServerRange::validjoin)
        stream << ": validjoin @" << (void*) r.join_;
    else
	stream << ": ??";
    return stream << "}";
}

void ServerRangeSet::push_back(ServerRange* r) {
    if (!(r->type() & types_))
	return;

    int i = nr_;
    while (sw_ > (1 << i))
	++i;
    assert(i == 0 || !(r->ibegin() < r_[i - 1]->ibegin()));
    assert(i < rangecap);

    r_[i] = r;
    if (last_ && first_ < r_[i]->ibegin())
	sw_ |= 1 << i;
    else if (last_ && r_[i]->iend() < last_) {
	++nr_;
	sw_ |= 1 << i;
    } else
	++nr_;
}

void ServerRangeSet::hard_visit(const Datum* datum) {
    while (sw_ > (1 << nr_) && !(datum->key() < r_[nr_]->ibegin())) {
	if (!(r_[nr_]->iend() < last_))
	    sw_ &= ~(1 << nr_);
	++nr_;
    }
    if (sw_ & ((1 << nr_) - 1))
	for (int i = 0; i != nr_; ++i)
	    if ((sw_ & (1 << i)) && !(datum->key() < r_[nr_]->iend())) {
		r_[i] = 0;
		sw_ &= ~(1 << i);
	    }
}

void ServerRangeSet::validate_join(ServerRange* jr, int ts) {
    Str last_valid = first_;
    for (int i = 0; i != ts; ++i)
        if (r_[i]
            && r_[i]->type() == ServerRange::validjoin
            && r_[i]->join() == jr->join()) {
            if (sw_ == 0)
                return;
            if (last_valid < r_[i]->ibegin())
                jr->validate(last_valid, r_[i]->ibegin(), *server_);
            last_valid = r_[i]->iend();
        }
    if (last_valid < last_)
        jr->validate(last_valid, last_, *server_);
}

void ServerRangeSet::validate() {
    int ts = total_size();
    for (int i = 0; i != ts; ++i)
        if (r_[i]->type() == ServerRange::joinsink)
            validate_join(r_[i], ts);
}

std::ostream& operator<<(std::ostream& stream, const ServerRangeSet& srs) {
    stream << "[";
    const char* sep = "";
    for (int i = 0; i != srs.nr_; ++i)
	if (srs.r_[i]) {
	    stream << sep << *srs.r_[i];
	    sep = ", ";
	}
    return stream << "]";
}

void Server::add_copy(Str first, Str last, Join* join, const Match& m) {
    for (auto it = source_ranges_.begin_contains(make_interval(first, last));
	 it != source_ranges_.end(); ++it)
	if (it->type() == ServerRange::copy && it->join() == join) {
	    // XXX may copy too much. This will not actually cause visible
	    // bugs I think?, but will grow the store
	    it->add_sink(m);
	    return;
	}
    ServerRange* r = ServerRange::make(first, last, ServerRange::copy, join);
    r->add_sink(m);
    source_ranges_.insert(r);
}

void Server::add_join(Str first, Str last, Join* join) {
    // track full ranges used for join and copy
    // XXX could do a more precise job if that was ever warranted
    std::vector<ServerRange*> ranges;
    ranges.push_back(ServerRange::make(first, last,
				       ServerRange::joinsink, join));
    for (int i = 1; i != join->size(); ++i)
	ranges.push_back(ServerRange::make((*join)[i].first(Match()),
					   (*join)[i].last(Match()),
					   ServerRange::joinsource, join));
    for (auto r : ranges)
	join_ranges_.insert(r);

    // check for recursion
    for (auto it = join_ranges_.begin_overlaps(ranges[0]->interval());
	 it != join_ranges_.end(); ++it)
	if (it->type() == ServerRange::joinsource)
	    join->set_recursive();
    for (int i = 1; i != join->size(); ++i)
	for (auto it = join_ranges_.begin_overlaps(ranges[i]->interval());
	     it != join_ranges_.end(); ++it)
	    if (it->type() == ServerRange::joinsink)
		it->join()->set_recursive();

    sink_ranges_.insert(ServerRange::make(first, last, ServerRange::joinsink,
					  join));
}

void Server::insert(const String& key, const String& value, bool notify) {
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
	d->value_ = value;
    }

    if (notify)
	for (auto it = source_ranges_.begin_contains(Str(key));
	     it != source_ranges_.end(); ++it)
	    if (it->type() == ServerRange::copy)
		it->notify(d, p.second ? ServerRange::notify_insert : ServerRange::notify_update, *this);
}

void Server::erase(const String& key, bool notify) {
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end()) {
	Datum* d = it.operator->();
	store_.erase(it);

	if (notify)
	    for (auto it = source_ranges_.begin_contains(Str(key));
		 it != source_ranges_.end(); ++it)
		if (it->type() == ServerRange::copy)
		    it->notify(d, ServerRange::notify_erase, *this);

	delete d;
    }
}

Json Server::stats() const {
    return Json().set("store_size", store_.size())
	.set("source_ranges_size", source_ranges_.size())
	.set("sink_ranges_size", sink_ranges_.size());
}

void Server::print(std::ostream& stream) {
    stream << "sources:" << std::endl << source_ranges_;
    stream << "sinks:" << std::endl << sink_ranges_;
    stream << "joins:" << std::endl << join_ranges_;
}

} // namespace


void simple() {
    std::cerr << "SIMPLE" << std::endl;
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
	{"f|00001|00002", "1"},
	{"f|00001|10000", "1"},
	{"p|00002|0000000000", "Should not appear"},
	{"p|00002|0000000001", "Hello,"},
	{"p|00002|0000000022", "Which is awesome"},
	{"p|10000|0000000010", "My name is"},
	{"p|10000|0000000018", "Jennifer Jones"}
    };
    server.replace_range("f|00001", "p}", values, values + sizeof(values) / sizeof(values[0]));

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    pq::Join j;
    j.assign_parse("t|<user_id:5>|<time:10>|<poster_id:5> "
		   "f|<user_id>|<poster_id> "
		   "p|<poster_id>|<time>");
    j.ref();
    server.add_join("t|", "t}", &j);

    server.validate("t|00001|0000000001", "t|00001}");

    std::cerr << "After processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    server.insert("p|10000|0000000022", "This should appear in t|00001", true);
    server.insert("p|00002|0000000023", "As should this", true);

    std::cerr << "After processing add_copy:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    server.print(std::cerr);
    std::cerr << std::endl;
}

void count() {
    std::cerr << "COUNT" << std::endl;
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"b|00002|0000000010", "b3"},
        {"b|00002|0000000020", "b4"},
        {"b|00003|0000000002", "b5"},
        {"d|00003|00001", "1"}
    };
    server.replace_range("a|00001", "d}", values, values + sizeof(values) / sizeof(values[0]));

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> "
                    "a|<a_id>|<b_id> "
                    "b|<b_id>|<time>");
    j1.ref();
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("e|<d_id:5>|<time:10>|<b_id:5> "
                    "d|<d_id>|<a_id:5> "
                    "c|<a_id>|<time>|<b_id>");
    j2.ref();
    server.add_join("e|", "e}", &j2);

    std::cerr << std::endl << "e| count: " << server.count("e|", "e}") << std::endl << std::endl;

    std::cerr << "After recursive count:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    std::cerr << "b| count: " << server.count("b|", "b}") << std::endl;
    std::cerr << "b|00002 count: " << server.count("b|00002|", "b|00002}") << std::endl;
    std::cerr << "b|00002 subcount: " << server.count("b|00002|0000000002", "b|00002|0000000015") << std::endl;
    std::cerr << "c| count: " << server.count("c|", "c}") << std::endl << std::endl;
}

void recursive() {
    std::cerr << "RECURSIVE" << std::endl;
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"d|00003|00001", "1"}
    };
    server.replace_range("a|00001", "d}", values, values + sizeof(values) / sizeof(values[0]));

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> "
                    "a|<a_id>|<b_id> "
                    "b|<b_id>|<time>");
    j1.ref();
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("e|<d_id:5>|<time:10>|<b_id:5> "
                    "d|<d_id>|<a_id:5> "
                    "c|<a_id>|<time>|<b_id>");
    j2.ref();
    server.add_join("e|", "e}", &j2);

    server.validate("e|00003|0000000001", "e|00003}");

    std::cerr << "After processing recursive join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    server.insert("b|00002|0000001000", "This should appear in c|00001 and e|00003", true);
    server.insert("b|00002|0000002000", "As should this", true);

    std::cerr << "After processing inserts:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    server.print(std::cerr);
    std::cerr << std::endl;
}

void srs() {
    pq::Server server;
    pq::ServerRangeSet srs(&server, "a001", "a}",
                       pq::ServerRange::joinsink | pq::ServerRange::validjoin);

    pq::Join j;
    pq::ServerRange *r0 = pq::ServerRange::make("a", "a}", pq::ServerRange::joinsink, &j);
    pq::ServerRange *r1 = pq::ServerRange::make("a003", "a005", pq::ServerRange::validjoin, &j);
    pq::ServerRange *r2 = pq::ServerRange::make("a007", "a010", pq::ServerRange::validjoin, &j);

    srs.push_back(r0);
    srs.push_back(r1);
    srs.push_back(r2);

    // i expect nr_ == 3 and sw_ == 7?
    // definitely not total_size of 1...
    std::cerr << srs.total_size() << std::endl;
    mandatory_assert(srs.total_size() == 3);
}

void facebook_like(pq::Server& server, pq::FacebookPopulator& fp,
                  uint32_t u, uint32_t p, Str value) {
    char buf[128];
    sprintf(buf, "l|%06d|%06d", u, p);
    server.insert(Str(buf, 15), value, true);
    if (fp.push())
          std::cerr << "NOT IMPLEMENTED" << std::endl;
//        for (auto it = tp.begin_followers(u); it != tp.end_followers(u); ++it) {
//            sprintf(buf, "t|%05u|%010u|%05u", *it, time, u);
//            server.insert(Str(buf, 24), value, false);
//        }
}

void facebook_populate(pq::Server& server, pq::FacebookPopulator& fp) {
    boost::mt19937 gen;
    gen.seed(0);

    fp.populate_likes(gen);
    fp.generate_friends(gen);
    fp.nusers();
    for (auto& x : fp.get_base_data()) {
        server.insert(x.first, Str("1", 1), true);
    }
    fp.report_counts(std::cout);

    if (!fp.push()) {
        pq::Join* j = new pq::Join;
        j->assign_parse("c|<user_id:6>|<page_id:6>|<fuser_id:6> "
                        "l|<fuser_id>|<page_id> "
                        "f|<user_id>|<fuser_id>");
        server.add_join("c|", "c}", j);
    }
}

void facebook_run(pq::Server& server, pq::FacebookPopulator& fp) {
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


static Clp_Option options[] = {
    { "push", 'p', 1000, 0, Clp_Negate },
    { "nusers", 'n', 1001, Clp_ValInt, 0 },
    { "facebook", 'f', 1002, 0, Clp_Negate },
    { "shape", 0, 1003, Clp_ValDouble, 0 },
    { "listen", 'l', 1004, Clp_ValInt, Clp_Optional },
    { "log", 0, 1005, 0, Clp_Negate },
    { "tests", 0, 1006, 0, 0 }
};

enum { mode_unknown, mode_twitter, mode_facebook, mode_listen, mode_tests };

int main(int argc, char** argv) {
    int mode = mode_unknown, listen_port = 8000;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    Json tp_param = Json().set("nusers", 5000);
    while (Clp_Next(clp) != Clp_Done) {
	if (clp->option->long_name == String("push"))
	    tp_param.set("push", !clp->negated);
	else if (clp->option->long_name == String("nusers"))
	    tp_param.set("nusers", clp->val.i);
	else if (clp->option->long_name == String("shape"))
	    tp_param.set("shape", clp->val.d);
	else if (clp->option->long_name == String("facebook"))
            mode = mode_facebook;
        else if (clp->option->long_name == String("twitter"))
            mode = mode_twitter;
        else if (clp->option->long_name == String("tests"))
            mode = mode_tests;
        else if (clp->option->long_name == String("listen")) {
            mode = mode_listen;
            if (clp->have_val)
                listen_port = clp->val.i;
        } else if (clp->option->long_name == String("log"))
            tp_param.set("log", !clp->negated);
        else
	    exit(1);
    }

    pq::Server server;
    if (mode == mode_tests) {
        simple();
        recursive();
        count();
    } else if (mode == mode_listen) {
        extern void server_loop(int port, pq::Server& server);
        server_loop(listen_port, server);
    } else if (mode == mode_twitter || mode == mode_unknown) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 8);
        pq::TwitterPopulator tp(tp_param);
        pq::TwitterRunner tr(server, tp);
        tr.populate();
        tr.run();
    } else {
        //server.print(std::cout);
        if (!tp_param.count("shape"))
            tp_param.set("shape", 5);
        pq::FacebookPopulator fp(tp_param);
        facebook_populate(server, fp);
        facebook_run(server, fp);
    }
    //server.print(std::cout);
}
