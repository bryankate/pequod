#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "pqtwitter.hh"
#include "pqfacebook.hh"
#include "clp.h"
#include <boost/random/random_number_generator.hpp>
#include <sys/resource.h>
#include <unistd.h>
#include "time.hh"
#include "check.hh"

namespace pq {

// XXX check circular expansion

inline ServerRange::ServerRange(Str first, Str last, range_type type, Join* join)
    : ibegin_len_(first.length()), iend_len_(last.length()),
      subtree_iend_(keys_ + ibegin_len_, iend_len_),
      type_(type), join_(join), expires_at_(0) {

    memcpy(keys_, first.data(), first.length());
    memcpy(keys_ + ibegin_len_, last.data(), last.length());

    if (join_->staleness())
        expires_at_ = tstamp() + tous(join_->staleness());
}

ServerRange* ServerRange::make(Str first, Str last, range_type type, Join* join) {
    char* buf = new char[sizeof(ServerRange) + first.length() + last.length()];
    return new(buf) ServerRange(first, last, type, join);
}

void ServerRange::add_sink(const Match& m) {
    assert(join_ && type_ == copy);
    resultkeys_.push_back(String::make_uninitialized(join_->sink().key_length()));
    join_->sink().expand(resultkeys_.back().mutable_udata(), m);
}

void ServerRange::notify(const Datum* d, int notifier, Server& server) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (type_ == copy && join_->back_source().match(d->key())) {
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
        join_->sink().match(first, mf);
        join_->sink().match(last, ml);
        validate(mf, ml, 0, server);

        if (join_->maintained() || join_->staleness())
            server.add_validjoin(first, last, join_);
    }
}

void ServerRange::validate(Match& mf, Match& ml, int joinpos, Server& server) {
    uint8_t kf[128], kl[128];
    int kflen = join_->source(joinpos).expand_first(kf, mf);
    int kllen = join_->source(joinpos).expand_last(kl, ml);

    // need to validate the source ranges in case they have not been
    // expanded yet.
    // XXX PERFORMANCE this is not always necessary
    server.validate(Str(kf, kflen), Str(kl, kllen));

    if (join_->maintained() && joinpos + 1 == join_->nsource())
        server.add_copy(Str(kf, kflen), Str(kl, kllen), join_, mf);

    auto it = server.lower_bound(Str(kf, kflen));
    auto ilast = server.lower_bound(Str(kl, kllen));

    Match mk(mf);
    mk &= ml;
    Match::state mfstate(mf.save()), mlstate(ml.save()), mkstate(mk.save());

    for (; it != ilast; ++it) {
	if (it->key().length() != join_->source(joinpos).key_length())
            continue;
        // XXX PERFORMANCE can prob figure out ahead of time whether this
        // match is simple/necessary
        if (join_->source(joinpos).match(it->key(), mk)) {
            if (joinpos + 1 == join_->nsource()) {
                kflen = join_->sink().expand_first(kf, mk);
                // XXX PERFORMANCE can prob figure out ahead of time whether
                // this insert is simple (no notifies)
                server.insert(Str(kf, kflen), it->value_, join_->recursive());
            } else {
                join_->source(joinpos).match(it->key(), mf);
                join_->source(joinpos).match(it->key(), ml);
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
    else if (r.type_ == ServerRange::validjoin) {
        stream << ": validjoin @" << (void*) r.join_ << ", expires: ";
        if (r.expires_at_) {
            uint64_t now = tstamp();
            if (r.expired_at(now))
                stream << "EXPIRED";
            else
                stream << "in " << fromus(r.expires_at_ - now) << " seconds";
        }
        else
            stream << "NEVER";
    }
    else
	stream << ": ??";
    return stream << "}";
}

void ServerRangeSet::push_back(ServerRange* r) {
    if (!(r->type() & types_))
	return;

    int i = nr_;
    while (sw_ >= (1 << i))
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
    while (sw_ >= (1 << nr_) && !(datum->key() < r_[nr_]->ibegin())) {
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
    uint64_t now = tstamp();
    Str last_valid = first_;
    for (int i = 0; i != ts; ++i)
        if (r_[i]
            && r_[i]->type() == ServerRange::validjoin
            && r_[i]->join() == jr->join()) {
            if (r_[i]->expired_at(now)) {
                // TODO: remove the expired validjoin from the server
                // (and possibly this set if it will be used after this validation)
                continue;
            }
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

Table::Table(Str name)
    : namelen_(name.length()) {
    assert(namelen_ <= (int) sizeof(name_));
    memcpy(name_, name.data(), namelen_);
}

const Table Table::empty_table{Str()};

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
    for (int i = 0; i != join->nsource(); ++i)
	ranges.push_back(ServerRange::make
                         (join->source(i).expand_first(Match()),
                          join->source(i).expand_last(Match()),
                          ServerRange::joinsource, join));
    for (auto r : ranges)
	join_ranges_.insert(r);

    // check for recursion
    for (auto it = join_ranges_.begin_overlaps(ranges[0]->interval());
	 it != join_ranges_.end(); ++it)
	if (it->type() == ServerRange::joinsource)
	    join->set_recursive();
    for (int i = 1; i != (int) ranges.size(); ++i)
	for (auto it = join_ranges_.begin_overlaps(ranges[i]->interval());
	     it != join_ranges_.end(); ++it)
	    if (it->type() == ServerRange::joinsink)
		it->join()->set_recursive();

    sink_ranges_.insert(ServerRange::make(first, last, ServerRange::joinsink,
					  join));
}

void Server::insert(const String& key, const String& value, bool notify) {
    Str tname = table_name(key);
    if (!tname)
        return;
    Table& t = add_table(tname);

    store_type::insert_commit_data cd;
    auto p = t.store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
	t.store_.insert_commit(*d, cd);
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
    auto tit = tables_.find(table_name(Str(key)));
    if (!tit)
        return;

    auto it = tit->store_.find(key, DatumCompare());
    if (it != tit->store_.end()) {
	Datum* d = it.operator->();
	tit->store_.erase(it);

	if (notify)
	    for (auto it = source_ranges_.begin_contains(Str(key));
		 it != source_ranges_.end(); ++it)
		if (it->type() == ServerRange::copy)
		    it->notify(d, ServerRange::notify_erase, *this);

	delete d;
    }
}

Json Server::stats() const {
    size_t store_size = 0;
    for (auto& t : tables_)
        store_size += t.store_.size();
    return Json().set("store_size", store_size)
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
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second, true);

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

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
    std::cerr << std::endl;

    server.insert("p|10000|0000000022", "This should appear in t|00001", true);
    server.insert("p|00002|0000000023", "As should this", true);

    std::cerr << "After processing add_copy:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

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
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second, true);

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

    std::cerr << std::endl << "e| count: " << server.validate_count("e|", "e}") << std::endl << std::endl;

    std::cerr << "After recursive count:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    std::cerr << "b| count: " << server.validate_count("b|", "b}") << std::endl;
    std::cerr << "b|00002 count: " << server.validate_count("b|00002|", "b|00002}") << std::endl;
    std::cerr << "b|00002 subcount: " << server.validate_count("b|00002|0000000002", "b|00002|0000000015") << std::endl;
    std::cerr << "c| count: " << server.validate_count("c|", "c}") << std::endl << std::endl;
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
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second, true);

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

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
    std::cerr << std::endl;

    server.insert("b|00002|0000001000", "This should appear in c|00001 and e|00003", true);
    server.insert("b|00002|0000002000", "As should this", true);

    std::cerr << "After processing inserts:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;
}

void annotation() {
    std::cerr << "ANNOTATION" << std::endl;
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"d|00003|00004", "1"},
        {"e|00004|0000000001", "e1"},
        {"e|00004|0000000002", "e2"},
        {"e|00004|0000000010", "e3"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second, true);

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> "
                    "a|<a_id>|<b_id> "
                    "b|<b_id>|<time>");
    j1.ref();
    // will validate join each time and not install autopush triggers
    j1.set_maintained(false);
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("f|<d_id:5>|<time:10>|<e_id:5> "
                    "d|<d_id>|<e_id> "
                    "e|<e_id>|<time>");
    j2.ref();
    // will not re-validate join within T seconds of last validation
    // for the requested range. the store will hold stale results
    j2.set_staleness(0.5);
    server.add_join("f|", "f}", &j2);

    server.validate("c|00001|0000000001", "c|00001}");

    // should NOT have a validrange for c|00001 or a copy for b|00002
    std::cerr << "After validating [c|00001|0000000001, c|00001})" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    // should NOT trigger the insertion of a new c|00001 key
    server.insert("b|00002|0000000005", "b3", true);

    std::cerr << "After inserting new b|00002 key" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    server.validate("f|00003|0000000001", "f|00003|0000000005");

    // SHOULD have a validrange for f|00003 (that expires) but NO copy of e|00004
    std::cerr << "After validating [f|00003|0000000001, f|00003|0000000005)" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    server.insert("e|00004|0000000003", "e4", true);

    // should NOT trigger the insertion of a new f|00003 key
    std::cerr << "After inserting new e|00004 key" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    // should NOT re-validate the lower part of the range.
    server.validate("f|00003|0000000001", "f|00003|0000000010");

    // SHOULD have 2 validranges for f|00003 (that expire) but NO copies of e|00004
    std::cerr << "After validating [f|00003|0000000001, f|00003|0000000010)" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    sleep(1);

    // SHOULD re-validate the whole range
    server.validate("f|00003|0000000001", "f|00003|0000000010");

    // SHOULD have a validrange for f|00003 (that expires) but NO copy of e|00004
    // MIGHT have 2 expired validranges for f|00003 we implement cleanup of expired ranges
    std::cerr << "After sleep and validating [f|00003|0000000001, f|00003|0000000010)" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl << std::endl;
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

    mandatory_assert(srs.total_size() == 3);
}

void test_join1() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("c|<a_id:5>|<b_id:5>|<index:5> "
                               "a|<a_id>|<b_id> "
                               "b|<index>|<b_id>"));

    j1.ref();
    server.add_join("c|", "c}", &j1);

    String begin("c|00000|");
    String end("c|10000}");
    server.insert("a|00000|B0000", "a: index-only", true);
    server.validate(begin, end);
    CHECK_EQ(size_t(0), server.count(begin, end));

    server.insert("b|I0000|B0000", "b: real value", true);
    CHECK_EQ(size_t(1), server.count(begin, end));
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

typedef void (*test_func)();
std::vector<std::pair<String, test_func> > tests_;

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
#define ADD_TEST(test) tests_.push_back(std::pair<String, test_func>(#test, test))
        ADD_TEST(simple);
        ADD_TEST(recursive);
        ADD_TEST(count);
        ADD_TEST(annotation);
        ADD_TEST(srs);
        ADD_TEST(test_join1);
        for (auto& t : tests_) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
        }
        std::cerr << "PASS" << std::endl;
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
