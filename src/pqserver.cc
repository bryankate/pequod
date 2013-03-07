#include <boost/random/random_number_generator.hpp>
#include <unistd.h>
#include <set>
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "pqtwitter.hh"
#include "pqfacebook.hh"
#include "pqhackernews.hh"
#include "pqclient.hh"
#include "clp.h"
#include "time.hh"
// #include "hashclient.hh"

namespace pq {

// XXX check circular expansion

inline ServerRange::ServerRange(Str first, Str last, range_type type, Join* join)
    : ibegin_len_(first.length()), iend_len_(last.length()),
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

void ServerRange::validate(Str first, Str last, Server& server) {
    if (type_ == joinsink) {
        Match mf, ml;
        join_->sink().match(first, mf);
        join_->sink().match(last, ml);
        validate(mf, ml, 0, server, 0);
        if (join_->maintained() || join_->staleness())
            server.add_validjoin(first, last, join_);
    }
}


void ServerRange::validate(Match& mf, Match& ml, int joinpos, Server& server,
                           SourceAccumulator* accum) {
    uint8_t kf[128], kl[128], kaccum[128];
    int kflen = join_->source(joinpos).expand_first(kf, mf);
    int kllen = join_->source(joinpos).expand_last(kl, ml);
    int kaccumlen = 0;

    // need to validate the source ranges in case they have not been
    // expanded yet.
    // XXX PERFORMANCE this is not always necessary
    // XXX For now don't do this if the join is recursive
    if (table_name(Str(kf, kflen)) != join_->sink().table_name())
        server.validate(Str(kf, kflen), Str(kl, kllen));

    SourceRange* r = 0;
    if (joinpos + 1 == join_->nsource())
        r = join_->make_source(server, mf, Str(kf, kflen), Str(kl, kllen));

    bool check_accum = false;
    if (joinpos == join_->completion_source()
        && (accum = join_->make_accumulator(server))) {
        kaccumlen = join_->sink().expand_first(kaccum, mf);
        check_accum = !join_->sink().match_complete(mf)
            || !join_->sink().match_same(Str(kaccum, kaccumlen), ml);
        if (check_accum)
            kaccumlen = 0;
    }

    auto it = server.lower_bound(Str(kf, kflen));
    auto ilast = server.lower_bound(Str(kl, kllen));

    Match mk(mf);
    mk &= ml;
    Match::state mfstate(mf.save()), mlstate(ml.save()), mkstate(mk.save());
    const Pattern& pat = join_->source(joinpos);

    for (; it != ilast; ++it) {
	if (it->key().length() != pat.key_length())
            continue;

        if (r && !accum) {
            r->notify(it.operator->(), String(), SourceRange::notify_insert);
            continue;
        }

        // XXX PERFORMANCE can prob figure out ahead of time whether this
        // match is simple/necessary
        if (pat.match(it->key(), mk)) {
            if (check_accum
                && !join_->sink().match_same(Str(kaccum, kaccumlen), mk)) {
                if (kaccumlen)
                    accum->commit(Str(kaccum, kaccumlen));
                kaccumlen = join_->sink().expand_first(kaccum, mk);
            }

            if (r)
                accum->notify(it.operator->());
            else {
                pat.match(it->key(), mf);
                pat.match(it->key(), ml);
                validate(mf, ml, joinpos + 1, server, accum);
                mf.restore(mfstate);
                ml.restore(mlstate);
            }
        }

        mk.restore(mkstate);
    }

    if (accum && joinpos == join_->completion_source()) {
        if (kaccumlen)
            accum->commit(Str(kaccum, kaccumlen));
        delete accum;
    }

    if (r) {
        if (join_->maintained())
            server.add_copy(r);
        else
            delete r;
    }
}

std::ostream& operator<<(std::ostream& stream, const ServerRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << ")";
    if (r.type_ == ServerRange::joinsink)
        stream << ": joinsink @" << (void*) r.join_;
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

void ServerRangeSet::validate_join(ServerRange* jr, Server& server) {
    uint64_t now = tstamp();
    Str last_valid = first_;
    for (int i = 0; i != r_.size(); ++i)
        if (r_[i]
            && r_[i]->type() == ServerRange::validjoin
            && r_[i]->join() == jr->join()) {
            if (r_[i]->expired_at(now)) {
                // TODO: remove the expired validjoin from the server
                // (and possibly this set if it will be used after this validation)
                continue;
            }
            // XXX PERFORMANCE can often avoid this check
            if (last_valid < r_[i]->ibegin())
                jr->validate(last_valid, r_[i]->ibegin(), server);
            last_valid = r_[i]->iend();
        }
    if (last_valid < last_)
        jr->validate(last_valid, last_, server);
}

void ServerRangeSet::validate(Server& server) {
    for (int i = 0; i != r_.size(); ++i)
        if (r_[i]->type() == ServerRange::joinsink)
            validate_join(r_[i], server);
}

std::ostream& operator<<(std::ostream& stream, const ServerRangeSet& srs) {
    stream << "[";
    const char* sep = "";
    for (int i = 0; i != srs.r_.size(); ++i)
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

void Table::add_copy(SourceRange* r) {
    for (auto it = source_ranges_.begin_contains(r->interval());
	 it != source_ranges_.end(); ++it)
	if (it->join() == r->join()) {
	    // XXX may copy too much. This will not actually cause visible
	    // bugs I think?, but will grow the store
	    it->add_sinks(*r);
            delete r;
	    return;
	}
    source_ranges_.insert(r);
}

void Table::add_join(Str first, Str last, Join* join) {
    // track full ranges used for join and copy
    sink_ranges_.insert(ServerRange::make(first, last, ServerRange::joinsink,
					  join));
}

void Table::insert(const String& key, String value) {
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
        value = String();
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
        d->value_.swap(value);
    }
    notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
}

void Table::erase(const String& key) {
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end()) {
	Datum* d = it.operator->();
	store_.erase(it);
        notify(d, String(), SourceRange::notify_erase);
	delete d;
    }
}

Json Server::stats() const {
    size_t store_size = 0, source_ranges_size = 0, sink_ranges_size = 0;
    for (auto& t : tables_) {
        store_size += t.store_.size();
        source_ranges_size += t.source_ranges_.size();
        sink_ranges_size += t.sink_ranges_.size();
    }
    return Json().set("store_size", store_size)
	.set("source_ranges_size", source_ranges_size)
	.set("sink_ranges_size", sink_ranges_size);
}

void Server::print(std::ostream& stream) {
    stream << "sources:" << std::endl;
    bool any = false;
    for (auto& t : tables_by_name_)
        if (!t.source_ranges_.empty()) {
            stream << t.source_ranges_;
            any = true;
        }
    if (!any)
        stream << "<empty>\n";

    stream << "sinks:" << std::endl;
    any = false;
    for (auto& t : tables_by_name_)
        if (!t.sink_ranges_.empty()) {
            stream << t.sink_ranges_;
            any = true;
        }
    if (!any)
        stream << "<empty>\n";
}

} // namespace


static Clp_Option options[] = {
    { "push", 'p', 1000, 0, Clp_Negate },
    { "nusers", 'n', 1001, Clp_ValInt, 0 },
    { "facebook", 'f', 1002, 0, Clp_Negate },
    { "shape", 0, 1003, Clp_ValDouble, 0 },
    { "listen", 'l', 1004, Clp_ValInt, Clp_Optional },
    { "log", 0, 1005, 0, Clp_Negate },
    { "tests", 0, 1006, 0, 0 },
    { "hn", 'h', 1007, 0, Clp_Negate },
    { "narticles", 'a', 1008, Clp_ValInt, 0 },
    { "nops", 'o', 1009, Clp_ValInt, 0 },
    { "materialize", 'm', 1010, 0, Clp_Negate },
#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
    { "memcached", 'd', 1011, 0, Clp_Negate },
#endif
    { "builtinhash", 'b', 1012, 0, Clp_Negate }
};

enum { mode_unknown, mode_twitter, mode_hn, mode_facebook, mode_listen, mode_tests };

int main(int argc, char** argv) {
    String envstr("TAMER_NOLIBEVENT=1");
    putenv(envstr.mutable_data());
    tamer::initialize();

    int mode = mode_unknown, listen_port = 8000;
    Clp_Parser* clp = Clp_NewParser(argc, argv, sizeof(options) / sizeof(options[0]), options);
    Json tp_param = Json().set("nusers", 5000);
    std::set<String> testcases;
    while (Clp_Next(clp) != Clp_Done) {
	if (clp->option->long_name == String("push"))
	    tp_param.set("push", !clp->negated);
	else if (clp->option->long_name == String("nusers"))
	    tp_param.set("nusers", clp->val.i);
	else if (clp->option->long_name == String("narticles"))
	    tp_param.set("narticles", clp->val.i);
	else if (clp->option->long_name == String("nops"))
	    tp_param.set("nops", clp->val.i);
	else if (clp->option->long_name == String("shape"))
	    tp_param.set("shape", clp->val.d);
	else if (clp->option->long_name == String("materialize"))
	    tp_param.set("materialize", !clp->negated);
        else if (clp->option->long_name == String("memcached"))
            tp_param.set("memcached", !clp->negated);
        else if (clp->option->long_name == String("builtinhash"))
            tp_param.set("builtinhash", !clp->negated);
	else if (clp->option->long_name == String("facebook"))
            mode = mode_facebook;
        else if (clp->option->long_name == String("twitter"))
            mode = mode_twitter;
        else if (clp->option->long_name == String("hn"))
            mode = mode_hn;
        else if (clp->option->long_name == String("tests"))
            mode = mode_tests;
        else if (clp->option->long_name == String("listen")) {
            mode = mode_listen;
            if (clp->have_val)
                listen_port = clp->val.i;
        } else if (clp->option->long_name == String("log"))
            tp_param.set("log", !clp->negated);
        else
            testcases.insert(clp->vstr);
    }

    pq::Server server;
    if (mode == mode_tests || !testcases.empty()) {
        extern void unit_tests(const std::set<String> &);
        unit_tests(testcases);
    } else if (mode == mode_listen) {
        extern void server_loop(int port, pq::Server& server);
        server_loop(listen_port, server);
    } else if (mode == mode_twitter || mode == mode_unknown) {
        if (!tp_param.count("shape"))
            tp_param.set("shape", 8);
        pq::TwitterPopulator tp(tp_param);
        if (tp_param["memcached"]) {
#if HAVE_LIBMEMCACHED_MEMCACHED_HPP
            mandatory_assert(tp_param["push"], "memcached pull is not supported");
            pq::HashTwitter<pq::MemcachedClient> dc;
            pq::TwitterRunner<pq::HashTwitter<pq::MemcachedClient> > tr(dc, tp);
            tr.populate();
            tr.run();
#else
            mandatory_assert(false);
#endif
        } else if (tp_param["builtinhash"]) {
#if 0
            mandatory_assert(tp_param["push"], "builtinhash pull is not supported");
            pq::HashTwitter<pq::BuiltinHashClient> dc;
            pq::TwitterRunner<pq::HashTwitter<pq::BuiltinHashClient> > tr(dc, tp);
            tr.populate();
            tr.run();
#else
            mandatory_assert(false);
#endif
        } else {
            pq::DirectClient dc(server);
            pq::TwitterRunner<pq::DirectClient> tr(dc, tp);
            tr.populate();
            tr.run();
        }
    } else if (mode == mode_hn) {
        pq::HackernewsPopulator hp(tp_param);
        pq::HackernewsRunner hr(server, hp);
        hr.populate();
        hr.run();
    } else {
        //server.print(std::cout);
        if (!tp_param.count("shape"))
            tp_param.set("shape", 5);
        pq::FacebookPopulator fp(tp_param);
        pq::facebook_populate(server, fp);
        pq::facebook_run(server, fp);
    }
    //server.print(std::cout);

    tamer::loop();
    tamer::cleanup();
}
