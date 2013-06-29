// -*- mode: c++ -*-
#include <unistd.h>
#include <set>
#include <vector>
#include "pqserver.hh"
#include "pqjoin.hh"
#include "pqinterconnect.hh"
#include "json.hh"
#include "error.hh"
#include <sys/resource.h>

namespace pq {

const Datum Datum::empty_datum{Str()};
const Datum Datum::max_datum(Str("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"));
Table Table::empty_table{Str(), nullptr, nullptr};
const char Datum::table_marker[] = "TABLE MARKER";

void Table::iterator::fix() {
 retry:
    if (it_ == table_->store_.end()) {
        if (table_->parent_) {
            it_ = table_->parent_->store_.iterator_to(*table_);
            table_ = table_->parent_;
            ++it_;
            goto retry;
        }
    } else if (it_->is_table()) {
        table_ = &it_->table();
        it_ = table_->store_.begin();
        goto retry;
    }
}

Table::Table(Str name, Table* parent, Server* server)
    : Datum(name, String::make_stable(Datum::table_marker)),
      triecut_(0), njoins_(0), flush_at_(0), all_pull_(true),
      server_{server}, parent_{parent}, 
      ninsert_(0), nmodify_(0), nmodify_nohint_(0), nerase_(0), nvalidate_(0),
      nevict_remote_(0), nevict_local_(0), nevict_sink_(0) {
}

Table::~Table() {
    while (SourceRange* r = source_ranges_.unlink_leftmost_without_rebalance()) {
        r->clear_without_deref();
        delete r;
    }
    while (JoinRange* r = join_ranges_.unlink_leftmost_without_rebalance())
        delete r;
    // delete store last since join_ranges_ have refs to Datums
    while (Datum* d = store_.unlink_leftmost_without_rebalance()) {
        if (d->is_table())
            delete &d->table();
        else
            delete d;
    }
}

Table* Table::next_table_for(Str key) {
    if (subtable_hashable()) {
        if (Table** tp = subtables_.get_pointer(subtable_hash_for(key)))
            return *tp;
    } else {
        auto it = store_.lower_bound(key.prefix(triecut_), DatumCompare());
        if (it != store_.end() && it->key() == key.prefix(triecut_))
            return &it->table();
    }
    return this;
}

Table* Table::make_next_table_for(Str key) {
    bool can_hash = subtable_hashable();
    if (can_hash) {
        if (Table* t = subtables_[subtable_hash_for(key)])
            return t;
    }

    auto it = store_.lower_bound(key.prefix(triecut_), DatumCompare());
    if (it != store_.end() && it->key() == key.prefix(triecut_))
        return &it->table();

    Table* t = new Table(key.prefix(triecut_), this, server_);
    t->all_pull_ = false;
    store_.insert_before(it, *t);

    if (can_hash)
        subtables_[subtable_hash_for(key)] = t;

    return t;
}

auto Table::lower_bound(Str key) -> iterator {
    Table* tbl = this;
    int len;
 retry:
    len = tbl->triecut_ ? tbl->triecut_ : key.length();
    auto it = tbl->store_.lower_bound(key.prefix(len), DatumCompare());
    if (len == tbl->triecut_ && it != tbl->store_.end() && it->key() == key.prefix(len)) {
        assert(it->is_table());
        tbl = static_cast<Table*>(it.operator->());
        goto retry;
    }
    return iterator(tbl, it);
}

size_t Table::count(Str key) const {
    const Table* tbl = this;
    int len;
 retry:
    len = tbl->triecut_ ? tbl->triecut_ : key.length();
    auto it = tbl->store_.lower_bound(key.prefix(len), DatumCompare());
    if (it != tbl->store_.end() && it->key() == key.prefix(len)) {
        if (len == tbl->triecut_) {
            assert(it->is_table());
            tbl = static_cast<const Table*>(it.operator->());
            goto retry;
        } else
            return 1;
    }
    return 0;
}

size_t Table::size() const {
    size_t x = store_.size();
    if (triecut_)
        for (auto& d : store_)
            if (d.is_table())
                x += static_cast<const Table&>(d).size();
    return x;
}

void Table::add_source(SourceRange* r) {
    for (auto it = source_ranges_.begin_contains(r->interval());
	 it != source_ranges_.end(); ++it)
	if (it->join() == r->join() && it->joinpos() == r->joinpos()) {
	    // XXX may copy too much. This will not actually cause visible
	    // bugs I think?, but will grow the store
	    it->take_results(*r);
            delete r;
	    return;
	}
    source_ranges_.insert(*r);
}

void Table::remove_source(Str first, Str last, SinkRange* sink, Str context) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
	 it != source_ranges_.end(); ) {
        SourceRange* source = it.operator->();
        ++it;
	if (source->join() == sink->join())
            source->remove_sink(sink, context);
    }
}

void Table::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    FileErrorHandler xerrh(stderr);
    errh = errh ? errh : &xerrh;

    // check for redundant join
    for (auto it = join_ranges_.begin_overlaps(first, last);
         it != join_ranges_.end(); ++it)
        if (it->join()->same_structure(*join)) {
            errh->error("join on [%p{Str}, %p{Str}) has same structure as overlapping join\n(new join ignored)", &first, &last);
            return;
        }

    join_ranges_.insert(*new JoinRange(first, last, join));
    if (join->maintained() || join->staleness())
        all_pull_ = false;
    ++njoins_;
}

void Server::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    join->attach(*this);
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_join(first, last, join, errh);

    // handle cuts: push only
    if (join->maintained())
        for (int i = 0; i != join->npattern(); ++i) {
            Table& t = make_table(join->pattern(i).table_name());
            int tc = join->pattern_subtable_length(i);
            if (t.triecut_ == 0 && t.store_.empty() && tc)
                t.triecut_ = tc;
        }
}

auto Table::insert(Table& t) -> local_iterator {
    assert(!triecut_ || t.name().length() < triecut_);
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(t.name(), DatumCompare(), cd);
    assert(p.second);
    return store_.insert_commit(t, cd);
}

void Table::insert(Str key, String value) {
    assert(!triecut_ || key.length() < triecut_);

    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
        value = String();
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
        d->value().swap(value);
    }

    PersistentStore* pstore = server_->persistent_store();
    if (unlikely(pstore && server_->is_owned_public(server_->owner_for(key))))
        pstore->put(key, value);

    notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    ++ninsert_;
    all_pull_ = false;
}

void Table::erase(Str key) {
    assert(!triecut_ || key.length() < triecut_);
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end())
        erase(iterator(this, it));
    ++nerase_;
}

std::pair<ServerStore::iterator, bool> Table::prepare_modify(Str key, const SinkRange* sink, ServerStore::insert_commit_data& cd) {
    assert(name() && sink);
    assert(!triecut_ || key.length() < triecut_);
    std::pair<ServerStore::iterator, bool> p;
    Datum* hint = sink->hint();
    if (!hint || !hint->valid()) {
        ++nmodify_nohint_;
        p = store_.insert_check(key, DatumCompare(), cd);
    } else {
        p.first = store_.iterator_to(*hint);
        if (hint->key() == key)
            p.second = false;
        else if (hint == store_.rbegin().operator->())
            p = store_.insert_check(store_.end(), key, DatumCompare(), cd);
        else {
            ++p.first;
            p = store_.insert_check(p.first, key, DatumCompare(), cd);
        }
    }
    return p;
}

void Table::finish_modify(std::pair<ServerStore::iterator, bool> p,
                          const store_type::insert_commit_data& cd,
                          Datum* d, Str key, const SinkRange* sink,
                          String value) {
    SourceRange::notify_type n = SourceRange::notify_update;
    if (!is_marker(value)) {
        if (p.second) {
            d = new Datum(key, sink);
            sink->add_datum(d);
            p.first = store_.insert_commit(*d, cd);
            n = SourceRange::notify_insert;
        }
    } else if (is_erase_marker(value)) {
        if (!p.second) {
            p.first = store_.erase(p.first);
            n = SourceRange::notify_erase;
        } else
            goto done;
    } else if (is_invalidate_marker(value)) {
        invalidate_dependents(d->key());
        const_cast<SinkRange*>(sink)->add_invalidate(key);
        goto done;
    } else
        goto done;

    d->value().swap(value);
    notify(d, value, n);
    if (n == SourceRange::notify_erase)
        d->invalidate();

 done:
    sink->update_hint(store_, p.first);
    ++nmodify_;
}

std::pair<bool, Table::iterator> Table::validate(Str first, Str last, uint64_t now,
                                                 tamer::gather_rendezvous& gr) {
    Table* t = this;
    while (t->parent_->triecut_)
        t = t->parent_;

    bool completed = true;
    int32_t owner = server_->owner_for(first);

    // todo: if we guarantee that a subtable is all remote, we can
    // do this check once when we create the subtable.
    if (server_->is_remote(owner)) {
        Str have = first;
        auto r = t->remote_ranges_.begin_overlaps(first, last);

        while(r != t->remote_ranges_.end()) {
            if (r->ibegin() > have) {
                if (last < r->ibegin())
                    break;
                else {
                    t->fetch_remote(have, r->ibegin(), owner, gr.make_event());
                    completed = false;
                }
            }
            have = r->iend();

            if (r->pending()) {
                r->add_waiting(gr.make_event());
                completed = false;
            }

            if (r->evicted()) {
                RemoteRange* rr = r.operator->();
                ++r;

                // todo: re-evict data here?
                // no - since we have been getting
                // updates to the table, the data should
                // be overwritten anyway on the remote fetch
                t->remote_ranges_.erase(*rr);
                t->invalidate_dependents(rr->ibegin(), rr->iend());

                server_->interconnect(owner)->unsubscribe(rr->ibegin(), rr->iend(),
                                                          server_->me(), tamer::event<>());
                t->fetch_remote(rr->ibegin(), rr->iend(), owner, gr.make_event());

                completed = false;
                delete rr;
            }
            else {
                if (!r->pending())
                    server_->lru_touch(r.operator->());
                ++r;
            }

            if (have >= last)
                break;
        }

        if (have < last) {
            t->fetch_remote(have, last, owner, gr.make_event());
            completed = false;
        }
    }
    else if (t->njoins_ != 0) {
        if (t->njoins_ == 1) {
            auto it = store_.lower_bound(first, DatumCompare());
            auto itx = it;
            if ((itx == store_.end() || itx->key() >= last) && itx != store_.begin())
                --itx;
            if (itx != store_.end() && itx->key() < last && itx->owner()
                    && itx->owner()->valid() && !itx->owner()->has_expired(now)
                    && itx->owner()->ibegin() <= first && last <= itx->owner()->iend()
                    && !itx->owner()->need_restart()
                    && !itx->owner()->need_update()) {
                if (itx->owner()->join()->maintained())
                    server_->lru_touch(const_cast<SinkRange*>(itx->owner()));
                return std::make_pair(true, iterator(this, it));
            }
        }
        for (auto it = t->join_ranges_.begin_overlaps(first, last);
                it != t->join_ranges_.end(); ++it)
            completed &= it->validate(first, last, *server_, now, gr);
    }

    return std::make_pair(completed, lower_bound(first));
}

void Table::notify(Datum* d, const String& old_value, SourceRange::notify_type notifier) {
    Str key(d->key());
    Table* t = &table_for(key);
 retry:
    for (auto it = t->source_ranges_.begin_contains(key);
         it != t->source_ranges_.end(); ) {
        // SourceRange::notify() might remove the SourceRange from the tree
        SourceRange* source = it.operator->();
        ++it;
        if (source->check_match(key))
            source->notify(d, old_value, notifier);
    }
    if ((t = t->parent_) && t->triecut_)
        goto retry;
}

void Table::invalidate_dependents(Str key) {
    Table* t = &table_for(key);
 retry:
    for (auto it = t->source_ranges_.begin_contains(key);
         it != t->source_ranges_.end(); ) {
        // simple, but obviously we could do better
        SourceRange* source = it.operator->();
        ++it;
        source->invalidate();
    }
    if ((t = t->parent_) && t->triecut_)
        goto retry;
}

inline void Table::invalidate_dependents_local(Str first, Str last) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
         it != source_ranges_.end(); ) {
        // simple, but obviously we could do better
        SourceRange* source = it.operator->();
        ++it;
        source->invalidate();
    }
}

void Table::invalidate_dependents_down(Str first, Str last) {
    for (auto it = store_.lower_bound(first.prefix(triecut_), DatumCompare());
         it != store_.end() && it->key() < last;
         ++it)
        if (it->is_table()) {
            it->table().invalidate_dependents_local(first, last);
            if (it->table().triecut_)
                it->table().invalidate_dependents_down(first, last);
        }
}

void Table::invalidate_dependents(Str first, Str last) {
    Table* t = &table_for(first, last);
    if (triecut_)
        t->invalidate_dependents_down(first, last);
 retry:
    t->invalidate_dependents_local(first, last);
    if ((t = t->parent_) && t->triecut_)
        goto retry;
}

bool Table::hard_flush_for_pull(uint64_t now) {
    while (Datum* d = store_.unlink_leftmost_without_rebalance()) {
        assert(!d->is_table());
        invalidate_dependents(d->key());
        d->invalidate();
    }
    flush_at_ = now;
    return true;
}

tamed void Table::fetch_remote(String first, String last, int32_t owner,
                               tamer::event<> done) {
    assert(!triecut_);

    tvars {
        RemoteRange* rr = new RemoteRange(this, first, last, owner);
        Interconnect::scan_result res;
    }

    rr->add_waiting(done);
    remote_ranges_.insert(*rr);

    //std::cerr << "fetching remote data: [" << first << ", " << last << std::endl;
    twait {
        server_->interconnect(owner)->subscribe(first, last, server_->me(),
                                                make_event(res));
    }
    twait { server_->interconnect(owner)->pace(make_event()); }

    // XXX: not sure if this is correct. what if the range goes outside this triecut?
    Table& sourcet = server_->make_table_for(first);
    for (auto it = res.begin(); it != res.end(); ++it)
        sourcet.insert(it->key(), it->value());

    server_->lru_add(rr);
    rr->notify_waiting();
}

void Table::evict_remote(RemoteRange* rr) {
    assert(!triecut_);
    assert(!rr->pending());
    assert(!rr->evicted());

    uint32_t kept = 0;
    auto sit = source_ranges_.begin_overlaps(rr->ibegin(), rr->iend());
    while(sit != source_ranges_.end()) {
        SourceRange* sr = sit.operator->();
        ++sit;

        if (!sr->can_evict()) {
            source_ranges_.erase(*sr);
            sr->invalidate();
        }
        else
            ++kept;
    }

    auto it = lower_bound(rr->ibegin());
    auto itx = lower_bound(rr->iend());
    while(it != itx) {
        it = erase_invalid(it);
        ++nevict_remote_;
    }

    //std::cerr << "evicting remote range [" << rr->ibegin() << ", " << rr->iend()
    //          << "), keeping " << kept << " source ranges in place " << std::endl;

    server_->lru_remove(rr);

    if (!kept) {
        server_->interconnect(rr->owner())->unsubscribe(rr->ibegin(), rr->iend(),
                                                        server_->me(), tamer::event<>());
        remote_ranges_.erase(*rr);
        delete rr;
    }
    else
        rr->mark_evicted();
}

void Table::evict_sink(SinkRange* sink) {
    assert(!triecut_);
    assert(!sink->need_restart());

    //std::cerr << "evicting sink range [" << sink->ibegin() << ", "
    //          << sink->iend() << ")" << std::endl;

    uint64_t before = SinkRange::invalidate_hit_keys;
    sink->invalidate(); // lru removal is handled within
    nevict_sink_ += (SinkRange::invalidate_hit_keys - before);
}

void Table::add_subscription(Str first, Str last, int32_t peer) {
    assert(peer != server_->me());

    //std::cerr << "subscribing " << peer << " to range [" << first << ", " << last << ")" << std::endl;
    SourceRange::parameters p {*server_, nullptr, -1, Match(),
                               first, last, server_->remote_sink(peer)};

    // todo: ensure subscription invariant holds
    add_source(new SubscribedRange(p));
}

void Table::remove_subscription(Str first, Str last, int32_t peer) {
    assert(peer != server_->me());

    //std::cerr << "unsubscribing " << peer << " from range [" << first << ", " << last << ")" << std::endl;
    remove_source(first, last, server_->remote_sink(peer), Str());
}

void Table::invalidate_remote(Str first, Str last) {
    Table* t = this;
    while (t->parent_->triecut_)
        t = t->parent_;

    // todo: truncate range instead of invalidating whole range?
    //std::cerr << "invalidating remote range [" << first << ", " << last << ")" << std::endl;
    auto r = t->remote_ranges_.begin_overlaps(first, last);
    while(r != t->remote_ranges_.end()) {
        RemoteRange* rrange = r.operator->();
        ++r;

        t->remote_ranges_.erase(*rrange);
        t->invalidate_dependents(rrange->ibegin(), rrange->iend());

        auto it = t->lower_bound(rrange->ibegin());
        auto itend = t->lower_bound(rrange->iend());
        while(it != itend)
            it = erase_invalid(it);

        if (!rrange->pending() && !rrange->evicted())
            server_->lru_remove(rrange);
        delete rrange;
    }
}


Server::Server()
    : persistent_store_(nullptr), supertable_(Str(), nullptr, this),
      last_validate_at_(0), validate_time_(0), insert_time_(0),
      part_(nullptr), me_(-1), prob_rng_(0,1), pevict_(0) {

    gen_.seed(112181);
}

Server::~Server() {
    for (auto& s : remote_sinks_)
        s->deref();

    if (persistent_store_)
        delete persistent_store_;
}

auto Server::create_table(Str tname) -> Table::local_iterator {
    assert(tname);
    Table* t = new Table(tname, &supertable_, this);
    return supertable_.insert(*t);
}

tamed void Server::validate(Str key, tamer::event<Table::iterator> done) {
    tvars {
        struct timeval tv[2];
        std::pair<bool, Table::iterator> it;
        tamer::gather_rendezvous gr;
        Table* t = &this->make_table_for(key);
    }

    gettimeofday(&tv[0], NULL);

    do {
        it = t->validate(key, next_validate_at(), gr);
        twait(gr);
    } while(!it.first);

    gettimeofday(&tv[1], NULL);
    validate_time_ += to_real(tv[1] - tv[0]);

    maybe_evict();
    done(it.second);
}

tamed void Server::validate(Str first, Str last, tamer::event<Table::iterator> done) {
    tvars {
        struct timeval tv[2];
        std::pair<bool, Table::iterator> it;
        tamer::gather_rendezvous gr;
        Table* t = &this->make_table_for(first, last);
    }

    gettimeofday(&tv[0], NULL);

    do {
        it = t->validate(first, last, next_validate_at(), gr);
        twait(gr);
    } while(!it.first);

    gettimeofday(&tv[1], NULL);
    validate_time_ += to_real(tv[1] - tv[0]);

    maybe_evict();
    done(it.second);
}

tamed void Server::validate_count(Str first, Str last, tamer::event<size_t> done) {
    tvars {
        Table::iterator it;
    }

    twait { validate(first, last, make_event(it)); }
    auto itend = make_table_for(first, last).end();
    size_t n = 0;

    for (; it != itend && it->key() < last; ++it)
        ++n;

    done(n);
}

void Table::add_stats(Json& j) const {
    j["ninsert"] += ninsert_;
    j["nmodify"] += nmodify_;
    j["nmodify_nohint"] += nmodify_nohint_;
    j["nerase"] += nerase_;
    j["store_size"] += store_.size();
    j["source_ranges_size"] += source_ranges_.size();
    for (auto& jr : join_ranges_)
        j["sink_ranges_size"] += jr.valid_ranges_size();
    j["remote_ranges_size"] += remote_ranges_.size();
    j["nvalidate"] += nvalidate_;
    j["nevict_remote"] += nevict_remote_;
    j["nevict_local"] += nevict_local_;
    j["nevict_sink"] += nevict_sink_;

    if (triecut_)
        for (auto& d : store_)
            if (d.is_table()) {
                j["nsubtables"]++;
                j["store_size"]--;
                d.table().add_stats(j);
            }
}

Json Server::stats() const {
    size_t store_size = 0, source_ranges_size = 0, join_ranges_size = 0,
           sink_ranges_size = 0, remote_ranges_size = 0;
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);

    Json tables = Json::make_array();
    for (auto it = supertable_.lbegin(); it != supertable_.lend(); ++it) {
        assert(it->is_table());
        Table& t = it->table();
        Json j = Json().set("name", t.name());
        t.add_stats(j);
        for (auto it = j.obegin(); it != j.oend(); )
            if (it->second.is_i() && !it->second.as_i())
                it = j.erase(it);
            else
                ++it;
        tables.push_back(j);

        store_size += j["store_size"].to_i();
        source_ranges_size += j["source_ranges_size"].to_i();
        join_ranges_size += t.join_ranges_.size();
        sink_ranges_size += j["sink_ranges_size"].to_i();
        remote_ranges_size += j["remote_ranges_size"].to_i();
    }

    Json answer;
    answer.set("store_size", store_size)
	.set("source_ranges_size", source_ranges_size)
	.set("join_ranges_size", join_ranges_size)
	.set("valid_ranges_size", sink_ranges_size)
        .set("server_user_time", to_real(ru.ru_utime))
        .set("server_system_time", to_real(ru.ru_stime))
        .set("server_validate_time", validate_time_)
        .set("server_insert_time", insert_time_)
        .set("server_other_time", to_real(ru.ru_utime + ru.ru_stime) - validate_time_ - insert_time_)
        .set("server_max_rss_mb", ru.ru_maxrss / 1024);
    if (SourceRange::allocated_key_bytes)
        answer.set("source_allocated_key_bytes", SourceRange::allocated_key_bytes);
    if (ServerRangeBase::allocated_key_bytes)
        answer.set("sink_allocated_key_bytes", ServerRangeBase::allocated_key_bytes);
    if (SinkRange::invalidate_hit_keys)
        answer.set("invalidate_hits", SinkRange::invalidate_hit_keys);
    if (SinkRange::invalidate_miss_keys)
        answer.set("invalidate_misses", SinkRange::invalidate_miss_keys);
    return answer.set("tables", tables);
}

void Server::control(const Json& cmd) {
    if (cmd["quit"])
        exit(0);
    else if (cmd["print"])
        print(std::cerr);
    else if (cmd["print_table_keys"]) {
        String tname = table_name(cmd["print_table_keys"].as_s());
        assert(tname);
        Table& t = table(tname);
        for (auto it = t.begin(); it != t.end(); ++it)
            std::cerr << it->key() << std::endl;
    }
}

void Table::print_sources(std::ostream& stream) const {
    stream << source_ranges_;
}

void Server::print(std::ostream& stream) {
    stream << "sources:" << std::endl;
    bool any = false;
    for (auto it = supertable_.lbegin(); it != supertable_.lend(); ++it) {
        assert(it->is_table());
        Table& t = it->table();
        if (!t.source_ranges_.empty()) {
            stream << t.source_ranges_;
            any = true;
        }
    }
    if (!any)
        stream << "<empty>\n";
}

} // namespace
