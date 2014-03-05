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
        if (table_->parent_ && (!top_ || top_ != table_)) {
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
      triecut_(0), njoins_(0), server_{server}, parent_{parent}, 
      ninsert_(0), nmodify_(0), nmodify_nohint_(0), nerase_(0), nvalidate_(0),
      nevict_persistent_(0), nevict_remote_(0), nevict_sink_(0), 
      nevict_range_reload_(0) {

    memset(&nsubtables_with_ranges_, 0, sizeof(nsubtables_with_ranges_));
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
        auto it = store_.lower_bound(key.prefix(triecut_), KeyCompare());
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

    auto it = store_.lower_bound(key.prefix(triecut_), KeyCompare());
    if (it != store_.end() && it->key() == key.prefix(triecut_))
        return &it->table();

    Table* t = new Table(key.prefix(triecut_), this, server_);
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
    auto it = tbl->store_.lower_bound(key.prefix(len), KeyCompare());
    if (len == tbl->triecut_ && it != tbl->store_.end() && it->key() == key.prefix(len)) {
        assert(it->is_table());
        tbl = static_cast<Table*>(it.operator->());
        goto retry;
    }
    return iterator(tbl, it, this); // will not iterate out of subtable
}

size_t Table::count(Str key) const {
    const Table* tbl = this;
    int len;
 retry:
    len = tbl->triecut_ ? tbl->triecut_ : key.length();
    auto it = tbl->store_.lower_bound(key.prefix(len), KeyCompare());
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

void Table::remove_source(Str first, Str last, Sink* sink, Str context) {
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
    auto p = store_.insert_check(t.name(), KeyCompare(), cd);
    assert(p.second);
    return store_.insert_commit(t, cd);
}

tamed void Table::insert(Str key, String value, tamer::event<> done) {
    tvars {
        int32_t owner = this->server_->owner_for(key);
    }

    // belongs on a remote server. send it along and wait for the write
    // to return before writing locally.
    if (unlikely(server_->is_remote(owner)))
        twait { server_->interconnect(owner)->insert(key, value, make_event()); }
    else if (unlikely(server_->writethrough() && server_->is_owned_public(owner)))
        twait { server_->persistent_store()->put(key, value, make_event()); }

    insert(key, value);
    done();
}

void Table::insert(Str key, String value) {
    assert(!triecut_ || key.length() < triecut_);

    //std::cerr << "INSERT: " << key << std::endl;
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, KeyCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
        value = String();
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
        d->value().swap(value);
    }

    notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    ++ninsert_;
}

tamed void Table::erase(Str key, tamer::event<> done) {
    tvars {
        int32_t owner = this->server_->owner_for(key);
    }

    // belongs on a remote server. send it along and wait for the write
    // to return before writing locally.
    if (unlikely(server_->is_remote(owner)))
        twait { server_->interconnect(owner)->erase(key, make_event()); }
    else if (unlikely(server_->writethrough() && server_->is_owned_public(owner)))
        twait { server_->persistent_store()->erase(key, make_event()); }

    erase(key);
    done();
}

void Table::erase(Str key) {
    assert(!triecut_ || key.length() < triecut_);

    //std::cerr << "ERASE: " << key << std::endl;
    auto it = store_.find(key, KeyCompare());
    if (it != store_.end())
        erase(iterator(this, it));
    else if (enable_memory_tracking) {
        // if eviction is enabled, the key being erased might have already
        // been evicted. generate a special notification because some
        // source ranges (e.g. CopySourceRange) can handle this case.
        // the notification will only be passed to sources that cover evicted ranges
        Datum tmp(key, erase_marker());
        notify(&tmp, erase_marker(), SourceRange::notify_erase_missing);
    }
    ++nerase_;
}

std::pair<ServerStore::iterator, bool> Table::prepare_modify(Str key, const Sink* sink,
                                                             ServerStore::insert_commit_data& cd) {
    assert(name() && sink);
    assert(!triecut_ || key.length() < triecut_);
    std::pair<ServerStore::iterator, bool> p;
    Datum* hint = sink->hint();
    if (!hint || !hint->valid()) {
        ++nmodify_nohint_;
        p = store_.insert_check(key, KeyCompare(), cd);
    } else {
        p.first = store_.iterator_to(*hint);
        if (hint->key() == key)
            p.second = false;
        else if (hint == store_.rbegin().operator->())
            p = store_.insert_check(store_.end(), key, KeyCompare(), cd);
        else {
            ++p.first;
            p = store_.insert_check(p.first, key, KeyCompare(), cd);
        }
    }
    return p;
}

void Table::finish_modify(std::pair<ServerStore::iterator, bool> p,
                          const store_type::insert_commit_data& cd,
                          Datum* d, Str key, const Sink* sink,
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
        const_cast<Sink*>(sink)->add_invalidate(key);
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

static bool cross_table_warning = false;

std::pair<bool, Table::iterator> Table::validate_local(Str first, Str last,
                                                       uint64_t now, uint32_t& log,
                                                       tamer::gather_rendezvous& gr) {
    if (triecut_ && !cross_table_warning) {
        std::cerr << "warning: [" << first << "," << last << ") crosses subtable boundary\n";
        cross_table_warning = true;
    }

    Table* t = this;
    while (t->parent_->triecut_)
        t = t->parent_;

    bool completed = true;

    if (t->njoins_) {
        //std::cerr << "validating join range [" << first << ", " << last << ")" << std::endl;

        // first, lookup a key in this range. if it's SinkRange is valid and
        // covers the whole lookup we do not need to do anymore work
        auto kit = store_.lower_bound(first, KeyCompare());
        auto kitx = kit;
        SinkRange* sr = nullptr;

        if ((kitx == store_.end() || kitx->key() >= last) && kitx != store_.begin())
            --kitx;
        if (kitx != store_.end() && kitx->key() < last && kitx->owner()) {
            sr = const_cast<SinkRange*>(kitx->owner()->range());

            // single range covers lookup?
            if (sr->ibegin() <= first && last <= sr->iend()) {
                if (sr->valid(now)) {
                    server_->lru_touch(sr);
                    return std::make_pair(true, iterator(this, kit, this));
                }
            }
            else
                sr = nullptr;
        }

        // we found a single SinkRange above that covers the lookup range, 
        // but it is invalid. just try to validate it and move on
        if (sr) {
            if (sr->validate(first, last, *server_, now, log, gr)) {
                server_->lru_touch(sr);
                return std::make_pair(true, lower_bound(first));
            }
            else
                return std::make_pair(false, end());
        }

        // either no valid SinkRange was found or the lookup is 
        // outside the span of the SinkRange found. fill in the gaps.
        local_vector<SinkRange*, 4> ranges;
        collect_ranges(first, last, ranges,
                       &Table::sink_ranges_, &Table::swr::sink);

        Str have = first;
        uint32_t inserted = 0;

        auto it = ranges.begin();
        while (have < last) {
            if (it != ranges.end() && have >= (*it)->ibegin()) {
                sr = *it;
                //std::cerr << "  existing sink range: " << sr->interval() << std::endl;
                completed &= sr->validate(first, last, *server_, now, log, gr);
                ++it;
            }
            else {
                Str sr_last = last;
                if (it != ranges.end() && sr_last > (*it)->ibegin())
                    sr_last = (*it)->ibegin();

                sr = new SinkRange(have, sr_last, this);
                for (auto j = t->join_ranges_.begin_overlaps(first, last);
                        j != t->join_ranges_.end(); ++j) {

                    //std::cerr << "  adding join to sink range: " << j->interval()
                    //          << " -> " << sr->interval() << std::endl;
                    completed &= sr->add_sink(j.operator->(), *server_, now, log, gr);
                }

                sink_ranges_.insert(*sr);
                server_->lru_touch(sr);
                ++inserted;
            }

            have = sr->iend();
        }

        if (inserted)
            for (t = parent_; t; t = t->parent_)
                t->nsubtables_with_ranges_.sink += inserted;
    }
    else if (server_->persistent_store()) {
        Str have = first;
        bool fetching = false;
        int32_t inserted = 0;

        local_vector<PersistedRange*, 4> ranges;
        collect_ranges(first, last, ranges,
                       &Table::persisted_ranges_, &Table::swr::persisted);

        for (auto r = ranges.begin(); r != ranges.end(); ++r) {
            PersistedRange* pr = *r;

            if (pr->ibegin() > have) {
                if (last < pr->ibegin())
                    break;
                else {
                    PersistedRange* pri = new PersistedRange(this, have, pr->ibegin());
                    persisted_ranges_.insert(*pri);
                    server_->lru_touch(pri);
                    ++inserted;
                }
            }
            have = pr->iend();

            if (pr->pending()) {
                pr->add_waiting(gr.make_event());
                fetching = true;
            }

            if (pr->evicted()) {
                t = pr->table();
                t->persisted_ranges_.erase(*pr);
                --inserted;

                // todo: do not invalidate remote sinks - some peers might have the range
                // and a scan by another peer will invalidate all the others!
                t->invalidate_dependents(pr->ibegin(), pr->iend());
                t->nevict_persistent_ += t->erase_purge(pr->ibegin(), pr->iend());
                ++t->nevict_range_reload_;

                t->fetch_persisted(pr->ibegin(), pr->iend(), gr.make_event());
                fetching = true;
                delete pr;
            }
            else if (!pr->pending())
                server_->lru_touch(pr);

            if (have >= last)
                break;
        }

        if (have < last) {
            PersistedRange* pri = new PersistedRange(this, have, last);
            persisted_ranges_.insert(*pri);
            server_->lru_touch(pri);
            ++inserted;
        }

        if (inserted)
            for (t = parent_; t; t = t->parent_)
                t->nsubtables_with_ranges_.persisted += inserted;

        if (fetching)
            log |= ValidateRecord::fetch_persisted;

        completed &= !fetching;
    }

    return std::make_pair(completed, lower_bound(first));
}

std::pair<bool, Table::iterator> Table::validate_remote(Str first, Str last,
                                                        int32_t owner, uint32_t& log,
                                                        tamer::gather_rendezvous& gr) {
    if (triecut_ && !cross_table_warning) {
        std::cerr << "warning: [" << first << "," << last << ") crosses subtable boundary\n";
        cross_table_warning = true;
    }

    local_vector<RemoteRange*, 4> ranges;
    collect_ranges(first, last, ranges,
                   &Table::remote_ranges_, &Table::swr::remote);

    bool completed = true;
    bool fetching = false;
    Str have = first;

    for (auto r = ranges.begin(); r != ranges.end(); ++r) {
        RemoteRange* rr = *r;

        if (rr->ibegin() > have) {
            if (last < rr->ibegin())
                break;
            else {
                fetch_remote(have, rr->ibegin(), owner, gr.make_event());
                fetching = true;
            }
        }
        have = rr->iend();

        if (rr->pending()) {
            rr->add_waiting(gr.make_event());
            fetching = true;
        }

        if (rr->evicted()) {
            Table* rrt = rr->table();

            ++rrt->nevict_range_reload_;
            nevict_remote_ += rrt->erase_purge(rr->ibegin(), rr->iend());
            rrt->invalidate_dependents(rr->ibegin(), rr->iend());
            rrt->remote_ranges_.erase(*rr);

            for (Table* t = rrt->parent_; t; t = t->parent_)
                --t->nsubtables_with_ranges_.remote;

            server_->interconnect(owner)->unsubscribe(rr->ibegin(), rr->iend(),
                                                      server_->me(), tamer::event<>());

            rrt->fetch_remote(rr->ibegin(), rr->iend(), owner, gr.make_event());
            fetching = true;
            delete rr;
        }
        else if (!rr->pending())
            server_->lru_touch(rr);

        if (have >= last)
            break;
    }

    if (have < last) {
        fetch_remote(have, last, owner, gr.make_event());
        fetching = true;
    }

    if (fetching)
        log |= ValidateRecord::fetch_remote;

    completed &= !fetching;
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
        if (enable_memory_tracking &&
            notifier == SourceRange::notify_erase_missing &&
            !source->purged())
            continue;
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
    for (auto it = store_.lower_bound(first.prefix(triecut_), KeyCompare());
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

tamed void Table::fetch_persisted(String first, String last, tamer::event<> done) {
    tvars {
        PersistedRange* pr = new PersistedRange(this, first, last);
        PersistentStore::ResultSet res;
    }

    pr->add_waiting(done);
    persisted_ranges_.insert(*pr);

    for (Table* t = parent_; t; t = t->parent_)
        ++t->nsubtables_with_ranges_.persisted;

    //std::cerr << "fetching persisted data: " << pr->interval() << std::endl;
    twait { server_->persistent_store()->scan(first, last, make_event(res)); }

    //std::cerr << "persisted data fetch: " << pr->interval() << " returned "
    //          << res.size() << " results" << std::endl;

    for (auto it = res.begin(); it != res.end(); ++it)
        server_->make_table_for(it->first).insert(it->first, it->second);

    server_->lru_touch(pr);
    pr->notify_waiting();
}

void Table::evict_persisted(PersistedRange* pr) {
    assert(!pr->pending());
    assert(!nsubtables_with_ranges_.persisted);

    uint32_t kept = 0;
    Table* t = this;

    retry:
    for (auto sit = t->source_ranges_.begin_overlaps(pr->ibegin(), pr->iend());
            sit != t->source_ranges_.end(); ++sit)
        if (sit->purge(*server_))
            ++kept;

    if ((t = t->parent_) && t->triecut_)
        goto retry;

    nevict_persistent_ += erase_purge(pr->ibegin(), pr->iend());
    
    //std::cerr << "evicting persisted range " << pr->interval()
    //          << ", keeping " << kept << " source ranges in place " << std::endl;

    if (!kept) {
        for (Table* t = parent_; t; t = t->parent_)
            --t->nsubtables_with_ranges_.persisted;

        persisted_ranges_.erase(*pr);
        delete pr;
    }
    else {
        pr->mark_evicted();
        server_->lru_touch(pr);
    }
}

tamed void Table::fetch_remote(String first, String last, int32_t owner,
                               tamer::event<> done) {
    tvars {
        RemoteRange* rr = new RemoteRange(this, first, last, owner);
        Interconnect::scan_result res;
    }

    rr->add_waiting(done);

    for (Table* t = parent_; t; t = t->parent_)
        ++t->nsubtables_with_ranges_.remote;
    remote_ranges_.insert(*rr);

    // std::cerr << "fetching remote data: " << rr->interval() << std::endl;
    twait {
        server_->interconnect(owner)->subscribe(first, last, server_->me(),
                                                make_event(res));
    }

    // std::cerr << "remote data fetch: " << rr->interval() << " returned "
    //          << res.size() << " results" << std::endl;

    for (auto it = res.begin(); it != res.end(); ++it)
        server_->make_table_for(it->key()).insert(it->key(), it->value());

    server_->lru_touch(rr);
    rr->notify_waiting();
}

void Table::evict_remote(RemoteRange* rr) {
    assert(!rr->pending());
    assert(!nsubtables_with_ranges_.remote);

    uint32_t kept = 0;
    Table* t = this;

    retry:
    for (auto sit = t->source_ranges_.begin_overlaps(rr->ibegin(), rr->iend());
            sit != t->source_ranges_.end(); ++sit)
        if (sit->purge(*server_))
            ++kept;

    if ((t = t->parent_) && t->triecut_)
        goto retry;

    nevict_remote_ += erase_purge(rr->ibegin(), rr->iend());

    //std::cerr << "evicting remote range " << rr->interval()
    //          << ", keeping " << kept << " source ranges in place " << std::endl;

    if (!kept) {
        server_->interconnect(rr->owner())->unsubscribe(rr->ibegin(), rr->iend(),
                                                        server_->me(), tamer::event<>());

        for (Table* t = parent_; t; t = t->parent_)
            --t->nsubtables_with_ranges_.remote;

        remote_ranges_.erase(*rr);
        delete rr;
    }
    else {
        rr->mark_evicted();
        server_->lru_touch(rr);
    }
}

void Table::invalidate_remote(Str first, Str last) {
    local_vector<RemoteRange*, 4> ranges;
    collect_ranges(first, last, ranges,
                   &Table::remote_ranges_, &Table::swr::remote);

    for (auto r = ranges.begin(); r != ranges.end(); ++r) {
        RemoteRange* rr = *r;
        Table* rrt = rr->table();

        if (rr->pending())
            continue;

        //std::cerr << "invalidating remote range " << rr->interval() << std::endl;
        rrt->remote_ranges_.erase(*rr);
        rrt->invalidate_dependents(rr->ibegin(), rr->iend());

        for (Table* t = rrt->parent_; t; t = t->parent_)
            --t->nsubtables_with_ranges_.remote;

        auto it = rrt->lower_bound(rr->ibegin());
        auto itend = rrt->lower_bound(rr->iend());
        while(it != itend)
            it = erase_invalid(it);

        delete rr;
    }
}

void Table::evict_sink(SinkRange* sr) {
    assert(!sr->evicted());

    //todo: purge sources and keep sinkrange

    //std::cerr << "evicting sink range " << sink->interval() << std::endl;

    uint64_t before = Sink::invalidate_hit_keys;

    sink_ranges_.erase(*sr);
    delete sr; // sinks invalidated within

    nevict_sink_ += (Sink::invalidate_hit_keys - before);
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


Server::Server()
    : persistent_store_(nullptr), writethrough_(false),
      supertable_(Str(), nullptr, this),
      last_validate_at_(0), validate_time_(0), insert_time_(0), evict_time_(0),
      part_(nullptr), me_(-1),
      prob_rng_(0,1), evict_lo_(0), evict_hi_(0), evict_scale_(0) {

    gettimeofday(&start_tv_, NULL);
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

tamed void Server::insert(Str key, const String& value, tamer::event<> done) {
    tvars {
        struct timeval tv[2];
    }

    gettimeofday(&tv[0], NULL);
    twait { make_table_for(key).insert(key, value, make_event()); }
    gettimeofday(&tv[1], NULL);
    insert_time_ += to_real(tv[1] - tv[0]);

    maybe_evict();
    done();
}

tamed void Server::erase(Str key, tamer::event<> done) {
    twait { table_for(key).erase(key, done); }
}

tamed void Server::validate(Str key, tamer::event<Table::iterator> done) {
    tvars {
        struct timeval tv[2];
        uint32_t log = 0;
        uint64_t difft = 0;
        std::pair<bool, Table::iterator> it;
        tamer::gather_rendezvous gr;
        Table* t = &this->make_table_for(key);
    }

    do {
        twait(gr);
        gettimeofday(&tv[0], NULL);
        it = t->validate(key, next_validate_at(), log, gr);
        gettimeofday(&tv[1], NULL);
        difft += tv2us(tv[1] - tv[0]);
        assert(gr.has_waiting() == !it.first);
    } while (gr.has_waiting());

    validate_time_ += fromus(difft);
    if (enable_validation_logging)
        validate_log_.emplace_back(difft, log);

    maybe_evict();
    done(it.second);
}

tamed void Server::validate(Str first, Str last, tamer::event<Table::iterator> done) {
    tvars {
        struct timeval tv[2];
        uint32_t log = 0;
        uint64_t difft = 0;
        std::pair<bool, Table::iterator> it;
        tamer::gather_rendezvous gr;
        Table* t = &this->make_table_for(first, last);
    }

    //std::cerr << "VALIDATING: [" << first << ", " << last << ")" << std::endl;
    do {
        twait(gr);
        gettimeofday(&tv[0], NULL);
        it = t->validate(first, last, next_validate_at(), log, gr);
        gettimeofday(&tv[1], NULL);
        difft += tv2us(tv[1] - tv[0]);
        it = t->validate(first, last, next_validate_at(), log, gr);
        assert(gr.has_waiting() == !it.first);
    } while (gr.has_waiting());

    validate_time_ += fromus(difft);
    if (enable_validation_logging)
        validate_log_.emplace_back(difft, log);

    maybe_evict();
    done(it.second);
}

void Table::add_stats(Json& j) {
    j["ninsert"] += ninsert_;
    j["nmodify"] += nmodify_;
    j["nmodify_nohint"] += nmodify_nohint_;
    j["nerase"] += nerase_;
    j["store_size"] += store_.size();
    j["source_ranges_size"] += source_ranges_.size();
    j["sink_ranges_size"] += sink_ranges_.size();
    j["remote_ranges_size"] += remote_ranges_.size();
    j["persisted_ranges_size"] += persisted_ranges_.size();
    j["nvalidate"] += nvalidate_;
    j["nevict_persistent"] += nevict_persistent_;
    j["nevict_remote"] += nevict_remote_;
    j["nevict_sink"] += nevict_sink_;
    j["nevict_range_reload"] += nevict_range_reload_;

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
           sink_ranges_size = 0, remote_ranges_size = 0, persisted_ranges_size = 0;
    struct rusage ru;
    struct timeval tv;

    getrusage(RUSAGE_SELF, &ru);
    gettimeofday(&tv, NULL);

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
        persisted_ranges_size += j["persisted_ranges_size"].to_i();
    }

    double wall_time = to_real(tv - start_tv_);

    Json answer;
    answer.set("store_size", store_size)
	.set("source_ranges_size", source_ranges_size)
	.set("join_ranges_size", join_ranges_size)
	.set("valid_ranges_size", sink_ranges_size)
        .set("server_max_rss_mb", maxrss_mb(ru.ru_maxrss))
        .set("server_user_time", to_real(ru.ru_utime))
        .set("server_system_time", to_real(ru.ru_stime))
        .set("server_wall_time", wall_time)
        .set("server_wall_time_insert", insert_time_)
        .set("server_wall_time_validate", validate_time_)
        .set("server_wall_time_evict", evict_time_)
        .set("server_wall_time_other", wall_time - insert_time_ - validate_time_ - evict_time_);

    if (enable_validation_logging) {
        uint32_t nclear = 0, ncompute = 0, nupdate = 0,
                 nrestart = 0, nremote = 0, npersisted = 0;

        for (auto& v : validate_log_) {
            if (v.is_clear()) {
                ++nclear;
                continue;
            }
            if (v.is_set(ValidateRecord::compute))
                ++ncompute;
            if (v.is_set(ValidateRecord::update))
                ++nupdate;
            if (v.is_set(ValidateRecord::restart))
                ++nrestart;
            if (v.is_set(ValidateRecord::fetch_remote))
                ++nremote;
            if (v.is_set(ValidateRecord::fetch_persisted))
                ++npersisted;
        }

        answer.set("server_validate_nclear", nclear)
              .set("server_validate_ncompute", ncompute)
              .set("server_validate_nupdate", nupdate)
              .set("server_validate_nrestart", nrestart)
              .set("server_validate_nfetch_remote", nremote)
              .set("server_validate_nfetch_persisted", npersisted);
    }

    if (SourceRange::allocated_key_bytes)
        answer.set("source_allocated_key_bytes", SourceRange::allocated_key_bytes);
    if (ServerRangeBase::allocated_key_bytes)
        answer.set("sink_allocated_key_bytes", ServerRangeBase::allocated_key_bytes);
    if (Sink::invalidate_hit_keys)
        answer.set("invalidate_hits", Sink::invalidate_hit_keys);
    if (Sink::invalidate_miss_keys)
        answer.set("invalidate_misses", Sink::invalidate_miss_keys);
    return answer.set("tables", tables);
}

Json Server::logs() const {
    return Json();
}

void Server::control(const Json& cmd) {
    if (cmd["quit"])
        exit(0);
    if (cmd["clear_log"])
        validate_log_.clear();
    if (enable_validation_logging && cmd["print_validation_log"]) {
        std::cerr << "VALIDATION LOG:" << std::endl << validate_log_.size() << std::endl;
        for (auto& v : validate_log_)
            std::cerr << v.time() << " "
                      << v.is_clear() << " "
                      << v.is_set(ValidateRecord::compute) << " "
                      << v.is_set(ValidateRecord::update) << " "
                      << v.is_set(ValidateRecord::restart) << " "
                      << v.is_set(ValidateRecord::fetch_remote) << " "
                      << v.is_set(ValidateRecord::fetch_persisted) << " " << std::endl;
    }
    if (cmd["print"])
        print(std::cerr);
    if (cmd["print_table_keys"]) {
        String tname = table_name(cmd["print_table_keys"].as_s());
        assert(tname);
        Table& t = table(tname);
        for (auto it = t.begin(); it != t.end(); ++it)
            std::cerr << it->key() << std::endl;
    }
    if (cmd["flush_db_queue"]) {
        if (persistent_store_)
            persistent_store_->flush();
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
