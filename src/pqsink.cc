#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRangeBase::allocated_key_bytes = 0;
uint64_t SinkRange::invalidate_hit_keys = 0;
uint64_t SinkRange::invalidate_miss_keys = 0;

Evictable::Evictable() : last_access_(0) {
}

Evictable::~Evictable() {
}

JoinRange::JoinRange(Str first, Str last, Join* join)
    : ServerRangeBase(first, last), join_(join), flush_at_(0) {
}

JoinRange::~JoinRange() {
    while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
        // XXX no one else had better deref this shit
        delete sink;
}

struct JoinRange::validate_args {
    RangeMatch rm;
    Server* server;
    SinkRange* sink;
    uint64_t now;
    int notifier;
    int filters;
    Match::state filtermatch;
    Table* sourcet[source_capacity];
    tamer::gather_rendezvous& pending;
    bool complete;

    validate_args(Str first, Str last, Server& server_, uint64_t now_,
                  SinkRange* sink_, int notifier_, tamer::gather_rendezvous& gr_)
        : rm(first, last), server(&server_), sink(sink_),
          now(now_), notifier(notifier_), filters(0),
          pending(gr_), complete(true) {
    }
};

inline bool JoinRange::validate_one(Str first, Str last, Server& server,
                                    uint64_t now, tamer::gather_rendezvous& gr) {
    validate_args va(first, last, server, now, nullptr, SourceRange::notify_insert, gr);
    join_->sink().match_range(va.rm);
    va.sink = new SinkRange(this, va.rm, now);
    //std::cerr << "validate_one " << first << ", " << last << " " << *join_ << "\n";
    valid_ranges_.insert(*va.sink);

    if (join_->maintained())
        server.lru_add(va.sink);

    return validate_step(va, 0);
}

bool JoinRange::validate(Str first, Str last, Server& server,
                         uint64_t now, tamer::gather_rendezvous& gr) {

    if (!join_->maintained() && !join_->staleness() && flush_at_ != now) {
        Table& t = server.table_for(first, last);
        if (t.flush_for_pull(now)) {
            while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
                delete sink;        // XXX be careful of refcounting
        } else {
            while (!valid_ranges_.empty())
                valid_ranges_.begin()->invalidate();
        }
        flush_at_ = now;
    }

    bool complete = true;
    Str last_valid = first;
    for (auto it = valid_ranges_.begin_overlaps(first, last);
         it != valid_ranges_.end(); ) {
        SinkRange* sink = it.operator->();
        if (sink->has_expired(now)) {
            ++it;
            sink->invalidate();
        } else {
            if (last_valid < sink->ibegin())
                complete &= validate_one(last_valid, sink->ibegin(), server, now, gr);
            if (sink->need_restart())
                complete &= sink->restart(first, last, server, now, gr);
            if (!sink->need_restart() && sink->need_update())
                complete &= sink->update(first, last, server, now, gr);

            if (join_->maintained())
                server.lru_touch(sink);
            last_valid = sink->iend();
            ++it;
        }
    }
    if (last_valid < last)
        complete &= validate_one(last_valid, last, server, now, gr);

    return complete;
}

bool JoinRange::validate_filters(validate_args& va) {
    bool complete = true;
    int filters = va.filters;
    Match::state mstate = va.rm.match.save();
    va.rm.match.restore(va.filtermatch);

    for (int jp = 0; filters; ++jp, filters >>= 1)
        if (filters & 1) {
            uint8_t kf[key_capacity], kl[key_capacity];
            int kflen = join_->expand_first(kf, join_->source(jp), va.rm);
            int kllen = join_->expand_last(kl, join_->source(jp), va.rm);
            assert(Str(kf, kflen) <= Str(kl, kllen));
            Table* sourcet = &va.server->make_table_for(Str(kf, kflen), Str(kl, kllen));
            va.sourcet[jp] = sourcet;
            complete &= sourcet->validate(Str(kf, kflen), Str(kl, kllen), va.now, va.pending).first;
        }

    va.rm.match.restore(mstate);
    return complete;
}

bool JoinRange::validate_step(validate_args& va, int joinpos) {
    if (!join_->maintained() && join_->source_is_filter(joinpos)) {
        if (!va.filters)
            va.filtermatch = va.rm.match.save();
        int known_at_sink = join_->source_mask(join_->nsource() - 1)
            | join_->known_mask(va.filtermatch);
        if ((join_->source_mask(joinpos) & ~known_at_sink) == 0) {
            va.filters |= 1 << joinpos;
            bool complete = validate_step(va, joinpos + 1);
            va.filters &= ~(1 << joinpos);
            return complete;
        }
    }

    uint8_t kf[key_capacity], kl[key_capacity];
    int kflen = join_->expand_first(kf, join_->source(joinpos), va.rm);
    int kllen = join_->expand_last(kl, join_->source(joinpos), va.rm);
    assert(Str(kf, kflen) <= Str(kl, kllen));
    Table* sourcet = &va.server->make_table_for(Str(kf, kflen), Str(kl, kllen));
    va.sourcet[joinpos] = sourcet;
    //std::cerr << "examine " << Str(kf, kflen) << ", " << Str(kl, kllen) << "\n";

    // need to validate the source ranges in case they have not been
    // expanded yet or they are missing.
    std::pair<bool, Table::iterator> srcval =
            sourcet->validate(Str(kf, kflen), Str(kl, kllen), va.now, va.pending);

    if (!srcval.first) {
        va.sink->add_restart(joinpos, va.rm.match, va.notifier);
        return false;
    }

    SourceRange* r = 0;
    if (joinpos + 1 == join_->nsource())
        r = join_->make_source(*va.server, va.rm.match,
                               Str(kf, kflen), Str(kl, kllen), va.sink);

    bool complete = true;
    auto it = srcval.second;
    auto itend = sourcet->end();
    if (it != itend) {
        Match::state mstate(va.rm.match.save());
        const Pattern& pat = join_->source(joinpos);
        ++sourcet->nvalidate_;

        // match not optimizable
        if (!r) {
            for (; it != itend && it->key() < Str(kl, kllen); ++it)
                if (it->key().length() == pat.key_length()) {
                    //std::cerr << "consider " << *it << "\n";
                    if (pat.match(it->key(), va.rm.match))
                        complete &= validate_step(va, joinpos + 1);
                    va.rm.match.restore(mstate);
                }
        } else if (va.filters) {
            bool filters_validated = false;
            uint8_t filterstr[key_capacity];
            for (; it != itend && it->key() < Str(kl, kllen); ++it)
                if (it->key().length() == pat.key_length()) {
                    if (pat.match(it->key(), va.rm.match)) {
                        //std::cerr << "consider match " << *it << "\n";
                        if (!filters_validated) {
                            complete &= validate_filters(va);
                            filters_validated = true;
                        }
                        if (!complete)
                            break;
                        int filters = va.filters;
                        for (int jp = 0; filters; ++jp, filters >>= 1)
                            if (filters & 1) {
                                int filterlen = join_->source(jp).expand(filterstr, va.rm.match);
                                //std::cerr << "validate " << Str(filterstr, filterlen) << "\n";
                                if (!va.sourcet[jp]->count(Str(filterstr, filterlen)))
                                    goto give_up;
                            }
                        r->notify(it.operator->(), String(), va.notifier);
                    }
                give_up:
                    va.rm.match.restore(mstate);
                }
        } else if (join_->maintained() || (!join_->maintained() && va.complete)) {
            for (; it != itend && it->key() < Str(kl, kllen); ++it)
                if (it->key().length() == pat.key_length()) {
                    //std::cerr << "consider " << *it << "\n";
                    if (pat.match(it->key(), va.rm.match))
                        r->notify(it.operator->(), String(), va.notifier);
                    va.rm.match.restore(mstate);
                }
        }

        // track completion outside of each step to avoid work in pull
        // mode when data is already known to be missing
        va.complete &= complete;
    }

    if (join_->maintained() && va.notifier == SourceRange::notify_erase) {
        assert(complete);
        if (r) {
            LocalStr<12> remove_context;
            join_->make_context(remove_context, va.rm.match, join_->context_mask(joinpos) & ~va.sink->context_mask());
            sourcet->remove_source(r->ibegin(), r->iend(), va.sink, remove_context);
            delete r;
        }
    } else if (join_->maintained()) {
        if (r && complete)
            sourcet->add_source(r);
        else if (!r) {
            SourceRange::parameters p{*va.server, join_, joinpos, va.rm.match,
                    Str(kf, kflen), Str(kl, kllen), va.sink};
            sourcet->add_source(new InvalidatorRange(p));
        }
    } else if (r)
        delete r;

    return complete;
}

#if 0
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
#endif

IntermediateUpdate::IntermediateUpdate(Str first, Str last,
                                       SinkRange* sink, int joinpos, const Match& m,
                                       int notifier)
    : ServerRangeBase(first, last), joinpos_(joinpos), notifier_(notifier) {
    if (joinpos_ >= 0) {
        Join* j = sink->join();
        unsigned context_mask = (j->context_mask(joinpos_) | j->source_mask(joinpos_)) & ~sink->context_mask();
        j->make_context(context_, m, context_mask);
    }
}

Restart::Restart(SinkRange* sink, int joinpos, const Match& m, int notifier)
    : joinpos_(joinpos), notifier_(notifier) {
    sink->join()->make_context(context_, m, sink->join()->known_mask(m));
}

SinkRange::SinkRange(JoinRange* jr, const RangeMatch& rm, uint64_t now)
    : ServerRangeBase(rm.first, rm.last), table_(nullptr), hint_{nullptr},
      dangerous_slot_(rm.dangerous_slot), expires_at_(0),
      refcount_(0), data_free_(uintptr_t(-1)), jr_(jr) {

    Join* j = jr_->join();

    if (j) {
        if (!j->maintained())
            expires_at_ = now + j->staleness();

        context_mask_ = j->known_mask(rm.match);
        j->make_context(context_, rm.match, context_mask_);

        table_ = &j->server().make_table_for(rm.first, rm.last);
        // if (dangerous_slot_ >= 0)
        //     std::cerr << rm.first << " " << rm.last <<  " " << dangerous_slot_ << "\n";
    }
}

SinkRange::~SinkRange() {
    while (IntermediateUpdate* iu = updates_.unlink_leftmost_without_rebalance())
        delete iu;
    if (hint_)
        hint_->deref();
}

void SinkRange::add_update(int joinpos, Str context, Str key, int notifier) {
    RangeMatch rm(ibegin(), iend());
    join()->source(joinpos).match(key, rm.match);
    join()->assign_context(rm.match, context);
    rm.dangerous_slot = dangerous_slot_;
    uint8_t kf[key_capacity], kl[key_capacity];
    int kflen = join()->expand_first(kf, join()->sink(), rm);
    int kllen = join()->expand_last(kl, join()->sink(), rm);

    IntermediateUpdate* iu = new IntermediateUpdate
        (Str(kf, kflen), Str(kl, kllen), this, joinpos, rm.match, notifier);
    updates_.insert(*iu);

    table_->invalidate_dependents(Str(kf, kflen), Str(kl, kllen));
    //std::cerr << *iu << "\n";
}

void SinkRange::add_restart(int joinpos, const Match& m, int notifier) {
    //std::cout << "adding restart with match " << m << std::endl;
    restarts_.push_back(new Restart(this, joinpos, m, notifier));
}

void SinkRange::add_invalidate(Str key) {
    uint8_t next_key[key_capacity + 1];
    memcpy(next_key, key.data(), key.length());
    next_key[key.length()] = 0;

    IntermediateUpdate* iu = new IntermediateUpdate
        (key, Str(next_key, key.length() + 1), this, -1, Match(), SourceRange::notify_insert);
    updates_.insert(*iu);

    table_->invalidate_dependents(key, Str(next_key, key.length() + 1));
    auto endit = table_->lower_bound(Str(next_key, key.length() + 1));
    for (auto it = table_->lower_bound(key); it != endit; )
        it = table_->erase_invalid(it);
    //std::cerr << *iu << "\n";
}

bool SinkRange::update_iu(Str first, Str last, IntermediateUpdate* iu, bool& remaining,
                          Server& server, uint64_t now, tamer::gather_rendezvous& gr) {

    LocalStr<24> f = first, l = last;

    if (f < iu->ibegin())
        f = iu->ibegin();
    if (iu->iend() < l)
        l = iu->iend();
    if (f != iu->ibegin() && l != iu->iend())
        // XXX embiggening range
        l = iu->iend();

    Join* join = jr_->join();
    JoinRange::validate_args va(f, l, server, now, this, iu->notifier_, gr);
    join->assign_context(va.rm.match, context_);
    join->assign_context(va.rm.match, iu->context_);
    if (!jr_->validate_step(va, iu->joinpos_ + 1))
        return false;

    if (f == iu->ibegin())
        iu->ibegin_ = l;
    if (l == iu->iend())
        iu->iend_ = f;

    remaining = iu->ibegin_ < iu->iend_;
    return true;
}

bool SinkRange::update(Str first, Str last, Server& server,
                       uint64_t now, tamer::gather_rendezvous& gr) {

    for (auto it = updates_.begin_overlaps(first, last);
         it != updates_.end(); ) {
        IntermediateUpdate* iu = it.operator->();
        ++it;

        updates_.erase(*iu);
        bool remaining = false;
        if (!update_iu(first, last, iu, remaining, server, now, gr))
            return false;
        if (remaining)
            updates_.insert(*iu);
        else
            delete iu;
    }
    return true;
}

bool SinkRange::restart(Str first, Str last, Server& server,
                        uint64_t now, tamer::gather_rendezvous& gr) {

    bool complete = true;
    int32_t nrestart = restarts_.size();
    Join* join = jr_->join();

    for (int32_t i = 0; i < nrestart; ++i) {
        Restart* r = restarts_.front();
        JoinRange::validate_args va(first, last, server, now, this,
                                    r->notifier_, gr);
        join->assign_context(va.rm.match, r->context_);
        va.rm.dangerous_slot = dangerous_slot_;

        //std::cerr << "restarting validation " << va.rm.first << " " << va.rm.last << " " << va.rm.match << std::endl;
        complete &= jr_->validate_step(va, r->joinpos_);
        restarts_.pop_front();
        delete r;
    }

    return complete;
}

void SinkRange::invalidate() {
    if (valid()) {
        jr_->valid_ranges_.erase(*this);

        while (data_free_ != uintptr_t(-1)) {
            uintptr_t pos = data_free_;
            data_free_ = (uintptr_t) data_[pos];
            data_[pos] = 0;
        }

        Table* t = table();
        for (auto d : data_)
            if (d) {
                t->invalidate_erase(d);
                ++invalidate_hit_keys;
            }

        if (join()->maintained())
            join()->server().lru_remove(this);
        ibegin_ = Str();
        table_ = nullptr;
        if (refcount_ == 0)
            delete this;
    }
}

void SinkRange::evict() {
    assert(table_);
    table_->evict_sink(this);
}

RemoteRange::RemoteRange(Table* table, Str first, Str last, int32_t owner)
    : ServerRangeBase(first, last), table_(table), owner_(owner), evicted_(false) {
}

void RemoteRange::evict() {
    assert(table_);
    table_->evict_remote(this);
}

RemoteSink::RemoteSink(Interconnect* conn, uint32_t peer)
    : SinkRange(new JoinRange("", "}", nullptr), RangeMatch("", "}"), 0),
      conn_(conn), peer_(peer) {

    ref(); // avoid auto destruction
}

RemoteSink::~RemoteSink() {
    delete jr_;
}

std::ostream& operator<<(std::ostream& stream, const IntermediateUpdate& iu) {
    stream << "UPDATE{" << iu.interval() << " "
           << (iu.notifier() > 0 ? "+" : "-")
           << iu.context() << " " << iu.context_ << "}";
    return stream;
}

std::ostream& operator<<(std::ostream& stream, const SinkRange& sink) {
    if (sink.valid())
        stream << "SINK{" << sink.interval().unparse().printable() << "}";
    else
        stream << "SINK{INVALID}";
    return stream;
}

} // namespace pq
