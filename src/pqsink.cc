#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRangeBase::allocated_key_bytes = 0;
uint64_t Sink::invalidate_hit_keys = 0;
uint64_t Sink::invalidate_miss_keys = 0;

Loadable::Loadable(Table* table) : table_(table) {
}

Loadable::~Loadable() {
}

Evictable::Evictable() : evicted_(false), last_access_(0) {
}

Evictable::~Evictable() {
}

uint32_t Evictable::priority() const {
    return pri_none;
}

void Evictable::unlink() {
    lru_hook::unlink();
}

bool Evictable::is_linked() const {
    return lru_hook::is_linked();
}

JoinRange::JoinRange(Str first, Str last, Join* join)
    : ServerRangeBase(first, last), join_(join) {
}

SinkRange::SinkRange(Str first, Str last, Table* table)
    : ServerRangeBase(first, last), table_(table) {
}

SinkRange::~SinkRange() {
    for (auto it = sinks_.begin(); it != sinks_.end(); ++it) {
        (*it)->invalidate();
        (*it)->deref();
    }
}

struct SinkRange::validate_args {
    RangeMatch rm;
    Server* server;
    Sink* sink;
    uint64_t now;
    int notifier;
    int filters;
    Match::state filtermatch;
    Table* sourcet[source_capacity];
    uint32_t& log;
    tamer::gather_rendezvous& pending;
    bool complete;

    validate_args(Str first, Str last, Server& server_, uint64_t now_,
                  Sink* sink_, int notifier_,
                  uint32_t& log_, tamer::gather_rendezvous& gr_)
        : rm(first, last), server(&server_), sink(sink_),
          now(now_), notifier(notifier_), filters(0),
          log(log_), pending(gr_), complete(true) {
    }
};

bool SinkRange::add_sink(JoinRange* jr, Server& server,
                         uint64_t now, uint32_t& log,
                         tamer::gather_rendezvous& gr) {

    //std::cerr << "add_sink " << first << ", " << last << "\n";
    Sink* sink = new Sink(jr, this);
    sinks_.push_back(sink);
    sink->ref();

    return validate_one(ibegin(), iend(), sink, server, now, log, gr);
}

inline bool SinkRange::validate_one(Str first, Str last,
                                    Sink* sink, Server& server,
                                    uint64_t now, uint32_t& log,
                                    tamer::gather_rendezvous& gr) {

    //std::cerr << "validate_one [" << first << ", " << last << ")\n";
    validate_args va(first, last, server, now,
                     nullptr, SourceRange::notify_insert, log, gr);

    sink->join()->sink().match_range(va.rm);
    va.sink = sink;
    sink->set_expiration(now);

    log |= ValidateRecord::compute;
    return validate_step(va, 0);
}

bool SinkRange::validate(Str first, Str last, Server& server,
                         uint64_t now, uint32_t& log,
                         tamer::gather_rendezvous& gr) {

    bool complete = true;

    for (auto sit = sinks_.begin(); sit != sinks_.end(); ++sit) {
        Sink* sink = *sit;
        sink->set_validating(true);

        if (unlikely(sink->has_expired(now))) {
            assert(!sink->join()->maintained());
            sink->clear_updates();
            sink->add_invalidate(ibegin(), iend());
            sink->set_expiration(now);
            sink->set_valid();
        }
        else if (!sink->valid()) {
            sink->add_invalidate(ibegin(), iend());
            sink->set_valid();
        }

        if (sink->need_restart())
            complete &= sink->restart(first, last, server, now, log, gr);
        if (!sink->need_restart() && sink->need_update())
            complete &= sink->update(first, last, server, now, log, gr);

        sink->set_validating(false);
    }

    if (complete)
        server.lru_touch(this);

    return complete;
}

bool SinkRange::validate_filters(validate_args& va) {
    bool complete = true;
    int filters = va.filters;
    Join* join = va.sink->join();
    Match::state mstate = va.rm.match.save();
    va.rm.match.restore(va.filtermatch);

    for (int jp = 0; filters; ++jp, filters >>= 1)
        if (filters & 1) {
            uint8_t kf[key_capacity], kl[key_capacity];
            int kflen = join->expand_first(kf, join->source(jp), va.rm);
            int kllen = join->expand_last(kl, join->source(jp), va.rm);
            assert(Str(kf, kflen) <= Str(kl, kllen));
            Table* sourcet = &va.server->make_table_for(Str(kf, kflen), Str(kl, kllen));
            va.sourcet[jp] = sourcet;
            complete &= sourcet->validate(Str(kf, kflen), Str(kl, kllen),
                                          va.now, va.log, va.pending).first;
        }

    va.rm.match.restore(mstate);
    return complete;
}

bool SinkRange::validate_step(validate_args& va, int joinpos) {
    Join* join = va.sink->join();
    assert(va.sink->valid());

    if (!join->maintained() && join->source_is_filter(joinpos)) {
        if (!va.filters)
            va.filtermatch = va.rm.match.save();
        int known_at_sink = join->source_mask(join->nsource() - 1)
            | join->known_mask(va.filtermatch);
        if ((join->source_mask(joinpos) & ~known_at_sink) == 0) {
            va.filters |= 1 << joinpos;
            bool complete = validate_step(va, joinpos + 1);
            va.filters &= ~(1 << joinpos);
            return complete;
        }
    }

    uint8_t kf[key_capacity], kl[key_capacity];
    int kflen = join->expand_first(kf, join->source(joinpos), va.rm);
    int kllen = join->expand_last(kl, join->source(joinpos), va.rm);
    assert(Str(kf, kflen) <= Str(kl, kllen));
    Table* sourcet = &va.server->make_table_for(Str(kf, kflen), Str(kl, kllen));
    va.sourcet[joinpos] = sourcet;
    //std::cerr << "examine " << Str(kf, kflen) << ", " << Str(kl, kllen) << "\n";

    // need to validate the source ranges in case they have not been
    // expanded yet or they are missing.
    std::pair<bool, Table::iterator> srcval =
            sourcet->validate(Str(kf, kflen), Str(kl, kllen),
                              va.now, va.log, va.pending);

    if (!srcval.first) {
        va.sink->add_restart(joinpos, va.rm.match, va.notifier);
        return false;
    }

    SourceRange* r = 0;
    if (joinpos + 1 == join->nsource())
        r = join->make_source(*va.server, va.rm.match,
                              Str(kf, kflen), Str(kl, kllen), va.sink);

    bool complete = true;
    auto it = srcval.second;
    auto itend = it.table_end();
    if (it != itend) {
        Match::state mstate(va.rm.match.save());
        const Pattern& pat = join->source(joinpos);
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
                                int filterlen = join->source(jp).expand(filterstr, va.rm.match);
                                //std::cerr << "validate " << Str(filterstr, filterlen) << "\n";
                                if (!va.sourcet[jp]->count(Str(filterstr, filterlen)))
                                    goto give_up;
                            }
                        r->notify(it.operator->(), String(), va.notifier);
                    }
                give_up:
                    va.rm.match.restore(mstate);
                }
        } else if (join->maintained() || (!join->maintained() && va.complete)) {
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

    if (join->maintained() && va.notifier == SourceRange::notify_erase) {
        assert(complete);
        if (r) {
            LocalStr<12> remove_context;
            join->make_context(remove_context, va.rm.match, join->context_mask(joinpos) & ~va.sink->context_mask());
            sourcet->remove_source(r->ibegin(), r->iend(), va.sink, remove_context);
            delete r;
        }
    } else if (join->maintained()) {
        if (r && complete)
            sourcet->add_source(r);
        else if (!r) {
            SourceRange::parameters p{*va.server, join, joinpos, va.rm.match,
                    Str(kf, kflen), Str(kl, kllen), va.sink};
            sourcet->add_source(new UsingRange(p));
        }
    } else if (r)
        delete r;

    return complete;
}

void SinkRange::evict() {
    assert(table_);
    table_->evict_sink(this);
}

uint32_t SinkRange::priority() const {
    return pri_sink;
}

IntermediateUpdate::IntermediateUpdate(Str first, Str last,
                                       Sink* sink, int joinpos, const Match& m,
                                       int notifier)
    : ServerRangeBase(first, last), joinpos_(joinpos), notifier_(notifier) {
    if (joinpos_ >= 0) {
        Join* j = sink->join();
        unsigned context_mask = (j->context_mask(joinpos_) | j->source_mask(joinpos_)) & ~sink->context_mask();
        j->make_context(context_, m, context_mask);
    }
}

Restart::Restart(Sink* sink, int joinpos, const Match& m, int notifier)
    : joinpos_(joinpos), notifier_(notifier) {
    sink->join()->make_context(context_, m, sink->join()->known_mask(m));
}

Sink::Sink(JoinRange* jr, SinkRange* sr)
    : valid_(true), validating_(false),
      table_(sr->table_), hint_{nullptr}, dangerous_slot_(0),
      expires_at_(0), refcount_(0), data_free_(uintptr_t(-1)),
      jr_(jr), sr_(sr) {

    Join* j = jr_->join();

    if (j) {
        RangeMatch rm(ibegin(), iend());
        j->sink().match_range(rm);
        context_mask_ = j->known_mask(rm.match);
        j->make_context(context_, rm.match, context_mask_);
        dangerous_slot_ = rm.dangerous_slot;

        // if (dangerous_slot_ >= 0)
        //     std::cerr << rm.first << " " << rm.last <<  " " << dangerous_slot_ << "\n";
    }
}

Sink::~Sink() {
    clear_updates();
    if (hint_)
        hint_->deref();
}

void Sink::add_update(int joinpos, Str context, Str key, int notifier) {
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

void Sink::add_restart(int joinpos, const Match& m, int notifier) {
    //std::cerr << "adding restart with match " << m << std::endl;
    restarts_.push_back(new Restart(this, joinpos, m, notifier));
}

void Sink::add_invalidate(Str key) {
    uint8_t next_key[key_capacity + 1];
    memcpy(next_key, key.data(), key.length());
    next_key[key.length()] = 0;

    add_invalidate(key, Str(next_key, key.length() + 1));
}

void Sink::add_invalidate(Str first, Str last) {
    IntermediateUpdate* iu = new IntermediateUpdate
            (first, last, this, -1, Match(), SourceRange::notify_insert);
    updates_.insert(*iu);

    if (valid()) {
        table_->invalidate_dependents(first, last);
        auto endit = table_->lower_bound(last);
        for (auto it = table_->lower_bound(first); it != endit; )
            if (it->owner() == this)
                it = table_->erase_invalid(it);
            else
                ++it;
    }
    //std::cerr << *iu << "\n";
}

bool Sink::update_iu(Str first, Str last, IntermediateUpdate* iu, bool& remaining,
                          Server& server, uint64_t now, uint32_t& log,
                          tamer::gather_rendezvous& gr) {
    assert(valid());
    LocalStr<24> f = first, l = last;

    if (f < iu->ibegin())
        f = iu->ibegin();
    if (iu->iend() < l)
        l = iu->iend();
    if (f != iu->ibegin() && l != iu->iend())
        // XXX embiggening range
        l = iu->iend();

    Join* join = jr_->join();
    SinkRange::validate_args va(f, l, server, now, this, iu->notifier_, log, gr);
    join->assign_context(va.rm.match, context_);
    join->assign_context(va.rm.match, iu->context_);

    //std::cerr << "UPDATE: [" << f << ", " << l << ")" << std::endl;
    if (!sr_->validate_step(va, iu->joinpos_ + 1))
        return false;

    if (f == iu->ibegin())
        iu->ibegin_ = l;
    if (l == iu->iend())
        iu->iend_ = f;

    remaining = iu->ibegin_ < iu->iend_;
    return true;
}

bool Sink::update(Str first, Str last, Server& server,
                       uint64_t now, uint32_t& log, tamer::gather_rendezvous& gr) {
    assert(valid());

    for (auto it = updates_.begin_overlaps(first, last); it != updates_.end(); ) {
        log |= ValidateRecord::update;

        IntermediateUpdate* iu = it.operator->();
        ++it;

        updates_.erase(*iu);
        bool remaining = false;
        if (!update_iu(first, last, iu, remaining, server, now, log, gr))
            return false;
        if (remaining)
            updates_.insert(*iu);
        else
            delete iu;
    }
    return true;
}

bool Sink::restart(Str first, Str last, Server& server,
                        uint64_t now, uint32_t& log, tamer::gather_rendezvous& gr) {
    log |= ValidateRecord::restart;

    bool complete = true;
    int32_t nrestart = restarts_.size();
    Join* join = jr_->join();

    for (int32_t i = 0; i < nrestart; ++i) {
        Restart* r = restarts_.front();
        restarts_.pop_front();

        SinkRange::validate_args va(first, last, server, now, this,
                                    r->notifier_, log, gr);
        join->assign_context(va.rm.match, r->context_);
        va.rm.dangerous_slot = dangerous_slot_;

        //std::cerr << "RESTART: [" << va.rm.first << ", " << va.rm.last
        //          << ") match: " << va.rm.match << std::endl;
        complete &= sr_->validate_step(va, r->joinpos_);
        delete r;
    }

    return complete;
}

void Sink::invalidate() {
    if (valid() && !validating_) {
        while (data_free_ != uintptr_t(-1)) {
            uintptr_t pos = data_free_;
            data_free_ = (uintptr_t) data_[pos];
            data_[pos] = 0;
        }

        if (hint_) {
            hint_->deref();
            hint_ = nullptr;
        }

        Table* t = table();
        for (auto d : data_)
            if (d) {
                t->invalidate_erase(d);
                ++invalidate_hit_keys;
            }

        data_.clear();
        data_free_ = uintptr_t(-1);

        clear_updates();
        valid_ = false;

        if (refcount_ == 0)
            delete this;
    }
}

PersistedRange::PersistedRange(Table* table, Str first, Str last)
    : ServerRangeBase(first, last), Loadable(table) {
}

void PersistedRange::evict() {
    assert(table_);
    table_->evict_persisted(this);
}

uint32_t PersistedRange::priority() const {
    return pri_persistent;
}

RemoteRange::RemoteRange(Table* table, Str first, Str last, int32_t owner)
    : ServerRangeBase(first, last), Loadable(table), owner_(owner) {
}

void RemoteRange::evict() {
    assert(table_);
    table_->evict_remote(this);
}

uint32_t RemoteRange::priority() const {
    return pri_remote;
}

RemoteSink::RemoteSink(Interconnect* conn, uint32_t peer)
    : Sink(new JoinRange("", "}", nullptr), new SinkRange("", "}", nullptr)),
      conn_(conn), peer_(peer) {

    ref(); // avoid auto destruction
}

RemoteSink::~RemoteSink() {
    delete jr_;
    delete sr_;
}

std::ostream& operator<<(std::ostream& stream, const IntermediateUpdate& iu) {
    stream << "UPDATE{" << iu.interval() << " "
           << (iu.notifier() > 0 ? "+" : "-")
           << iu.context() << " " << iu.context_ << "}";
    return stream;
}

std::ostream& operator<<(std::ostream& stream, const Sink& sink) {
    if (sink.valid())
        stream << "SINK{" << sink.interval().unparse().printable() << "}";
    else
        stream << "SINK{INVALID}";
    return stream;
}

} // namespace pq
