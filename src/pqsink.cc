#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRangeBase::allocated_key_bytes = 0;
uint64_t SinkRange::invalidate_hit_keys = 0;
uint64_t SinkRange::invalidate_miss_keys = 0;

JoinRange::JoinRange(Str first, Str last, Join* join)
    : ServerRangeBase(first, last), join_(join) {
}

JoinRange::~JoinRange() {
    while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
        // XXX no one else had better deref this shit
        delete sink;
}

struct JoinRange::validate_args {
    Str first;
    Str last;
    Match match;
    Server* server;
    SinkRange* sink;
    uint64_t now;
    int notifier;
};

inline void JoinRange::validate_one(Str first, Str last, Server& server,
                                    uint64_t now) {
    validate_args va{first, last, Match(), &server, nullptr, now,
            SourceRange::notify_insert};
    join_->sink().match_range(first, last, va.match);
    va.sink = new SinkRange(this, first, last, va.match, now);
    valid_ranges_.insert(va.sink);
    validate_step(va, 0);
}

void JoinRange::validate(Str first, Str last, Server& server, uint64_t now) {
    Str last_valid = first;
    for (auto it = valid_ranges_.begin_overlaps(first, last);
         it != valid_ranges_.end(); ) {
        SinkRange* sink = it.operator->();
        if (sink->has_expired(now)) {
            ++it;
            sink->invalidate();
        } else {
            if (last_valid < sink->ibegin())
                validate_one(last_valid, sink->ibegin(), server, now);
            if (sink->need_update())
                sink->update(first, last, server, now);
            last_valid = sink->iend();
            ++it;
        }
    }
    if (last_valid < last)
        validate_one(last_valid, last, server, now);
}

void JoinRange::validate_step(validate_args& va, int joinpos) {
    ++join_->source_table(joinpos)->nvalidate_;

    uint8_t kf[128], kl[128];
    int kflen = join_->expand_first(kf, join_->source(joinpos),
                                    va.first, va.last, va.match);
    int kllen = join_->expand_last(kl, join_->source(joinpos),
                                   va.first, va.last, va.match);
    assert(Str(kf, kflen) <= Str(kl, kllen));

    // need to validate the source ranges in case they have not been
    // expanded yet.
    // XXX PERFORMANCE this is not always necessary
    // XXX For now don't do this if the join is recursive
    if (table_name(Str(kf, kflen)) != join_->sink().table_name())
        join_->source_table(joinpos)->validate(Str(kf, kflen), Str(kl, kllen),
                                               va.now);

    SourceRange* r = 0;
    if (joinpos + 1 == join_->nsource())
        r = join_->make_source(*va.server, va.match,
                               Str(kf, kflen), Str(kl, kllen), va.sink);

    auto it = va.server->lower_bound(Str(kf, kflen));
    auto ilast = va.server->lower_bound(Str(kl, kllen));

    Match::state mstate(va.match.save());
    const Pattern& pat = join_->source(joinpos);

    // figure out whether match is necessary
    int mopt = pat.check_optimized_match(va.match);

    if (mopt < 0) {
        // match not optimizable
        for (; it != ilast; ++it)
            if (it->key().length() == pat.key_length()) {
                if (pat.match(it->key(), va.match)) {
                    if (r)
                        r->notify(it.operator->(), String(), va.notifier);
                    else
                        validate_step(va, joinpos + 1);
                }
                va.match.restore(mstate);
            }
    } else {
        // match optimizable
        ++join_->source_table(joinpos)->nvalidate_optimized_;
        for (; it != ilast; ++it)
            if (it->key().length() == pat.key_length()) {
                pat.assign_optimized_match(it->key(), mopt, va.match);
                if (r)
                    r->notify(it.operator->(), String(), va.notifier);
                else
                    validate_step(va, joinpos + 1);
                va.match.restore(mstate);
            }
    }

    if (join_->maintained() && va.notifier == SourceRange::notify_erase) {
        if (r) {
            LocalStr<12> remove_context;
            join_->make_context(remove_context, va.match, join_->context_mask(joinpos) & ~va.sink->context_mask());
            va.server->remove_source(r->ibegin(), r->iend(), va.sink, remove_context);
            delete r;
        }
    } else if (join_->maintained()) {
        if (r)
            va.server->add_source(r);
        else {
            SourceRange::parameters p{*va.server, join_, joinpos, va.match,
                    Str(kf, kflen), Str(kl, kllen), va.sink};
            va.server->add_source(new InvalidatorRange(p));
        }
    } else if (r)
        delete r;
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

void JoinRange::pull_flush() {
    mandatory_assert(valid_ranges_.size() <= 1);
    while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
        // XXX no one else had better deref this shit
        delete sink;
}

IntermediateUpdate::IntermediateUpdate(Str first, Str last,
                                       SinkRange* sink, int joinpos, const Match& m,
                                       int notifier)
    : ServerRangeBase(first, last), joinpos_(joinpos), notifier_(notifier) {
    Join* j = sink->join();
    unsigned context_mask = (j->context_mask(joinpos) | j->source_mask(joinpos)) & ~sink->context_mask();
    j->make_context(context_, m, context_mask);
}

SinkRange::SinkRange(JoinRange* jr, Str first, Str last, const Match& m, uint64_t now)
    : ServerRangeBase(first, last), jr_(jr), refcount_(0), hint_{nullptr},
      data_free_(uintptr_t(-1)) {
    Join* j = jr_->join();
    if (j->maintained())
        expires_at_ = 0;
    else
        expires_at_ = now + j->staleness();

    context_mask_ = j->known_mask(m);
    j->make_context(context_, m, context_mask_);
}

SinkRange::~SinkRange() {
    while (IntermediateUpdate* iu = updates_.unlink_leftmost_without_rebalance())
        delete iu;
    if (hint_)
        hint_->deref();
}

void SinkRange::add_update(int joinpos, Str context, Str key, int notifier) {
    Match m;
    join()->source(joinpos).match(key, m);
    join()->assign_context(m, context);
    uint8_t kf[key_capacity], kl[key_capacity];
    int kflen = join()->expand_first(kf, join()->sink(), ibegin(), iend(), m);
    int kllen = join()->expand_last(kl, join()->sink(), ibegin(), iend(), m);

    IntermediateUpdate* iu = new IntermediateUpdate
        (Str(kf, kflen), Str(kl, kllen), this, joinpos, m, notifier);
    updates_.insert(iu);

    join()->sink_table()->invalidate_dependents(Str(kf, kflen), Str(kl, kllen));
    // std::cerr << *iu << "\n";
}

bool SinkRange::update_iu(Str first, Str last, IntermediateUpdate* iu,
                          Server& server, uint64_t now) {
    if (first < iu->ibegin())
        first = iu->ibegin();
    if (iu->iend() < last)
        last = iu->iend();
    if (first != iu->ibegin() && last != iu->iend())
        // XXX embiggening range
        last = iu->iend();

    Join* join = jr_->join();
    JoinRange::validate_args va{first, last, Match(), &server, this, now,
            iu->notifier_};
    join->assign_context(va.match, context_);
    join->assign_context(va.match, iu->context_);
    jr_->validate_step(va, iu->joinpos_ + 1);

    if (first == iu->ibegin())
        iu->ibegin_ = last;
    if (last == iu->iend())
        iu->iend_ = first;
    return iu->ibegin_ < iu->iend_;
}

void SinkRange::update(Str first, Str last, Server& server, uint64_t now) {
    for (auto it = updates_.begin_overlaps(first, last);
         it != updates_.end(); ) {
        IntermediateUpdate* iu = it.operator->();
        ++it;

        updates_.erase(iu);
        if (update_iu(first, last, iu, server, now))
            updates_.insert(iu);
        else
            delete iu;
    }
}

void SinkRange::invalidate() {
    if (valid()) {
        jr_->valid_ranges_.erase(this);

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

        ibegin_ = Str();
        if (refcount_ == 0)
            delete this;
    }
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
