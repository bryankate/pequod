#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRangeBase::allocated_key_bytes = 0;

JoinRange::JoinRange(Str first, Str last, Join* join)
    : ServerRangeBase(first, last), join_(join) {
}

JoinRange::~JoinRange() {
    while (SinkRange* sink = valid_ranges_.unlink_leftmost_without_rebalance())
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
    va.sink = new SinkRange(first, last, this, now);
    valid_ranges_.insert(va.sink);
    validate_step(va, 0);
}

void JoinRange::validate(Str first, Str last, Server& server, uint64_t now) {
    Str last_valid = first;
    for (auto it = valid_ranges_.begin_overlaps(first, last);
         it != valid_ranges_.end(); ) {
        if (it->has_expired(now)) {
            SinkRange* vjr = it.operator->();
            ++it;
            valid_ranges_.erase(vjr);
            vjr->flush();
            vjr->deref();
            continue;
        }
        if (last_valid < it->ibegin())
            validate_one(last_valid, it->ibegin(), server, now);
        if (it->need_update())
            it->update(first, last, server, now);
        last_valid = it->iend();
        ++it;
    }
    if (last_valid < last)
        validate_one(last_valid, last, server, now);
}

void JoinRange::validate_step(validate_args& va, int joinpos) {
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
    if (joinpos + 1 == join_->nsource()) {
        r = join_->make_source(*va.server, va.match,
                               Str(kf, kflen), Str(kl, kllen));
        r->set_sink(va.sink);
    }

    auto it = va.server->lower_bound(Str(kf, kflen));
    auto ilast = va.server->lower_bound(Str(kl, kllen));

    Match::state mstate(va.match.save());
    const Pattern& pat = join_->source(joinpos);

    for (; it != ilast; ++it) {
	if (it->key().length() != pat.key_length())
            continue;

        // XXX PERFORMANCE can prob figure out ahead of time whether this
        // match is simple/necessary
        if (pat.match(it->key(), va.match)) {
            if (r)
                r->notify(it.operator->(), String(), va.notifier);
            else
                validate_step(va, joinpos + 1);
        }

        va.match.restore(mstate);
    }

    if (join_->maintained() && va.notifier == SourceRange::notify_erase) {
        if (r)
            // XXX want to remove just the right one
            va.server->remove_source(r->ibegin(), r->iend(), va.sink);
        delete r;
    } else if (join_->maintained()) {
        if (r)
            va.server->add_source(r);
        else
            va.server->add_source(new InvalidatorRange
                                  (*va.server, join_, joinpos, va.match,
                                   Str(kf, kflen), Str(kl, kllen), va.sink));
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

IntermediateUpdate::IntermediateUpdate(Str first, Str last,
                                       Str context, Str key,
                                       int joinpos, int notifier)
    : ServerRangeBase(first, last), context_(context), key_(key),
      joinpos_(joinpos), notifier_(notifier) {
}

SinkRange::~SinkRange() {
    while (IntermediateUpdate* iu = updates_.unlink_leftmost_without_rebalance())
        delete iu;
    if (hint_)
        hint_->deref();
}

void SinkRange::add_update(int joinpos, const String& context,
                                Str key, int notifier) {
    Match m;
    join()->source(joinpos).match(key, m);
    join()->parse_match_context(context, joinpos, m);
    uint8_t kf[key_capacity], kl[key_capacity];
    int kflen = join()->expand_first(kf, join()->sink(), ibegin(), iend(), m);
    int kllen = join()->expand_last(kl, join()->sink(), ibegin(), iend(), m);

    IntermediateUpdate* iu = new IntermediateUpdate(Str(kf, kflen),
                                                    Str(kl, kllen),
                                                    context,
                                                    key, joinpos, notifier);
    updates_.insert(iu);

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
    join->source(iu->joinpos_).match(iu->key_, va.match);
    join->parse_match_context(iu->context_, iu->joinpos_, va.match);
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

void SinkRange::flush() {
    Table* t = table();
    auto endit = t->lower_bound(iend());
    for (auto it = t->lower_bound(ibegin()); it != endit; )
        if (it->owner() == this)
            it = t->erase(it);
        else
            ++it;
}

std::ostream& operator<<(std::ostream& stream, const IntermediateUpdate& iu) {
    stream << "UPDATE{" << iu.interval() << " "
           << (iu.notifier() > 0 ? "+" : "-")
           << iu.key() << " " << iu.context_ << "}";
    return stream;
}

} // namespace pq
