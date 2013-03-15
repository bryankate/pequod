#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRange::allocated_key_bytes = 0;

ServerRange::ServerRange(Str first, Str last, range_type type, Join* join)
    : ibegin_(first), iend_(last), type_(type), join_(join), expires_at_(0) {
    if (!ibegin_.is_local())
        allocated_key_bytes += ibegin_.length();
    if (!iend_.is_local())
        allocated_key_bytes += iend_.length();

    if (join_->staleness())
        expires_at_ = tstamp() + tous(join_->staleness());
}

ServerRange::~ServerRange() {
}

struct ServerRange::validate_args {
    Str first;
    Str last;
    Match match;
    Server* server;
    ValidJoinRange* sink;
    int notifier;
};

void ServerRange::validate(Str first, Str last, Server& server) {
    if (type_ == joinsink) {
        validate_args va{first, last, Match(), &server, 0, SourceRange::notify_insert};
        if (join_->maintained() || join_->staleness())
            va.sink = server.add_validjoin(first, last, join_);
        validate(va, 0);
    }
}

void ServerRange::validate(validate_args& va, int joinpos) {
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
        va.server->validate(Str(kf, kflen), Str(kl, kllen));

    SourceRange* r = 0;
    if (joinpos + 1 == join_->nsource())
        r = join_->make_source(*va.server, va.match,
                               Str(kf, kflen), Str(kl, kllen));

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
                validate(va, joinpos + 1);
        }

        va.match.restore(mstate);
    }

    if (join_->maintained() && va.notifier == SourceRange::notify_erase) {
        if (r)
            // XXX want to remove just the right one
            va.server->remove_source(r->ibegin(), r->iend(), va.sink);
        delete r;
    } else if (join_->maintained()) {
        if (r) {
            r->set_sink(va.sink);
            va.server->add_source(r);
        } else
            va.server->add_source(new InvalidatorRange
                                  (*va.server, join_, joinpos, va.match,
                                   Str(kf, kflen), Str(kl, kllen), va.sink));
    } else if (r)
        delete r;
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
            ValidJoinRange* sink = static_cast<ValidJoinRange*>(r_[i]);
            if (sink->expired_at(now) || !sink->valid()) {
                // TODO: remove the expired validjoin from the server
                // (and possibly this set if it will be used after this validation)
                continue;
            }
            // XXX PERFORMANCE can often avoid this check
            if (last_valid < sink->ibegin())
                jr->validate(last_valid, sink->ibegin(), server);
            if (sink->need_update())
                sink->update(first_, last_, server);
            last_valid = sink->iend();
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

IntermediateUpdate::IntermediateUpdate(Str first, Str last,
                                       Str context, Str key,
                                       int joinpos, int notifier)
    : ibegin_(first), iend_(last), context_(context), key_(key),
      joinpos_(joinpos), notifier_(notifier) {
}

void ValidJoinRange::add_update(int joinpos, const String& context,
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

bool ValidJoinRange::update_iu(Str first, Str last, IntermediateUpdate* iu,
                               Server& server) {
    if (first < iu->ibegin())
        first = iu->ibegin();
    if (iu->iend() < last)
        last = iu->iend();
    if (first != iu->ibegin() && last != iu->iend())
        // XXX embiggening range
        last = iu->iend();

    validate_args va{first, last, Match(), &server, this, iu->notifier_};
    join_->source(iu->joinpos_).match(iu->key_, va.match);
    join_->parse_match_context(iu->context_, iu->joinpos_, va.match);
    validate(va, iu->joinpos_ + 1);

    if (first == iu->ibegin())
        iu->ibegin_ = last;
    if (last == iu->iend())
        iu->iend_ = first;
    return iu->ibegin_ < iu->iend_;
}

void ValidJoinRange::update(Str first, Str last, Server& server) {
    for (auto it = updates_.begin_overlaps(first, last);
         it != updates_.end(); ) {
        IntermediateUpdate* iu = it.operator->();
        ++it;

        updates_.erase(iu);
        if (update_iu(first, last, iu, server))
            updates_.insert(iu);
        else
            delete iu;
    }
}

std::ostream& operator<<(std::ostream& stream, const IntermediateUpdate& iu) {
    stream << "UPDATE{" << iu.interval() << " "
           << (iu.notifier() > 0 ? "+" : "-")
           << iu.key() << " " << iu.context_ << "}";
    return stream;
}

} // namespace pq
