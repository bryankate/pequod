#include "pqsink.hh"
#include "pqsource.hh"
#include "pqserver.hh"
#include "time.hh"

namespace pq {

uint64_t ServerRange::allocated_key_bytes = 0;

ServerRange::ServerRange(Str first, Str last, range_type type, Join* join)
    : type_(type), join_(join), expires_at_(0) {
    char* buf = buf_;
    char* endbuf = buf_ + sizeof(buf_);

    if (endbuf - buf >= first.length()) {
        ibegin_.assign(buf, first.length());
        buf += first.length();
    } else {
        ibegin_.assign(new char[first.length()], first.length());
        allocated_key_bytes += first.length();
    }
    memcpy(ibegin_.mutable_data(), first.data(), first.length());

    if (endbuf - buf >= last.length())
        iend_.assign(buf, last.length());
    else {
        iend_.assign(new char[last.length()], last.length());
        allocated_key_bytes += last.length();
    }
    memcpy(iend_.mutable_data(), last.data(), last.length());

    if (join_->staleness())
        expires_at_ = tstamp() + tous(join_->staleness());
}

ServerRange::~ServerRange() {
    if (ibegin_.data() < buf_ || ibegin_.data() >= buf_ + sizeof(buf_))
        delete[] ibegin_.mutable_data();
    if (iend_.data() < buf_ || iend_.data() >= buf_ + sizeof(buf_))
        delete[] iend_.mutable_data();
}

void ServerRange::validate(Str first, Str last, Server& server) {
    if (type_ == joinsink) {
        Match mf, ml;
        join_->sink().match(first, mf);
        join_->sink().match(last, ml);
        ValidJoinRange* sink = 0;
        if (join_->maintained() || join_->staleness())
            sink = server.add_validjoin(first, last, join_);
        validate(mf, ml, 0, server, sink);
    }
}

void ServerRange::validate(Match& mf, Match& ml, int joinpos, Server& server,
                           ValidJoinRange* sink) {
    uint8_t kf[128], kl[128];
    int kflen = join_->source(joinpos).expand_first(kf, mf);
    int kllen = join_->source(joinpos).expand_last(kl, ml);

    // need to validate the source ranges in case they have not been
    // expanded yet.
    // XXX PERFORMANCE this is not always necessary
    // XXX For now don't do this if the join is recursive
    if (table_name(Str(kf, kflen)) != join_->sink().table_name())
        server.validate(Str(kf, kflen), Str(kl, kllen));

    SourceRange* r = 0;
    if (joinpos + 1 == join_->nsource())
        r = join_->make_source(server, mf, Str(kf, kflen), Str(kl, kllen));

    auto it = server.lower_bound(Str(kf, kflen));
    auto ilast = server.lower_bound(Str(kl, kllen));

    Match mk(mf);
    mk &= ml;
    Match::state mfstate(mf.save()), mlstate(ml.save()), mkstate(mk.save());
    const Pattern& pat = join_->source(joinpos);

    for (; it != ilast; ++it) {
	if (it->key().length() != pat.key_length())
            continue;

        // XXX PERFORMANCE can prob figure out ahead of time whether this
        // match is simple/necessary
        if (pat.match(it->key(), mk)) {
            if (r)
                r->notify(it.operator->(), String(), SourceRange::notify_insert, true);
            else {
                pat.match(it->key(), mf);
                pat.match(it->key(), ml);
                validate(mf, ml, joinpos + 1, server, sink);
                mf.restore(mfstate);
                ml.restore(mlstate);
            }
        }

        mk.restore(mkstate);
    }

    if (join_->maintained()) {
        if (r) {
            r->set_sink(sink);
            server.add_copy(r);
        } else
            server.add_copy(new InvalidatorRange(server, join_, joinpos,
                                                 Str(kf, kflen), Str(kl, kllen),
                                                 sink));
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

} // namespace pq
