#include "pqsource.hh"
#include "pqserver.hh"

namespace pq {

// XXX check circular expansion

uint64_t SourceRange::allocated_key_bytes = 0;

SourceRange::SourceRange(Server& server, Join* join, const Match& m,
                         Str ibegin, Str iend)
    : join_(join), dst_table_(&server.make_table(join->sink().table_name())) {
    assert(table_name(ibegin, iend));
    char* buf = buf_;
    char* endbuf = buf_ + sizeof(buf_);

    if (endbuf - buf >= ibegin.length()) {
        ibegin_.assign(buf, ibegin.length());
        buf += ibegin.length();
    } else {
        ibegin_.assign(new char[ibegin.length()], ibegin.length());
        allocated_key_bytes += ibegin.length();
    }
    memcpy(ibegin_.mutable_data(), ibegin.data(), ibegin.length());

    if (endbuf - buf >= iend.length())
        iend_.assign(buf, iend.length());
    else {
        iend_.assign(new char[iend.length()], iend.length());
        allocated_key_bytes += iend.length();
    }
    memcpy(iend_.mutable_data(), iend.data(), iend.length());

    String str = String::make_uninitialized(join_->sink().key_length());
    join_->sink().expand(str.mutable_udata(), m);
    results_.push_back(result{std::move(str), 0});
}

SourceRange::~SourceRange() {
    if (ibegin_.data() < buf_ || ibegin_.data() >= buf_ + sizeof(buf_))
        delete[] ibegin_.mutable_data();
    if (iend_.data() < buf_ || iend_.data() >= buf_ + sizeof(buf_))
        delete[] iend_.mutable_data();
    for (auto& r : results_)
        if (r.sink)
            r.sink->deref();
}

void SourceRange::take_results(SourceRange& r) {
    assert(join() == r.join());
    for (auto& rk : r.results_)
        results_.push_back(std::move(rk));
    r.results_.clear();
}

void SourceRange::notify(const Datum* src, const String& old_value, int notifier, bool known_match) const {
    if (!known_match && !join_->back_source().match(src->key()))
        return;
    using std::swap;
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; )
        if (!it->sink || it->sink->valid()) {
	    join_->expand(it->key.mutable_udata(), src->key());
            notify(*it, src, old_value, notifier);
            ++it;
        } else {
            it->sink->deref();
            swap(*it, endit[-1]);
            results_.pop_back();
            --endit;
        }
}

std::ostream& operator<<(std::ostream& stream, const SourceRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << "): copy ->";
    for (auto& res : r.results_)
        stream << " " << res.key;
    return stream << " ]" << r.subtree_iend() << "}";
}


void InvalidatorRange::notify(const Datum* src, const String&, int, bool known_match) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->source(joinpos_).match(src->key())) {
	for (auto& res : results_) {
            res.sink->invalidate();
            res.sink->deref();
        }
        results_.clear();
    }
}

void CopySourceRange::notify(result& res, const Datum* src, const String&, int notifier) const {
    dst_table_->modify(res.key, res.sink, [=](Datum*) {
             return notifier >= 0 ? src->value() : erase_marker();
        });
}


void CountSourceRange::notify(result& res, const Datum* src, const String&, int notifier) const {
    dst_table_->modify(res.key, res.sink, [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

void MinSourceRange::notify(result& res, const Datum* src, const String& old_value, int notifier) const {
    dst_table_->modify(res.key, res.sink, [&](Datum* dst) -> String {
            if (!dst || src->value() < dst->value())
                 return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
                assert(0 && "removing old min");
            else
                return unchanged_marker();
        });
}

void MaxSourceRange::notify(result& res, const Datum* src, const String& old_value, int notifier) const {
    dst_table_->modify(res.key, res.sink, [&](Datum* dst) -> String {
            if (!dst || dst->value() < src->value())
                return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
               assert(0 && "removing old max");
            else
                return unchanged_marker();
        });
}

void SumSourceRange::notify(result& res, const Datum* src, const String& old_value, int notifier) const {
    dst_table_->modify(res.key, res.sink, [&](Datum* dst) {
            if (!dst)
                return src->value();
            else {
                long diff = (notifier == notify_update) ?
                    src->value().to_i() - old_value.to_i() : 
                    src->value().to_i();
                 if (notifier == notify_erase)
                     diff *= -1;
                 if (diff)
                     return String(dst->value().to_i() + diff);
                 else
                     return unchanged_marker();
           }
        });
}

void BoundedCopySourceRange::notify(result& res, const Datum* src, const String& oldval, int notifier) const {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    dst_table_->modify(res.key, res.sink, [=](Datum*) {
            return notifier >= 0 ? src->value() : erase_marker();
        });
}


void BoundedCountSourceRange::notify(result& res, const Datum* src, const String& oldval, int notifier) const {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    if (!notifier)
        return;
    dst_table_->modify(res.key, res.sink, [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

} // namespace pq
