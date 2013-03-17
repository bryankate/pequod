#include "pqsource.hh"
#include "pqserver.hh"

namespace pq {

// XXX check circular expansion

uint64_t SourceRange::allocated_key_bytes = 0;

SourceRange::SourceRange(const parameters& p)
    : ibegin_(p.first), iend_(p.last), join_(p.join),
      joinpos_(p.join->nsource() - 1),
      dst_table_(&p.server.make_table(p.join->sink().table_name())) {
    assert(table_name(p.first, p.last));
    if (!ibegin_.is_local())
        allocated_key_bytes += ibegin_.length();
    if (!iend_.is_local())
        allocated_key_bytes += iend_.length();

    String str = String::make_uninitialized(join_->sink().key_length());
    join_->sink().expand(str.mutable_udata(), p.match);
    results_.push_back(result{std::move(str), p.sink});
    if (p.sink)
        p.sink->ref();
}

SourceRange::~SourceRange() {
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

void SourceRange::remove_sink(SinkRange* sink) {
    assert(join() == sink->join());
    for (int i = 0; i != results_.size(); )
        if (results_[i].sink == sink) {
            results_[i] = results_.back();
            results_.pop_back();
        } else
            ++i;
}

void SourceRange::notify(const Datum* src, const String& old_value, int notifier) const {
    using std::swap;
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; )
        if (!it->sink || true /* XXX it->sink->valid() */) {
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


void InvalidatorRange::notify(const Datum* d, const String&, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (notifier)
        for (auto& res : results_)
            res.sink->add_update(joinpos_, res.key, d->key(), notifier);
}

void CopySourceRange::notify(result& res, const Datum* src, const String&, int notifier) const {
    dst_table_->modify(res.key, res.sink, [=](Datum*) {
             return notifier >= 0 ? src->value() : erase_marker();
        });
}


void CountSourceRange::notify(result& res, const Datum*, const String&, int notifier) const {
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
    long diff = (notifier == notify_update) ?
        src->value().to_i() - old_value.to_i() :
        src->value().to_i();
    if (notifier == notify_erase)
        diff *= -1;
    dst_table_->modify(res.key, res.sink, [&](Datum* dst) {
            if (!dst)
                return src->value();
            else if (diff)
                return String(dst->value().to_i() + diff);
            else
                return unchanged_marker();
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
