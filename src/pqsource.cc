#include "pqsource.hh"
#include "pqserver.hh"

namespace pq {

// XXX check circular expansion

uint64_t SourceRange::allocated_key_bytes = 0;

SourceRange::SourceRange(const parameters& p)
    : ibegin_(p.first), iend_(p.last), join_(p.join), joinpos_(p.joinpos) {
    assert(table_name(p.first, p.last));
    if (!ibegin_.is_local())
        allocated_key_bytes += ibegin_.length();
    if (!iend_.is_local())
        allocated_key_bytes += iend_.length();

    unsigned sink_mask = (p.sink ? p.sink->context_mask() : 0);
    unsigned context = p.join->context_mask(p.joinpos) & ~sink_mask;
    results_.push_back(result{Str(), p.sink});
    p.join->make_context(results_.back().context, p.match, context);
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

void SourceRange::remove_sink(SinkRange* sink, Str context) {
    assert(join() == sink->join());
    for (int i = 0; i != results_.size(); )
        if (results_[i].sink == sink && results_[i].context == context) {
            sink->deref();
            results_[i] = results_.back();
            results_.pop_back();
        } else
            ++i;
    if (results_.empty()) {
        source_table()->unlink_source(this);
        delete this;
    }
}

void SourceRange::notify(const Datum* src, const String& old_value, int notifier) const {
    using std::swap;
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; )
        if (!it->sink || it->sink->valid()) {
            unsigned sink_mask = it->sink ? it->sink->context_mask() : 0;
            if (sink_mask)
                join_->expand_sink_key_context(it->sink->context());
            if (it->context)
                join_->expand_sink_key_context(it->context);
            join_->expand_sink_key_source(src->key(), sink_mask);
            notify(join_->sink_key(), it->sink, src, old_value, notifier);
            ++it;
        } else {
            it->sink->deref();
            swap(*it, endit[-1]);
            results_.pop_back();
            --endit;
        }
    if (results_.empty()) {
        source_table()->unlink_source(const_cast<SourceRange*>(this));
        delete this;
    }
}

void SourceRange::invalidate() {
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ++it)
        if (it->sink && it->sink->valid())
            it->sink->invalidate();
    source_table()->unlink_source(this);
    delete this;
}

std::ostream& operator<<(std::ostream& stream, const SourceRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << "): copy ->";
    for (auto& res : r.results_)
        if (res.sink && res.sink->context_mask()) {
            Match m;
            r.join_->assign_context(m, res.sink->context());
            r.join_->assign_context(m, res.context);
            stream << " " << r.join_->unparse_match(m);
        } else
            stream << " " << r.join_->unparse_context(res.context);
    return stream << " ]" << r.subtree_iend() << "}";
}


void InvalidatorRange::notify(const Datum* d, const String&, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (notifier)
        for (auto& res : results_)
            res.sink->add_update(joinpos_, res.context, d->key(), notifier);
}

void CopySourceRange::notify(Str sink_key, SinkRange* sink, const Datum* src, const String&, int notifier) const {
    sink_table()->modify(sink_key, sink, [=](Datum*) {
             return notifier >= 0 ? src->value() : erase_marker();
        });
}


void CountSourceRange::notify(Str sink_key, SinkRange* sink, const Datum*, const String&, int notifier) const {
    sink_table()->modify(sink_key, sink, [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

void MinSourceRange::notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const {
    sink_table()->modify(sink_key, sink, [&](Datum* dst) -> String {
            if (!dst || src->value() < dst->value())
                 return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
                assert(0 && "removing old min");
            else
                return unchanged_marker();
        });
}

void MaxSourceRange::notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const {
    sink_table()->modify(sink_key, sink, [&](Datum* dst) -> String {
            if (!dst || dst->value() < src->value())
                return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
               assert(0 && "removing old max");
            else
                return unchanged_marker();
        });
}

void SumSourceRange::notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const {
    long diff = (notifier == notify_update) ?
        src->value().to_i() - old_value.to_i() :
        src->value().to_i();
    if (notifier == notify_erase)
        diff *= -1;
    sink_table()->modify(sink_key, sink, [&](Datum* dst) {
            if (!dst)
                return src->value();
            else if (diff)
                return String(dst->value().to_i() + diff);
            else
                return unchanged_marker();
        });
}

void BoundedCopySourceRange::notify(Str sink_key, SinkRange* sink, const Datum* src, const String& oldval, int notifier) const {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    sink_table()->modify(sink_key, sink, [=](Datum*) {
            return notifier >= 0 ? src->value() : erase_marker();
        });
}


void BoundedCountSourceRange::notify(Str sink_key, SinkRange* sink, const Datum* src, const String& oldval, int notifier) const {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    if (!notifier)
        return;
    sink_table()->modify(sink_key, sink, [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

} // namespace pq
