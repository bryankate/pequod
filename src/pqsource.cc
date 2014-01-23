#include "pqsource.hh"
#include "pqserver.hh"
#include "pqinterconnect.hh"
#include <typeinfo>

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

    results_.push_back(result{Str(), p.sink});
    p.sink->ref();

    if (p.join) {
        unsigned sink_mask = (p.sink ? p.sink->context_mask() : 0);
        unsigned context = p.join->context_mask(p.joinpos) & ~sink_mask;
        p.join->make_context(results_.back().context, p.match, context);
    }
}

SourceRange::~SourceRange() {
    for (auto& r : results_)
        r.sink->deref();
}

void SourceRange::kill() {
    join_->server().table_for(ibegin(), iend()).unlink_source(this);
    delete this;
}

void SourceRange::take_results(SourceRange& r) {
    assert(join() == r.join());
    for (auto& rk : r.results_)
        results_.push_back(std::move(rk));
    r.results_.clear();
}

void SourceRange::remove_sink(Sink* sink, Str context) {
    assert(join() == sink->join());
    for (int i = 0; i != results_.size(); )
        if (results_[i].sink == sink && results_[i].context == context) {
            sink->deref();
            results_[i] = results_.back();
            results_.pop_back();
        } else
            ++i;
    if (results_.empty())
        kill();
}

bool SourceRange::can_evict() const {
    return false;
}

bool SourceRange::check_match(Str key) const {
    return join_->source(joinpos_).match(key);
}

void SourceRange::notify(const Datum* src, const String& old_value, int notifier) const {
    using std::swap;
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ) {
        if (it + 1 != endit)
            (it + 1)->sink->prefetch();
        if (it->sink->valid()) {
            it->sink->table()->prefetch();
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
    }
    if (results_.empty())
        const_cast<SourceRange*>(this)->kill();
}

void SourceRange::invalidate() {
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ++it)
        if (it->sink->valid())
            it->sink->invalidate();
    kill();
}

std::ostream& operator<<(std::ostream& stream, const SourceRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << "): "
           << typeid(r).name() << " ->";
    for (auto& res : r.results_) {
        stream << " ";
        if (res.sink) {
            stream << res.sink->interval();
            if (res.context)
                stream << "@";
        }
        if (res.context)
            stream << r.join_->unparse_context(res.context);
    }
    return stream << "}";
}


void InvalidatorRange::notify(const Datum* d, const String&, int notifier) const {
    using std::swap;

    if (!notifier)
        return;

    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ) {
        if (it->sink->valid()) {
            it->sink->add_update(joinpos_, it->context, d->key(), notifier);
            ++it;
        }
        else {
            it->sink->deref();
            swap(*it, endit[-1]);
            results_.pop_back();
            --endit;
        }
    }

    if (results_.empty())
        const_cast<InvalidatorRange*>(this)->kill();
}

bool InvalidatorRange::can_evict() const {
    return true;
}

void SubscribedRange::invalidate() {
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ++it) {
        RemoteSink* sink = reinterpret_cast<RemoteSink*>(it->sink);
        //std::cerr << "sending invalidation of " << interval() << " to " << sink->peer() << std::endl;
        sink->conn()->invalidate(ibegin(), iend(), tamer::event<>());
    }
    kill();
}

void SubscribedRange::kill() {
    server_.table_for(ibegin(), iend()).unlink_source(this);
    delete this;
}

bool SubscribedRange::check_match(Str) const {
    return true;
}

void SubscribedRange::notify(const Datum* src, const String&, int notifier) const {
    for (result* it = results_.begin(); it != results_.end(); ++it) {
        RemoteSink* sink = reinterpret_cast<RemoteSink*>(it->sink);
        switch(notifier) {
            case SourceRange::notify_erase:
                sink->conn()->notify_erase(src->key(), tamer::event<>());
                break;
            case SourceRange::notify_insert:
            case SourceRange::notify_update:
                sink->conn()->notify_insert(src->key(), src->value(), tamer::event<>());
                break;
        }
    }
}

bool SubscribedRange::can_evict() const {
    return true;
}

void CopySourceRange::notify(Str sink_key, Sink* sink, const Datum* src, const String&, int notifier) const {
#if HAVE_VALUE_SHARING_ENABLED
    sink->make_table_for(sink_key).modify(sink_key, sink, [=](Datum*) {
             return notifier >= 0 ? src->value() : erase_marker();
        });
#else
    sink->table()->modify(sink_key, sink, [=](Datum*) {
             return notifier >= 0 ? String(src->value().data(), src->value().length()) : erase_marker();
        });
#endif
}

bool CopySourceRange::can_evict() const {
    return true;
}

void CountSourceRange::notify(Str sink_key, Sink* sink, const Datum*, const String&, int notifier) const {
    sink->make_table_for(sink_key).modify(sink_key, sink, [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

void MinSourceRange::notify(Str sink_key, Sink* sink, const Datum* src, const String& old_value, int notifier) const {
    sink->make_table_for(sink_key).modify(sink_key, sink, [&](Datum* dst) -> String {
            if (!dst || src->value() < dst->value())
                 return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
                return invalidate_marker();
            else
                return unchanged_marker();
        });
}

void MaxSourceRange::notify(Str sink_key, Sink* sink, const Datum* src, const String& old_value, int notifier) const {
    sink->make_table_for(sink_key).modify(sink_key, sink, [&](Datum* dst) -> String {
            if (!dst || dst->value() < src->value())
                return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
                return invalidate_marker();
            else
                return unchanged_marker();
        });
}

void SumSourceRange::notify(Str sink_key, Sink* sink, const Datum* src, const String& old_value, int) const {
    long diff = src->value().to_i() - old_value.to_i();
    sink->make_table_for(sink_key).modify(sink_key, sink, [&](Datum* dst) -> String {
            if (!dst)
                return src->value();
            else if (diff)
                return String(dst->value().to_i() + diff);
            else
                return unchanged_marker();
        });
}

void BoundedCopySourceRange::notify(Str sink_key, Sink* sink, const Datum* src, const String& oldval, int notifier) const {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    sink->make_table_for(sink_key).modify(sink_key, sink, [=](Datum*) -> String {
            return notifier >= 0 ? src->value() : erase_marker();
        });
}


void BoundedCountSourceRange::notify(Str sink_key, Sink* sink, const Datum* src, const String& oldval, int notifier) const {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    if (!notifier)
        return;
    sink->make_table_for(sink_key).modify(sink_key, sink, [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

} // namespace pq
