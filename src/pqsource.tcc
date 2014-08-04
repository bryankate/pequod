#include "pqsource.hh"
#include "pqserver.hh"
#include "pqinterconnect.hh"
#include <typeinfo>

namespace pq {

// XXX check circular expansion

uint64_t SourceRange::allocated_key_bytes = 0;

BloomFilter* make_bloom(Server& server, Str first, Str last) {
    BloomFilter* bloom = new BloomFilter(1 << 18, 0.001);

    auto it = server.table_for(first, last).lower_bound(first);
    auto itend = it.table_end();

    for (; it != itend; ++it)
        bloom->add(it->key().data(), it->key().length());

    return bloom;
}

SourceRange::SourceRange(const parameters& p)
    : ibegin_(p.first), iend_(p.last), join_(p.join), joinpos_(p.joinpos), purged_(false) {
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

bool SourceRange::purge(Server&) {
    purged_ = true;

#if PREVENT_EVICTION_OVERHEAD
    return true;
#else
    invalidate();
    return false;
#endif
}

bool SourceRange::check_match(Str key) const {
    return join_->source(joinpos_).match(key);
}

void SourceRange::notify(const Datum* src, const String& old_value, int notifier) {
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

tamed void UsingRange::notify(const Datum* d, const String&, int notifier) {
    using std::swap;

    if (!notifier)
        return;

    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ) {
        if (it->sink->valid()) {
            it->sink->add_update(joinpos_, it->context, d->key(), notifier);
            if (!lazy_)
                eager_update(it->sink);
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
        kill();
}

tamed void UsingRange::eager_update(Sink* sink) {
    tvars {
        uint32_t log = 0;
        tamer::gather_rendezvous gr;
    }

    do {
        twait(gr);
        if (!sink->valid())
            break;
        
        sink->validate(sink->ibegin(), sink->iend(), server_,
                       server_.next_validate_at(), log, gr);
    } while (gr.has_waiting());
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

void SubscribedRange::notify(const Datum* src, const String&, int notifier) {
    for (result* it = results_.begin(); it != results_.end(); ++it) {
        RemoteSink* sink = reinterpret_cast<RemoteSink*>(it->sink);
        if (notifier < 0)
            sink->conn()->notify_erase(src->key(), tamer::event<>());
        else
            sink->conn()->notify_insert(src->key(), src->value(), tamer::event<>());
    }
}

void CopySourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                             const String&, int notifier) {
#if HAVE_VALUE_SHARING_ENABLED
    sink->make_table_for(sink_key).modify(sink_key, sink, [=](Datum*) {
             return notifier >= 0 ? src->value() : erase_marker();
        });
#else
    sink->table()->modify(sink_key, sink, [=](Datum*) {
             return notifier >= 0 ? String(src->value().data(), src->value().length())
                                  : erase_marker();
        });
#endif
}

bool CountSourceRange::purge(Server& server) {
    purged_ = true;

#if PREVENT_EVICTION_OVERHEAD
    if (!bloom_)
        bloom_ = make_bloom(server, ibegin(), iend());
    return true;
#else
    invalidate();
    return false;
#endif
}

void CountSourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                              const String&, int notifier) {
    if (bloom_) {
        switch(notifier) {
            case notify_erase:
                // we can only get this notifier if the source key is present,
                // meaning it was added after eviction or it belonged to a
                // range that overlaps with this source but was not evicted.
                // it is safe to decrement the counter in this case.
                goto mod;

            case notify_insert:
                // we can count it if it is a new insertion
                if (!bloom_->check(src->key().data(), src->key().length())) {
                    bloom_->add(src->key().data(), src->key().length());
                    goto mod;
                }
                break;

            case notify_update:
                // we can only get an update notification for a key that
                // is present in the store and already counted. ignore it.
                return;

            case notify_erase_missing:
                // an erase command was issued by the client but the key
                // was not in the store. if it is not in the filter
                // then it is spurious and can be ignored
                if (!bloom_->check(src->key().data(), src->key().length()))
                    return;
                break;
        }

        // we cannot guarantee correctness, so invalidate the source
        invalidate();
        return;
    }

    mod:
    sink->make_table_for(sink_key).modify(sink_key, sink,
        [=](Datum* dst) {
            return String(notifier + (dst ? dst->value().to_i() : 0));
        });
}

void MinSourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                            const String& old_value, int notifier) {
    sink->make_table_for(sink_key).modify(sink_key, sink,
        [&](Datum* dst) -> String {
            if (!dst || src->value() < dst->value())
                return src->value();
            else if (old_value == dst->value()
                    && (notifier < 0 || src->value() != old_value))
                return invalidate_marker();
            else
                return unchanged_marker();
        });
}

void MaxSourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                            const String& old_value, int notifier) {
    sink->make_table_for(sink_key).modify(sink_key, sink,
        [&](Datum* dst) -> String {
            if (!dst || dst->value() < src->value())
                return src->value();
            else if (old_value == dst->value()
                     && (notifier < 0 || src->value() != old_value))
                return invalidate_marker();
            else
                return unchanged_marker();
        });
}

bool SumSourceRange::purge(Server& server) {
    purged_ = true;

#if PREVENT_EVICTION_OVERHEAD
    if (!bloom_)
        bloom_ = make_bloom(server, ibegin(), iend());
    return true;
#else
    invalidate();
    return false;
#endif
}

void SumSourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                            const String& old_value, int notifier) {
    if (bloom_) {
        switch(notifier) {
            case notify_erase:
                // we can only get this notifier if the source key is present,
                // meaning it was added after eviction or it belonged to a
                // range that overlaps with this source but was not evicted.
                // it is safe to decrement the sum in this case
                goto mod;

            case notify_insert:
                // we can count it if it is a new insertion
                if (!bloom_->check(src->key().data(), src->key().length())) {
                    bloom_->add(src->key().data(), src->key().length());
                    goto mod;
                }
                break;

            case notify_update:
                // we can only get an update notification for a key
                // that is in the store. handle it accordingly.
                goto mod;

            case notify_erase_missing:
                // an erase command was issued by the client but the key
                // was not in the store. if it is not in the filter
                // then it is spurious and can be ignored
                if (!bloom_->check(src->key().data(), src->key().length()))
                    return;
                break;
        }

        // we cannot guarantee correctness, so invalidate the source
        invalidate();
        return;
    }

    mod:
    long diff = src->value().to_i() - old_value.to_i();
    sink->make_table_for(sink_key).modify(sink_key, sink,
        [&](Datum* dst) -> String {
            if (!dst)
                return src->value();
            else if (diff)
                return String(dst->value().to_i() + diff);
            else
                return unchanged_marker();
        });
}

void BoundedCopySourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                                    const String& oldval, int notifier) {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    CopySourceRange::notify(sink_key, sink, src, oldval, notifier);
}

void BoundedCountSourceRange::notify(Str sink_key, Sink* sink, const Datum* src,
                                     const String& oldval, int notifier) {
    if (!bounds_.check_bounds(src->value(), oldval, notifier))
        return;
    if (!notifier)
        return;
    CountSourceRange::notify(sink_key, sink, src, oldval, notifier);
}

} // namespace pq
