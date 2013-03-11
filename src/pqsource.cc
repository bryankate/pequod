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

void SourceRange::expand_results(const Datum* src) const {
    using std::swap;
    result* endit = results_.end();
    for (result* it = results_.begin(); it != endit; ++it)
        if (!it->sink || it->sink->valid())
	    join_->expand(it->key.mutable_udata(), src->key());
        else {
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


void CopySourceRange::notify(const Datum* src, const String&, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        expand_results(src);
	for (auto& res : results_) {
	    if (notifier >= 0)
                dst_table_->insert(res.key, src->value());
            else
		dst_table_->erase(res.key);
	}
    }
}


void CountSourceRange::notify(const Datum* src, const String&, int notifier) const {
    assert(notifier >= -1 && notifier <= 1);
    // XXX PERFORMANCE the match() is often not necessary
    if (notifier && join_->back_source().match(src->key())) {
        expand_results(src);
        for (auto& res : results_)
            dst_table_->modify(res.key, [=](Datum* dst, bool insert) {
                    return String(notifier
                                  + (insert ? 0 : dst->value_.to_i()));
                });
    }
}

void CountSourceAccumulator::notify(const Datum*) {
    ++n_;
}

void CountSourceAccumulator::commit(Str dst_key) {
    if (n_)
        dst_table_->insert(dst_key, String(n_));
    n_ = 0;
}


void MinSourceRange::notify(const Datum* src, const String& old_value, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        expand_results(src);
        for (auto& res : results_)
            dst_table_->modify(res.key, [&](Datum* dst, bool insert) -> String {
                    if (insert || src->value_ < dst->value_)
                        return src->value_;
                    else if (old_value == dst->value_
                             && (notifier < 0 || src->value_ != old_value))
                        assert(0 && "removing old min");
                    else
                        return unchanged_marker();
                });
    }
}

void MinSourceAccumulator::notify(const Datum* src) {
    if (!any_ || src->value_ < val_)
        val_ = src->value_;
    any_ = true;
}

void MinSourceAccumulator::commit(Str dst_key) {
    if (any_)
        dst_table_->insert(dst_key, std::move(val_));
    any_ = false;
    val_ = String();
}


void MaxSourceRange::notify(const Datum* src, const String& old_value, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        expand_results(src);
        for (auto& res : results_)
            dst_table_->modify(res.key, [&](Datum* dst, bool insert) -> String {
                    if (insert || dst->value_ < src->value_)
                        return src->value_;
                    else if (old_value == dst->value_
                             && (notifier < 0 || src->value_ != old_value))
                        assert(0 && "removing old max");
                    else
                        return unchanged_marker();
                });
    }
}

void MaxSourceAccumulator::notify(const Datum* src) {
    if (val_ < src->value_)
        val_ = src->value_;
    any_ = true;
}

void MaxSourceAccumulator::commit(Str dst_key) {
    if (any_)
        dst_table_->insert(dst_key, std::move(val_));
    any_ = false;
    val_ = String();
}


void SumSourceRange::notify(const Datum* src, const String& old_value, int notifier) const {
    if (join_->back_source().match(src->key())) {
        expand_results(src);
        for (auto& res : results_) {
            dst_table_->modify(res.key, [&](Datum* dst, bool insert) {
                if (insert)
                    return src->value_;
                else {
                    long diff = (notifier == notify_update) ?
                            src->value_.to_i() - old_value.to_i() :
                            src->value_.to_i();

                    if (notifier == notify_erase)
                        diff *= -1;

                    if (diff)
                        return String(dst->value_.to_i() + diff);
                    else
                        return unchanged_marker();
                }
            });
        }
    }
}

void SumSourceAccumulator::notify(const Datum* src) {
    sum_ += src->value_.to_i();
    any_ = true;
}

void SumSourceAccumulator::commit(Str dst_key) {
    if (any_)
        dst_table_->insert(dst_key, String(sum_));
    any_ = false;
    sum_ = 0;
}


void BoundedCopySourceRange::notify(const Datum* src, const String& oldval, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        if (!bounds_.check_bounds(src->value_, oldval, notifier))
            return;

        expand_results(src);
	for (auto& res : results_) {
	    if (notifier >= 0)
                dst_table_->insert(res.key, src->value());
            else
		dst_table_->erase(res.key);
	}
    }
}


void BoundedCountSourceRange::notify(const Datum* src, const String& oldval, int notifier) const {
    assert(notifier >= -1 && notifier <= 1);
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        if (!bounds_.check_bounds(src->value_, oldval, notifier))
            return;

        if (!notifier)
            return;

        expand_results(src);
        for (auto& res : results_)
            dst_table_->modify(res.key, [=](Datum* dst, bool insert) {
                    return String(notifier
                                  + (insert ? 0 : dst->value_.to_i()));
                });
    }
}

void BoundedCountSourceAccumulator::notify(const Datum* d) {
    if (bounds_.has_bounds() && !bounds_.in_bounds(d->value_.to_i()))
        return;
    ++n_;
}

void BoundedCountSourceAccumulator::commit(Str dst_key) {
    if (n_)
        dst_table_->insert(dst_key, String(n_));
    n_ = 0;
}

} // namespace pq
