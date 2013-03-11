#include "pqsource.hh"
#include "pqserver.hh"

namespace pq {

// XXX check circular expansion

uint64_t SourceRange::allocated_key_bytes = 0;

SourceRange::SourceRange(Server& server, Join* join, const Match& m,
                         Str ibegin, Str iend, SinkBound *sb)
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
    if (sb == NULL)
        sb = new SinkBound(dst_table_, join_->jvt() != jvt_copy_last);
    resultkeys_.push_back(resultkey_type(std::move(str), sb));
}

SourceRange::~SourceRange() {
    if (ibegin_.data() < buf_ || ibegin_.data() >= buf_ + sizeof(buf_))
        delete[] ibegin_.mutable_data();
    if (iend_.data() < buf_ || iend_.data() >= buf_ + sizeof(buf_))
        delete[] iend_.mutable_data();
}

void SourceRange::add_sinks(const SourceRange& r) {
    assert(join() == r.join());
    for (auto rk : r.resultkeys_)
        resultkeys_.push_back(rk);
}

std::ostream& operator<<(std::ostream& stream, const SourceRange& r) {
    stream << "{" << "[" << r.ibegin() << ", " << r.iend() << "): copy ->";
    for (auto s : r.resultkeys_)
        stream << " " << s.first;
    return stream << " ]" << r.subtree_iend() << "}";
}

SinkBound::SinkBound(Table *t, bool single_sink)
    : first_(t->end()), last_(t->end()), single_sink_(single_sink) {
}

void SinkBound::update(StoreIterator it, Table *t, bool insert) {
    if (insert) {
        if (single_sink_) {
            first_ = last_ = it;
            return;
        }
        if (first_ == t->end() || inext(it) == first_)
            first_ = it;
        if (last_ == t->end() || inext(last_) == it)
            last_ = it;
    } else {
        if (single_sink_) {
            first_ = last_ = t->end();
            return;
        }
        if (first_ == it)
            first_ = inext(it);
        if (last_ == it)
            last_ = (it == t->begin()) ? t->end() : iprev(it);
    }
}


void CopySourceRange::notify(const Datum* src, const String& oldval, int notifier) {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        if (!check_bounds(src->value_, oldval, notifier))
            return;

	for (auto& s : resultkeys_) {
	    join_->expand(s.first.mutable_udata(), src->key());
            dst_table_->modify(s.first, *s.second, [&](Datum *) -> String {
                    return (notifier >= 0) ? src->value_ : erase_marker();
                });
	}
    }
}


void CountSourceRange::notify(const Datum* src, const String& oldval, int notifier) {
    assert(notifier >= -1 && notifier <= 1);
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        if (!check_bounds(src->value_, oldval, notifier))
            return;

        if (!notifier)
            return;

        for (auto& s : resultkeys_) {
            join_->expand(s.first.mutable_udata(), src->key());
            dst_table_->modify(s.first, *s.second, [=](Datum* dst) {
                    return String(notifier
                                  + (dst ? dst->value_.to_i() : 0));
                });
        }
    }
}


void MinSourceRange::notify(const Datum* src, const String& old_value, int notifier) {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        for (auto& s : resultkeys_) {
            join_->expand(s.first.mutable_udata(), src->key());
            dst_table_->modify(s.first, *s.second, [&](Datum* dst) -> String {
                    if (!dst || src->value_ < dst->value_)
                        return src->value_;
                    else if (old_value == dst->value_
                             && (notifier < 0 || src->value_ != old_value))
                        assert(0 && "removing old min");
                    else
                        return unchanged_marker();
                });
        }
    }
}

void MaxSourceRange::notify(const Datum* src, const String& old_value, int notifier) {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        for (auto& s : resultkeys_) {
            join_->expand(s.first.mutable_udata(), src->key());
            dst_table_->modify(s.first, *s.second, [&](Datum* dst) -> String {
                    if (!dst || dst->value_ < src->value_)
                        return src->value_;
                    else if (old_value == dst->value_
                             && (notifier < 0 || src->value_ != old_value))
                        assert(0 && "removing old max");
                    else
                        return unchanged_marker();
                });
        }
    }
}

void SumSourceRange::notify(const Datum* src, const String& old_value, int notifier) {
    if (join_->back_source().match(src->key())) {
        for (auto& s : resultkeys_) {
            join_->expand(s.first.mutable_udata(), src->key());
            dst_table_->modify(s.first, *s.second, [&](Datum* dst) {
                if (!dst)
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

} // namespace pq
