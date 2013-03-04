#include "pqsource.hh"
#include "pqserver.hh"

namespace pq {

// XXX check circular expansion

SourceRange::SourceRange(Server& server, Join* join, const Match& m,
                         Str ibegin, Str iend)
    : join_(join), dst_table_(&server.make_table(join->sink().table_name())) {
    assert(table_name(ibegin, iend));
    char* s = buf_;
    char* ends = buf_ + sizeof(buf_);

    if (ends - s >= ibegin.length()) {
        ibegin_.assign(s, ibegin.length());
        s += ibegin.length();
    } else
        ibegin_.assign(new char[ibegin.length()], ibegin.length());
    memcpy(ibegin_.mutable_data(), ibegin.data(), ibegin.length());

    if (ends - s >= iend.length())
        iend_.assign(s, iend.length());
    else
        iend_.assign(new char[iend.length()], iend.length());
    memcpy(iend_.mutable_data(), iend.data(), iend.length());

    String str = String::make_uninitialized(join_->sink().key_length());
    join_->sink().expand(str.mutable_udata(), m);
    resultkeys_.push_back(std::move(str));
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
        stream << " " << s;
    return stream << " ]" << r.subtree_iend() << "}";
}


void CopySourceRange::notify(const Datum* src, const String&, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key()))
	for (auto& s : resultkeys_) {
	    join_->expand(s.mutable_udata(), src->key());
	    if (notifier >= 0)
                dst_table_->insert(s, src->value());
            else
		dst_table_->erase(s);
	}
}


void CountSourceRange::notify(const Datum* src, const String&, int notifier) const {
    assert(notifier >= -1 && notifier <= 1);
    // XXX PERFORMANCE the match() is often not necessary
    if (notifier && join_->back_source().match(src->key())) {
        for (auto& s : resultkeys_) {
            join_->expand(s.mutable_udata(), src->key());
            dst_table_->modify(s, [=](Datum* dst, bool insert) {
                    return String(notifier
                                  + (insert ? 0 : dst->value_.to_i()));
                });
        }
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
        for (auto& s : resultkeys_) {
            join_->expand(s.mutable_udata(), src->key());
            dst_table_->modify(s, [=](Datum* dst, bool insert) -> String {
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
        for (auto& s : resultkeys_) {
            join_->expand(s.mutable_udata(), src->key());
            dst_table_->modify(s, [=](Datum* dst, bool insert) -> String {
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


void JVSourceRange::notify(const Datum* src, const String&, int notifier) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (join_->back_source().match(src->key())) {
        JoinValue jv(join_->jvt());
	for (auto& s : resultkeys_) {
	    join_->expand(s.mutable_udata(), src->key());
	    if (notifier >= 0) {
                jv.reset();
                jv.accum(s, src->value_, true, true);
                dst_table_->modify(jv.key(), jv);
            } else
		dst_table_->erase(s);
	}
    }
}

} // namespace pq
