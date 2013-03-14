#include "pqjoin.hh"
#include "pqserver.hh"
#include "hashtable.hh"
#include "json.hh"
#include "error.hh"
namespace pq {

Match& Match::operator&=(const Match& m) {
    for (int i = 0; i != slot_capacity; ++i) {
        int l = 0;
        while (l != ms_.slotlen_[i] && l != m.ms_.slotlen_[i]
               && slot_[i][l] == m.slot_[i][l])
            ++l;
        ms_.slotlen_[i] = l;
    }
    return *this;
}

std::ostream& operator<<(std::ostream& stream, const Match& m) {
    stream << "{";
    const char* sep = "";
    for (int i = 0; i != slot_capacity; ++i)
	if (m.known_length(i)) {
	    stream << sep << i << ": ";
	    stream.write(reinterpret_cast<const char*>(m.data(i)),
			 m.known_length(i));
	    sep = ", ";
	}
    return stream << "}";
}

void Pattern::append_literal(uint8_t ch) {
    assert(ch < 128);
    assert(plen_ < pcap);
    pat_[plen_] = ch;
    ++plen_;
    ++klen_;
}

void Pattern::append_slot(int si, int len) {
    assert(plen_ < pcap);
    assert(si >= 0 && si < slot_capacity);
    assert(!has_slot(si));
    assert(len != 0);
    slotlen_[si] = len;
    slotpos_[si] = klen_;
    pat_[plen_] = 128 + si;
    ++plen_;
    klen_ += len;
}

void Pattern::clear() {
    klen_ = plen_ = 0;
    memset(pat_, 0, sizeof(pat_));
    for (int i = 0; i < slot_capacity; ++i)
        slotlen_[i] = slotpos_[i] = 0;
}

bool operator==(const Pattern& a, const Pattern& b) {
    return a.plen_ == b.plen_
        && a.klen_ == b.klen_
        && memcmp(a.pat_, b.pat_, Pattern::pcap) == 0
        && memcmp(a.slotlen_, b.slotlen_, slot_capacity) == 0;
}

void Join::clear() {
    npat_ = 0;
    for (int i = 0; i != pcap; ++i)
        pat_[i].clear();
}

/** Expand @a pat into the first matching string that matches @a match and
    could affect the sink range [@a sink_first, @a sink_last). */
int Join::expand_first(uint8_t* buf, const Pattern& pat,
                       Str sink_first, Str sink_last,
                       const Match& match) const {
    (void) sink_last;
    const Pattern& sinkpat = sink();
    uint8_t* os = buf;

    for (const uint8_t* ps = pat.pat_; ps != pat.pat_ + pat.plen_; ++ps)
        if (*ps < 128) {
            *os = *ps;
            ++os;
        } else {
            int slot = *ps - 128;

            // search for slot values in match and range
            int matchlen = match.known_length(slot);
            int sinkpos = 0, sinklen = 0;
            if (sinkpat.has_slot(slot)) {
                sinkpos = sinkpat.slot_position(slot);
                sinklen = std::min(std::max(sink_first.length() - sinkpos, 0), sinkpat.slot_length(slot));
            }
            if (matchlen == 0 && sinklen == 0)
                break;

            if (matchlen > 0 && sinklen > 0) {
                int cmp = String_generic::compare(match.data(slot), matchlen, sink_first.udata() + sinkpos, sinklen);
                if (cmp > 0) {
                    // The data contained in the match is greater than the
                    // data from the sink range. Use the match. But because
                    // we use the match, succeeding data from sink_first is
                    // no longer relevant, so cut it off.
                    sinklen = 0;
                    sink_first = Str(sink_first.data(), sinkpos);
                } else
                    // The data first position in the range is greater than
                    // the match. Use the range.
                    matchlen = 0;
            }

            const uint8_t* data;
            if (matchlen >= sinklen)
                data = match.data(slot);
            else {
                data = sink_first.udata() + sinkpos;
                matchlen = sinklen;
            }

            memcpy(os, data, matchlen);
            os += matchlen;
            if (matchlen < sinkpat.slot_length(slot))
                break;
        }

    return os - buf;
}

String Join::expand_first(const Pattern& pat, Str sink_first, Str sink_last, const Match& match) const {
    StringAccum sa(pat.key_length());
    sa.adjust_length(expand_first(sa.udata(), pat, sink_first, sink_last, match));
    return sa.take_string();
}

/** Expand @a pat into an upper bound for strings that match @a match and
    could affect the sink range [@a sink_first, @a sink_last). */
int Join::expand_last(uint8_t* buf, const Pattern& pat,
                      Str sink_first, Str sink_last,
                      const Match& match) const {
    const Pattern& sinkpat = sink();
    uint8_t* os = buf;
    int last_position = 0;

    for (const uint8_t* ps = pat.pat_; ps != pat.pat_ + pat.plen_; ++ps)
        if (*ps < 128) {
            *os = *ps;
            ++os;
        } else {
            int slot = *ps - 128;

            // search for slot in match and range
            int matchlen = match.known_length(slot);
            int sinkpos = 0, sinklen = 0;
            if (sinkpat.has_slot(slot)) {
                sinkpos = sinkpat.slot_position(slot);
                sinklen = std::min(std::max(sink_last.length() - sinkpos, 0), sinkpat.slot_length(slot));
            }
            if (matchlen == 0 && sinklen == 0)
                break;

            // Consider a range ["t|11111", "t|11112") (where the pattern is
            // "t|<x:5>"). All keys that actually match this range will have
            // the form "t|11111"! Detect this case.
            bool use_first = false;
            //std::cerr << "SINK " << sinklen << " " << sinkpat.slot_length(slot) << " " << matchlen << " "\n";
            if (sinklen == sinkpat.slot_length(slot) && matchlen < sinklen
                && sink_first.length() >= sinkpos + sinklen
                && sink_last.length() == sinkpos + sinklen) {
                const uint8_t* ldata = sink_last.udata() + sinkpos;
                const uint8_t* fdata = sink_first.udata() + sinkpos;
                int x = sinklen - 1;
                while (x >= 0 && ldata[x] == 0 && fdata[x] == 255)
                    --x;
                use_first = x >= 0 && ldata[x] == fdata[x] + 1
                    && memcmp(ldata, fdata, x) == 0;
            }

            const uint8_t* data;
            if (matchlen >= sinklen)
                data = match.data(slot);
            else if (use_first) {
                data = sink_first.udata() + sinkpos;
                matchlen = sinklen;
            } else {
                data = sink_last.udata() + sinkpos;
                matchlen = sinklen;
                last_position = (os - buf) + sinklen;
            }

            memcpy(os, data, matchlen);
            os += matchlen;
            if (matchlen < sinkpat.slot_length(slot))
                break;
        }

    if (os - buf != last_position) {
        int x = os - buf;
        while (x > 0 && ++buf[x - 1] == 0)
            --x;
    }
    return os - buf;
}

String Join::expand_last(const Pattern& pat, Str sink_first, Str sink_last, const Match& match) const {
    StringAccum sa(pat.key_length());
    sa.adjust_length(expand_last(sa.udata(), pat, sink_first, sink_last, match));
    return sa.take_string();
}

SourceRange* Join::make_source(Server& server, const Match& m,
                               Str ibegin, Str iend) {
    if (jvt() == jvt_copy_last)
        return new CopySourceRange(server, this, m, ibegin, iend);
    else if (jvt() == jvt_count_match)
        return new CountSourceRange(server, this, m, ibegin, iend);
    else if (jvt() == jvt_min_last)
        return new MinSourceRange(server, this, m, ibegin, iend);
    else if (jvt() == jvt_max_last)
        return new MaxSourceRange(server, this, m, ibegin, iend);
    else if (jvt() == jvt_sum_match)
        return new SumSourceRange(server, this, m, ibegin, iend);
    else if (jvt() == jvt_bounded_copy_last)
        return new BoundedCopySourceRange(server, this, m, ibegin, iend);
    else if (jvt() == jvt_bounded_count_match)
        return new BoundedCountSourceRange(server, this, m, ibegin, iend);
    else
        assert(0);
}

bool Pattern::assign_parse(Str str, HashTable<Str, int> &slotmap,
                           ErrorHandler* errh) {
    plen_ = klen_ = 0;
    for (auto s = str.ubegin(); s != str.uend(); ) {
	if (plen_ == pcap) {
            errh->error("pattern %<%p{Str}%> too long, max length %d chars",
                        &str, pcap);
	    return false;
        }
	if (*s != '<') {
	    append_literal(*s);
	    ++s;
	} else {
	    ++s;
	    // parse name up to length description
	    auto name0 = s;
	    while (s != str.uend() && *s != ':' && *s != '>')
		++s;
	    if (s == name0) {
                errh->error("malformed slot in pattern %<%p{Str}%>", &str);
		return false;
            }
	    Str name(name0, s);
            if (*s != ':' && slotmap.get(name) == -1) {
                errh->error("length of slot %<%p{Str}%> unknown", &name);
		return false;
            }
	    // parse name description
	    int len = 0;
	    if (*s == ':' && (s + 1 == str.uend() || !isdigit(s[1]))) {
                errh->error("malformed length for slot %<%p{Str}%>", &name);
		return false;
            }
	    else if (*s == ':') {
		len = s[1] - '0';
		for (s += 2; s != str.uend() && isdigit(*s); ++s)
		    len = 10 * len + *s - '0';
	    }
	    if (s == str.uend() || *s != '>') {
                errh->error("malformed slot %<%p{Str}%>", &name);
		return false;
            }
	    ++s;
	    // look up slot, maybe store it in map
	    int slot = slotmap.get(name);
	    if (slot == -1) {
		if (len == 0 || slotmap.size() == slot_capacity) {
                    errh->error("too many slots in pattern %<%p{Str}%>, max %d", &str, slot_capacity);
		    return false;
                }
		slot = len + 256 * slotmap.size();
		slotmap.set(name, slot);
	    } else if (len != 0 && len != (slot & 255)) {
                errh->error("length of slot %<%p{Str}%> redeclared (was %d, now %d)", &name, slot & 255, len);
		return false;
            }
	    // add slot
	    append_slot(slot >> 8, slot & 255);
	}
    }
    if (klen_ > key_capacity) {
        errh->error("key length implied by pattern too long (have %d, max %d)", klen_, key_capacity);
        return false;
    }
    return true;
}

bool Pattern::assign_parse(Str str, ErrorHandler* errh) {
    HashTable<Str, int> slotmap(-1);
    return assign_parse(str, slotmap, errh);
}

bool Join::assign_parse(Str str, ErrorHandler* errh) {
    FileErrorHandler base_errh(stderr);
    errh = errh ? errh : &base_errh;
    HashTable<Str, int> slotmap(-1);
    npat_ = 0;
    auto s = str.ubegin();
    while (1) {
	while (s != str.uend() && isspace(*s))
	    ++s;
	auto pbegin = s;
	while (s != str.uend() && !isspace(*s))
	    ++s;
	if (pbegin == s)
            return analyze(slotmap, errh);
        if (npat_ == pcap) {
            errh->error("too many patterns in join (max %d)", pcap);
            return false;
        }
	if (!pat_[npat_].assign_parse(Str(pbegin, s), slotmap, errh))
	    return false;
        // Every pattern must involve a known table
        if (!pat_[npat_].table_name()) {
            errh->error("pattern %<%.*s%> does not match a unique table prefix", (int) (s - pbegin), pbegin);
            return false;
        }
	++npat_;
    }
}

bool Join::analyze(HashTable<Str, int>& slotmap, ErrorHandler* errh) {
    // fail if too few patterns
    if (npat_ <= 1) {
        errh->error("too few patterns in join (min 2)");
        return false;
    }

    // account for slots across all patterns
    // completion_source_ is the source number after which sink() is complete
    int need_slots = 0;
    for (int s = 0; s < slot_capacity; ++s)
        if (sink().has_slot(s))
            need_slots |= 1 << s;

    completion_source_ = -1;
    while (need_slots) {
        ++completion_source_;
        if (completion_source_ >= nsource()) {
            String slot_name;
            for (auto it = slotmap.begin(); it != slotmap.end(); ++it)
                if (need_slots & (1 << it->second))
                    slot_name = it->first;
            errh->error("slot %<%s%> in sink not defined by sources", slot_name.c_str());
            return false;
        }
        for (int s = 0; s < slot_capacity; ++s)
            if (source(completion_source_).has_slot(s)
                && source(completion_source_).slot_length(s) == sink().slot_length(s))
                need_slots &= ~(1 << s);
    }

    // success
    return true;
}

Json Pattern::unparse_json() const {
    Json j = Json::make_array();
    const uint8_t* p = pat_;
    while (p != pat_ + plen_) {
	if (*p >= 128) {
	    j.push_back(Json::make_array(*p - 127, slotlen_[*p - 128]));
	    ++p;
	} else {
	    const uint8_t* pfirst = p;
	    for (++p; p != pat_ + plen_ && *p < 128; ++p)
		/* do nothing */;
	    j.push_back(String(pfirst, p));
	}
    }
    return j;
}

String Pattern::unparse() const {
    return unparse_json().unparse();
}

std::ostream& operator<<(std::ostream& stream, const Pattern& m) {
    stream << "{";
    const uint8_t* p = m.pat_;
    while (p != m.pat_ + m.plen_) {
        if (*p >= 128) {
            stream << "<pos: " << (int)m.slotpos_[*p - 128]
                   << " len: " << (int)m.slotlen_[*p - 128] << ">";
            ++p;
        } else {
            const uint8_t* pfirst = p;
            for (++p; p != m.pat_ + m.plen_ && *p < 128; ++p)
                /* do nothing */;
            stream << String(pfirst, p);
        }
    }
    return stream << "}";
}

Json Join::unparse_json() const {
    Json j = Json::make_array();
    for (int i = 0; i < npat_; ++i)
	j.push_back(pat_[i].unparse_json());
    return j;
}

String Join::unparse() const {
    return unparse_json().unparse();
}

bool Join::same_structure(const Join& x) const {
    if (npat_ != x.npat_ || jvt_ != x.jvt_)
        return false;
    for (int i = 0; i != npat_; ++i)
        if (pat_[i] != x.pat_[i])
            return false;
    return true;
}

std::ostream& operator<<(std::ostream& stream, const Join& join) {
    return stream << join.unparse();
}

} // namespace
