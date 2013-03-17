#include "pqjoin.hh"
#include "pqserver.hh"
#include "hashtable.hh"
#include "json.hh"
#include "time.hh"
#include "error.hh"
namespace pq {

// k|<user> = count v|<follower>
//      using s|<follower>|<time> using x|<user>|<poster>

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

Pattern::Pattern() {
    static_assert((sizeof(klen_) << 8) >= key_capacity
                  && (sizeof(slotpos_[0]) << 8) >= key_capacity,
                  "key_capacity too big for Pattern use");
    clear();
}

void Pattern::clear() {
    klen_ = plen_ = 0;
    memset(pat_, 0, sizeof(pat_));
    for (int i = 0; i != slot_capacity; ++i)
        slotlen_[i] = slotpos_[i] = 0;
}

void Pattern::match_range(Str first, Str last, Match& m) const {
    const uint8_t* fs = first.udata(), *ls = last.udata();
    const uint8_t* efs = fs + std::min(first.length(), last.length());
    for (const uint8_t* p = pat_; p != pat_ + plen_ && fs != efs; ++p)
        if (*p < 128) {
            if (*fs != *p || *ls != *p)
                return;
            ++fs, ++ls;
        } else {
            const uint8_t* ms = m.data(*p - 128);
            int ml = m.known_length(*p - 128);
            int mp = 0;
            while (mp != ml) {
                if (fs + mp == efs || fs[mp] != ms[mp] || ls[mp] != ms[mp])
                    return;
                ++mp;
            }
            while (mp != slotlen_[*p - 128] && fs + mp != efs
                   && fs[mp] == ls[mp])
                ++mp;
            if (mp != slotlen_[*p - 128] && fs + slotlen_[*p - 128] == efs
                && fs[mp] + 1 == ls[mp]) {
                for (int xp = mp + 1; xp != slotlen_[*p - 128]; ++xp)
                    if (fs[xp] != 255 || ls[xp] != 0)
                        goto do_not_extend;
                mp = slotlen_[*p - 128];
            do_not_extend: ;
            }
            if (mp != ml)
                m.set_slot(*p - 128, fs, mp);
            fs += mp, ls += mp;
        }
}

bool operator==(const Pattern& a, const Pattern& b) {
    return a.plen_ == b.plen_
        && a.klen_ == b.klen_
        && memcmp(a.pat_, b.pat_, Pattern::pcap) == 0
        && memcmp(a.slotlen_, b.slotlen_, slot_capacity) == 0;
}

void Join::clear() {
    npat_ = 0;
    for (int i = 0; i != pcap; ++i) {
        pat_[i].clear();
        table_[i] = 0;
    }
    for (int i = 0; i != slot_capacity; ++i) {
        slotlen_[i] = slottype_[i] = 0;
        slotname_[i] = String();
    }
    jvt_ = 0;
    maintained_ = true;
}

void Join::attach(Server& server) {
    for (int i = 0; i != npat_; ++i)
        table_[i] = &server.make_table(pat_[i].table_name());
}

void Join::set_staleness(double s) {
    if (s <= 0)
        mandatory_assert(false && "Cannot unset staleness.");
    staleness_ = tous(s);
    maintained_ = false;
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
                               Str ibegin, Str iend, SinkRange* sink) {
    SourceRange::parameters p{server, this, nsource() - 1, m,
            ibegin, iend, sink};
    if (jvt() == jvt_copy_last)
        return new CopySourceRange(p);
    else if (jvt() == jvt_count_match)
        return new CountSourceRange(p);
    else if (jvt() == jvt_min_last)
        return new MinSourceRange(p);
    else if (jvt() == jvt_max_last)
        return new MaxSourceRange(p);
    else if (jvt() == jvt_sum_match)
        return new SumSourceRange(p);
    else if (jvt() == jvt_bounded_copy_last)
        return new BoundedCopySourceRange(p);
    else if (jvt() == jvt_bounded_count_match)
        return new BoundedCountSourceRange(p);
    else
        assert(0);
}

int Join::parse_slot_name(Str word, ErrorHandler* errh) {
    const char* name = word.begin();
    const char* s = name;
    while (name != word.end() && (isalpha((unsigned char) *s) || *s == '_'))
        ++s;
    const char* nameend = s;
    int nc = -1;
    int type = 0;
    if (s != word.end() && (*s == ':' || isdigit((unsigned char) *s))) {
        if (*s == ':')
            ++s;
        if (s == word.end() || !isdigit((unsigned char) *s))
            return errh->error("syntax error in slot in %<%p{Str}%>", &word);
        for (nc = 0; s != word.end() && isdigit((unsigned char) *s); ++s)
            nc = 10 * nc + *s - '0'; // XXX overflow
        if (s != word.end() && *s == 's')
            type = stype_text, ++s;
        else if (s != word.end() && *s == 'd')
            type = stype_decimal, ++s;
        else if (s != word.end() && *s == 'n')
            type = stype_binary_number, ++s;
    }
    if (name == nameend && nc == -1)
        return errh->error("syntax error in slot in %<%p{Str}%>", &word);
    else if (s != word.end())
        return errh->error("name of slot should contain only letters and underscores in %<%p{Str}%>", &word);

    int slot;
    for (slot = 0; slot != slot_capacity && slotname_[slot]; ++slot)
        if (name != nameend
            && slotname_[slot].equals(name, nameend - name))
            break;
    if (slot == slot_capacity)
        return errh->error("too many slots in join pattern, max %d", slot_capacity);
    if (!slotname_[slot])
        slotlen_[slot] = slottype_[slot] = 0;
    if (slotlen_[slot] != 0 && nc != -1 && slotlen_[slot] != nc)
        return errh->error("slot %<%p{String}%> has inconsistent lengths (%d vs. %d)", &slotname_[slot], nc, slotlen_[slot]);
    if (name != nameend)
        slotname_[slot] = String(name, nameend);
    else
        slotname_[slot] = String(".anon") + String(slot + 1);
    if (nc != -1)
        slotlen_[slot] = nc;
    if (type != 0)
        slottype_[slot] = type;
    return slot;
}

int Join::parse_slot_names(Str word, String& out, ErrorHandler* errh) {
    StringAccum sa;
    const char* s = word.begin();
    const char* last = s;
    while (s != word.end() && *s != '<' && *s != '|')
        ++s;
    if (s == word.end())
        return errh->error("key %<%p{Str}%> lacks table name", &word);
    while (1) {
        while (s != word.end() && *s != '<')
            ++s;
        if (s == word.end())
            break;

        sa.append(last, s);
        ++s;
        const char* sbegin = s;
        while (s != word.end() && *s != '>')
            ++s;
        if (s == word.end())
            return errh->error("unterminated slot in %<%p{Str}%>", &word);

        int slot = parse_slot_name(Str(sbegin, s), errh);
        if (slot < 0)
            return -1;

        sa << (char) (128 + slot);
        ++s;
        last = s;
    }
    sa.append(last, s);
    out = sa.take_string();
    return 0;
}

int Join::hard_assign_parse(Str str, ErrorHandler* errh) {
    FileErrorHandler base_errh(stderr);
    errh = errh ? errh : &base_errh;
    clear();

    // separate input string into words
    std::vector<Str> words;
    for (auto s = str.begin(); s != str.end(); ) {
        while (s != str.end() && (isspace((unsigned char) *s) || *s == ','))
            ++s;
        const char* wstart = s;
        while (s != str.end() && !isspace((unsigned char) *s) && *s != ',')
            ++s;
        if (s != wstart)
            words.push_back(Str(wstart, s - wstart));
    }

    // basic checks
    if (words.size() < 3 || words[1] != "=")
        return errh->error("syntax error: expected %<SINK = SOURCES%>");

    // parse words into table references
    std::vector<Str> sourcestr;
    std::vector<Str> withstr;
    sourcestr.push_back(words[0]);
    Str lastsourcestr;
    jvt_ = -1;
    maintained_ = true;

    int op = -1, any_op = -1;
    for (unsigned i = 2; i != words.size(); ++i) {
        int new_op = -1;
        if (words[i] == "copy")
            new_op = jvt_copy_last;
        else if (words[i] == "min")
            new_op = jvt_min_last;
        else if (words[i] == "max")
            new_op = jvt_max_last;
        else if (words[i] == "count")
            new_op = jvt_count_match;
        else if (words[i] == "sum")
            new_op = jvt_sum_match;
        else if (words[i] == "using")
            new_op = jvt_using;
        else if (words[i] == "with" || words[i] == "where")
            new_op = jvt_slotdef;
        else if (words[i] == "pull")
            maintained_ = false;
        else if (op == jvt_slotdef || op == jvt_slotdef1) {
            withstr.push_back(words[i]);
            op = jvt_slotdef1;
        } else {
            if (op == jvt_using || op == -1)
                sourcestr.push_back(words[i]);
            else if (lastsourcestr)
                return errh->error("syntax error near %<%p{Str}%>: transformation already defined", &words[i]);
            else {
                lastsourcestr = words[i];
                jvt_ = op;
            }
            op = -1;
        }
        if (new_op != -1 && op != -1)
            return errh->error("syntax error near %<%p{Str}%>", &words[i]);
        if (new_op != -1)
            op = new_op;
        if (new_op != -1 && new_op != jvt_slotdef)
            any_op = 1;
    }
    if (op != -1 && op != jvt_slotdef1)
        return errh->error("syntax error near %<%p{Str}%>", &words.back());
    if (lastsourcestr)
        sourcestr.push_back(lastsourcestr);
    else if (any_op != -1)
        return errh->error("join pattern %<%p{Str}%> lacks source key", &str);
    else
        jvt_ = jvt_copy_last;
    if (sourcestr.size() > (unsigned) pcap)
        return errh->error("too many elements in join pattern, max %d", pcap);
    if (sourcestr.empty())
        return errh->error("no elements in join pattern, min 2");

    // parse placeholders
    std::vector<String> patstr(sourcestr.size(), String());
    for (unsigned i = 1; i != sourcestr.size(); ++i)
        if (parse_slot_names(sourcestr[i], patstr[i], errh) < 0)
            return -1;
    if (parse_slot_names(sourcestr[0], patstr[0], errh) < 0)
        return -1;

    // check that all slots have lengths
    for (unsigned i = 0; i != withstr.size(); ++i)
        if (parse_slot_name(withstr[i], errh) < 0)
            return -1;
    for (int i = 0; i != slot_capacity; ++i)
        if (slotname_[i] && slotlen_[i] == 0)
            return errh->error("slot %<%p{String}%> was not given a length", &slotname_[i]);

    // assign patterns
    for (unsigned p = 0; p != sourcestr.size(); ++p) {
        if (patstr[p].length() > Pattern::pcap)
            return errh->error("pattern %<%p{Str}%> too long, max %d chars", &sourcestr[p], Pattern::pcap);
        Pattern& pat = pat_[p];
        pat.clear();
        pat_mask_[p] = 0;
        for (int j = 0; j != patstr[p].length(); ++j) {
            unsigned char x = patstr[p][j];
            pat.pat_[pat.plen_] = x;
            ++pat.plen_;
            if (x < 128)
                ++pat.klen_;
            else {
                int s = x - 128;
                pat.slotlen_[s] = slotlen_[s];
                pat.slotpos_[s] = pat.klen_;
                pat.klen_ += slotlen_[s];
                pat_mask_[p] |= 1 << s;
            }
        }
        if (pat.klen_ > key_capacity)
            return errh->error("key in pattern %<%p{Str}%> too long, max %d chars", &sourcestr[p], key_capacity);
    }
    npat_ = sourcestr.size();

    return analyze(errh);
}

int Join::analyze(ErrorHandler* errh) {
    // account for slots across all patterns
    // completion_source_ is the source number after which sink() is complete
    int need_slots = 0;
    for (int s = 0; s != slot_capacity && slotlen_[s]; ++s)
        if (sink().has_slot(s))
            need_slots |= 1 << s;

    completion_source_ = -1;
    while (need_slots) {
        ++completion_source_;
        if (completion_source_ >= nsource()) {
            for (int s = 0; s < slot_capacity; ++s)
                if (need_slots & (1 << s))
                    errh->error("slot %<%s%> in sink not defined by sources", slotname_[s].c_str());
            return -1;
        }
        for (int s = 0; s < slot_capacity; ++s)
            if (source(completion_source_).has_slot(s)
                && source(completion_source_).slot_length(s) == sink().slot_length(s))
                need_slots &= ~(1 << s);
    }

    // create context_length_
    static_assert(key_capacity < (1U << (8 * sizeof(context_length_[0]))),
                  "key_capacity too big for context_length_ entries");
    memset(context_length_, 0, sizeof(context_length_));
    for (int m = 0; m != (1 << slot_capacity); ++m) {
        for (int s = 0; s != slot_capacity; ++s)
            if (m & (1 << s))
                context_length_[m] += slotlen_[s] + 1;
    }

    // create context_mask_
    static_assert(slot_capacity <= 8 * sizeof(pat_mask_[0]),
                  "slot_capacity too big for pat_mask_ entries");
    context_mask_[0] = 0;
    for (int p = 1; p != npat_; ++p) {
        context_mask_[p] = 0;
        for (int px = 1; px != p; ++px)
            context_mask_[p] |= pat_mask_[px];
        context_mask_[p] &= ~pat_mask_[p];
    }

    // create match_context_flags_
    for (int p = 0; p != npat_ - 1; ++p)
        match_context_flags_[p] = (p > 1 ? match_context_flags_[p - 1] : 0)
            | pat_mask_[p];
    for (int p = 0; p != npat_ - 1; ++p)
        match_context_flags_[p] &= ~pat_mask_[p];

    // create sink_key_
    sink_key_.assign_uninitialized(sink().key_length());
    uint8_t* sk = sink_key_.mutable_udata();
    for (const uint8_t* p = sink().pat_; p != sink().pat_ + sink().plen_; ++p)
        if (*p < 128) {
            *sk = *p;
            ++sk;
        } else {
            memset(sk, 'X', slotlen_[*p - 128]);
            sk += slotlen_[*p - 128];
        }

    // success
    return 0;
}

bool Join::assign_parse(Str str, ErrorHandler* errh) {
    return hard_assign_parse(str, errh) >= 0;
}

Json Join::unparse_context(Str context) const {
    Json j;
    const uint8_t* ends = context.udata() + context.length();
    for (const uint8_t* s = context.udata(); s != ends; ) {
        j.set(slotname_[*s], Str(s + 1, slotlen_[*s]));
        s += slotlen_[*s] + 1;
    }
    return j;
}

Json Join::unparse_match(const Match& m) const {
    Json j;
    for (int s = 0; s != slot_capacity; ++s)
        if (m.has_slot(s))
            j.set(slotname_[s], m.slot(s));
    return j;
}

String Join::hard_unparse_match_context(int joinpos, const Match& match) const {
    assert(joinpos >= 0 && joinpos < nsource());
    int slot_flags = match_context_flags_[joinpos + 1];
    StringAccum sa;
    for (int s = 0; slot_flags; ++s, slot_flags >>= 1)
        if (slot_flags & 1) {
            assert(match.known_length(s) == slotlen_[s]);
            sa.append(match.data(s), slotlen_[s]);
        }
    return sa.take_string();
}

void Join::hard_parse_match_context(Str context, int joinpos, Match& match) const {
    assert(joinpos >= 0 && joinpos < nsource());
    int slot_flags = match_context_flags_[joinpos + 1];
    int pos = 0;
    for (int s = 0; slot_flags; ++s, slot_flags >>= 1)
        if (slot_flags & 1) {
            assert(pos + slotlen_[s] <= context.length());
            match.set_slot(s, context.data() + pos, slotlen_[s]);
            pos += slotlen_[s];
        }
}

Json Pattern::unparse_json() const {
    Json j = Json::make_array();
    const uint8_t* p = pat_;
    while (p != pat_ + plen_) {
	if (*p >= 128) {
	    j.push_back(Json::make_array(*p - 127, slotlen_[*p - 128],
                                         slotpos_[*p - 128]));
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
