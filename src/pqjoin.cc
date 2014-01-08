#include "pqjoin.hh"
#include "pqserver.hh"
#include "hashtable.hh"
#include "json.hh"
#include "time.hh"
#include "error.hh"
namespace pq {

bool Join::allow_subtables = true;

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

void Pattern::match_range(RangeMatch& rm) const {
    const uint8_t* fs = rm.first.udata(), *ls = rm.last.udata();
    const uint8_t* efs = fs + std::min(rm.first.length(), rm.last.length());
    for (const uint8_t* p = pat_; p != pat_ + plen_ && fs != efs; ++p)
        if (*p < 128) {
            if (*fs != *p || *ls != *p)
                goto done;
            ++fs, ++ls;
        } else {
            int slot = *p - 128;
            const uint8_t* fs1 = fs;
            const uint8_t* ms = rm.match.data(slot);
            int ml = rm.match.known_length(slot);
            int mp = 0;
            while (mp != ml) {
                if (fs == efs || *fs != ms[mp] || *ls != ms[mp])
                    goto done;
                ++mp, ++fs, ++ls;
            }
            while (mp != slotlen_[slot] && fs != efs && *fs == *ls)
                ++mp, ++fs, ++ls;
            if (mp != slotlen_[slot] && fs1 + slotlen_[slot] == efs
                && *fs + 1 == *ls) {
                int mpleft = slotlen_[slot] - mp;
                for (int xp = 1; xp != mpleft; ++xp)
                    if (fs[xp] != 255 || ls[xp] != 0)
                        goto do_not_extend;
                fs += mpleft, ls += mpleft, mp += mpleft;
            }
        do_not_extend:
            if (mp != ml)
                rm.match.set_slot(slot, fs1, mp);
        }
 done:
    rm.dangerous_slot = -1;
    int dangerous_slot = -1;
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
        if (*p >= 128) {
            int slot = *p - 128;
            if (dangerous_slot < 0) {
                if (rm.match.known_length(slot) != slotlen_[slot])
                    dangerous_slot = slot;
            } else if (rm.first.length() > slotpos_[slot]
                       || rm.last.length() > slotpos_[slot])
                rm.dangerous_slot = dangerous_slot;
        }
}

int Pattern::expand(uint8_t* s, const Match& m) const {
    uint8_t* first = s;
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
        if (*p < 128) {
            *s = *p;
            ++s;
        } else {
            memcpy(s, m.data(*p - 128), m.known_length(*p - 128));
            s += m.known_length(*p - 128);
            if (m.known_length(*p - 128) != slotlen_[*p - 128])
                break;
        }
    return s - first;
}

int Pattern::check_optimized_match(const Match& m) const {
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
        if (*p >= 128 && m.known_length(*p - 128) != slotlen_[*p - 128]) {
            if (p + 1 != pat_ + plen_)
                return -1;
            else
                return 1 << (*p - 128);
        }
    return 0;
}

bool Join::check_increasing_match(int si, const Match& m) const {
    // Are all keys matching pattern source(si + 1) at match m
    // less than all keys matching pattern source(si + 1) at a match m'
    // which was obtained from later keys in source(si)?

    assert(si >= 0 && si < npat_ - 1);
    if (si == npat_ - 2)
        return false;
    unsigned prev_changing_slots = pat_mask_[si + 1] & pat_mask_[si + 2]
        & ~known_mask(m);
    if (prev_changing_slots == 0)
        return false;
    const Pattern& next = pat_[si + 2];
    for (const uint8_t* p = next.pat_; p != next.pat_ + next.plen_; ++p)
        if (*p >= 128
            && m.known_length(*p - 128) != slotlen_[*p - 128]
            && (prev_changing_slots & (1 << (*p - 128))))
            return true;
    return true;
}

bool operator==(const Pattern& a, const Pattern& b) {
    return a.plen_ == b.plen_
        && a.klen_ == b.klen_
        && memcmp(a.pat_, b.pat_, Pattern::pcap) == 0
        && memcmp(a.slotlen_, b.slotlen_, slot_capacity) == 0;
}

void Join::clear() {
    npat_ = 0;
    server_ = nullptr;
    for (int i = 0; i != pcap; ++i)
        pat_[i].clear();
    for (int i = 0; i != slot_capacity; ++i) {
        slotlen_[i] = slottype_[i] = 0;
        slotname_[i] = String();
    }
    jvt_ = 0;
    maintained_ = true;
    filters_ = 0;
}

void Join::attach(Server& server) {
    server_ = &server;
}

void Join::set_staleness(double s) {
    if (s <= 0)
        mandatory_assert(false && "Cannot unset staleness.");
    staleness_ = tous(s);
    maintained_ = false;
}

/** Expand @a pat into the first matching string that matches @a rm. */
int Join::expand_first(uint8_t* buf, const Pattern& pat,
                       const RangeMatch& rm) const {
    const Pattern& sinkpat = sink();
    uint8_t* os = buf;
    Str sink_first = rm.first;
    if (rm.dangerous_slot >= 0 && rm.match.known_length(rm.dangerous_slot)) {
        int sp = sinkpat.slotpos_[rm.dangerous_slot];
        int x = std::min(rm.match.known_length(rm.dangerous_slot), sink_first.length() - sp);
        if (x > 0 && memcmp(rm.match.data(rm.dangerous_slot), sink_first.data() + sp, x) != 0)
            sink_first = sink_first.prefix(sp);
    }

    for (const uint8_t* ps = pat.pat_; ps != pat.pat_ + pat.plen_; ++ps)
        if (*ps < 128) {
            *os = *ps;
            ++os;
        } else {
            int slot = *ps - 128;

            // search for slot values in match and range
            int matchlen = rm.match.known_length(slot);
            int sinkpos = 0, sinklen = 0;
            if (sinkpat.has_slot(slot)) {
                sinkpos = sinkpat.slot_position(slot);
                sinklen = std::min(std::max(sink_first.length() - sinkpos, 0), sinkpat.slot_length(slot));
            }
            if (matchlen == 0 && sinklen == 0)
                break;

            if (matchlen > 0 && sinklen > 0) {
                int cmp = String_generic::compare(rm.match.data(slot), matchlen, sink_first.udata() + sinkpos, sinklen);
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
                data = rm.match.data(slot);
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

String Join::expand_first(const Pattern& pat, const RangeMatch& rm) const {
    StringAccum sa(pat.key_length());
    sa.adjust_length(expand_first(sa.udata(), pat, rm));
    return sa.take_string();
}

/** Expand @a pat into an upper bound for strings that match @a match and
    could affect the sink range [@a sink_first, @a sink_last). */
int Join::expand_last(uint8_t* buf, const Pattern& pat,
                      const RangeMatch& rm) const {
    const Pattern& sinkpat = sink();
    uint8_t* os = buf;
    int last_position = 0;
    Str sink_last = rm.last;
    if (rm.dangerous_slot >= 0 && rm.match.known_length(rm.dangerous_slot)) {
        int sp = sinkpat.slotpos_[rm.dangerous_slot];
        int x = std::min(rm.match.known_length(rm.dangerous_slot), sink_last.length() - sp);
        if (x > 0 && memcmp(rm.match.data(rm.dangerous_slot), sink_last.data() + sp, x) != 0)
            sink_last = sink_last.prefix(sp);
    }

    for (const uint8_t* ps = pat.pat_; ps != pat.pat_ + pat.plen_; ++ps)
        if (*ps < 128) {
            *os = *ps;
            ++os;
        } else {
            int slot = *ps - 128;

            // search for slot in match and range
            int matchlen = rm.match.known_length(slot);
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
            if (sinklen == sinkpat.slot_length(slot)
                && matchlen < sinklen
                && rm.first.length() >= sinkpos + sinklen) {
                if (memcmp(&rm.first[sinkpos], &sink_last[sinkpos], sinklen) == 0)
                    use_first = true;
                else if (sink_last.length() == sinkpos + sinklen) {
                    const uint8_t* ldata = sink_last.udata() + sinkpos;
                    const uint8_t* fdata = rm.first.udata() + sinkpos;
                    int x = sinklen - 1;
                    while (x >= 0 && ldata[x] == 0 && fdata[x] == 255)
                        --x;
                    use_first = x >= 0 && ldata[x] == fdata[x] + 1
                        && memcmp(ldata, fdata, x) == 0;
                }
            }

            const uint8_t* data;
            if (matchlen >= sinklen)
                data = rm.match.data(slot);
            else if (use_first) {
                data = rm.first.udata() + sinkpos;
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

String Join::expand_last(const Pattern& pat, const RangeMatch& rm) const {
    StringAccum sa(pat.key_length());
    sa.adjust_length(expand_last(sa.udata(), pat, rm));
    return sa.take_string();
}

int Join::pattern_subtable_length(int i) const {
    if (!allow_subtables)
        return 0;
    int cut = 0, last_cut = 0;
    const Pattern& pat = pat_[i];
    for (const uint8_t* p = pat.pat_; p != pat.pat_ + pat.plen_; ++p)
        if (*p < 128)
            ++cut;
        else if ((slottype_[*p - 128] & stype_subtables)
                 && p + 1 != pat.pat_ + pat.plen_) {
            cut += slotlen_[*p - 128];
            last_cut = cut;
        } else
            break;
    return last_cut;
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
        for (; s != word.end(); ++s) {
            if (*s == 's')
                type = (type & ~stype_type_mask) | stype_text;
            else if (*s == 'd')
                type = (type & ~stype_type_mask) | stype_decimal;
            else if (*s == 'n')
                type = (type & ~stype_type_mask) | stype_binary_number;
            else if (*s == 't')
                type |= stype_subtables;
            else
                break;
        }
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
        else if (words[i] == "using") {
            if (op != jvt_filter)
                new_op = jvt_using;
        } else if (words[i] == "filter") {
            if (op == jvt_using)
                op = -1;
            new_op = jvt_filter;
        } else if (words[i] == "with" || words[i] == "where")
            new_op = jvt_slotdef;
        else if (words[i] == "pull")
            maintained_ = false;
        else if (words[i] == "push")
            maintained_ = true;
        else if (words[i] == "and")
            /* do nothing */;
        else if (op == jvt_slotdef || op == jvt_slotdef1) {
            withstr.push_back(words[i]);
            op = jvt_slotdef1;
        } else {
            if (op == jvt_filter) {
                filters_ |= 1 << (sourcestr.size() - 1);
                sourcestr.push_back(words[i]);
            } else if (op == jvt_using || op == -1)
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
    // check that sink() is not used as a source
    for (int p = 1; p != npat_; ++p)
        if (pat_[p].table_name() == pat_[0].table_name())
            return errh->error("table %<%.*s%> used as both source and sink",
                               pat_[p].table_name().length(), pat_[p].table_name().data());

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

Json Pattern::unparse_json() const {
    Json j = Json::make_array();
    const uint8_t* p = pat_;
    while (p != pat_ + plen_) {
	if (*p >= 128) {
	    j.push_back(Json::array(*p - 127, slotlen_[*p - 128],
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
