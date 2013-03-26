#ifndef PEQUOD_PQJOIN_HH
#define PEQUOD_PQJOIN_HH 1
#include <stdint.h>
#include "pqbase.hh"
#include "str.hh"
#include "local_str.hh"
#include "json.hh"
template <typename K, typename V> class HashTable;
class Json;
class String;
class ErrorHandler;

namespace pq {
class Join;
class SourceRange;
class SinkRange;
class Server;
class Table;

enum { slot_capacity = 5 };
enum { source_capacity = 4 };

class Match {
  public:
    class state {
      private:
        uint8_t slotlen_[(slot_capacity + 3) & ~3];
        friend class Match;
    };

    inline Match();

    inline bool has_slot(int slot) const;
    inline Str slot(int slot) const;
    inline int known_length(int slot) const;
    static inline int known_length(const state& state, int slot);
    inline const uint8_t* data(int slot) const;

    inline void set_slot(int slot, const char* data, int len);
    inline void set_slot(int slot, const uint8_t* data, int len);
    inline void clear();

    inline const state& save() const;
    inline void restore(const state& state);

    friend std::ostream& operator<<(std::ostream&, const Match&);

  private:
    state ms_;
    const uint8_t* slot_[slot_capacity];
};

struct RangeMatch {
    Str first;
    Str last;
    Match match;
    int dangerous_slot;
    inline RangeMatch(Str f, Str l);
    inline RangeMatch(Str f, Str l, const Match& m, int ds = -1);
};

class Pattern {
  public:
    Pattern();

    void clear();

    inline int key_length() const;
    inline Str table_name() const;
    inline bool has_slot(int si) const;
    inline int slot_length(int si) const;
    inline int slot_position(int si) const;

    inline bool match(Str str) const;
    inline bool match(Str str, Match& m) const;
    void match_range(RangeMatch& rm) const;

    int expand(uint8_t* s, const Match& m) const;

    int check_optimized_match(const Match& m) const;
    inline void assign_optimized_match(Str str, int mopt, Match& m) const;

    Json unparse_json() const;
    String unparse() const;

    friend std::ostream& operator<<(std::ostream&, const Pattern&);

    friend bool operator==(const Pattern& a, const Pattern& b);

  private:
    enum { pcap = 12 };
    uint8_t plen_;
    uint8_t klen_;
    uint8_t pat_[pcap];
    uint8_t slotlen_[slot_capacity];
    uint8_t slotpos_[slot_capacity];

    friend class Join;
};

// every type >=jvt_min_last is aggregation.
enum JoinValueType {
    jvt_copy_last = 0, jvt_min_last, jvt_max_last,
    jvt_count_match, jvt_sum_match,
    jvt_bounded_copy_last, jvt_bounded_count_match,
    /* next ones are internal */
    jvt_using, jvt_filter, jvt_slotdef, jvt_slotdef1
};

class Join {
  public:
    inline Join();

    inline void ref();
    inline void deref();
    void clear();
    void attach(Server& server);

    inline int nsource() const;
    inline int completion_source() const;
    inline const Pattern& sink() const;
    inline const Pattern& source(int si) const;
    inline const Pattern& back_source() const;

    inline Server& server() const;

    inline int slot(Str name) const;

    inline unsigned known_mask(const Match& m) const;
    inline unsigned known_mask(const Match::state& mstate) const;
    inline unsigned source_mask(int si) const;
    inline unsigned context_mask(int si) const;
    inline int context_length(unsigned mask) const;
    inline void write_context(uint8_t* s, const Match& m, unsigned mask) const;
    template <int C>
    inline void make_context(LocalStr<C>& str, const Match& m, unsigned mask) const;
    inline void assign_context(Match& m, Str context) const;
    Json unparse_context(Str context) const;
    Json unparse_match(const Match& m) const;

    inline bool source_is_filter(int si) const;

    inline void expand_sink_key_context(Str context) const;
    inline void expand_sink_key_source(Str source_key, unsigned sink_mask) const;
    inline Str sink_key() const;

    bool check_increasing_match(int si, const Match& m) const;

    int expand_first(uint8_t* buf, const Pattern& pat, const RangeMatch& rm) const;
    String expand_first(const Pattern& pat, const RangeMatch& rm) const;
    int expand_last(uint8_t* buf, const Pattern& pat, const RangeMatch& rm) const;
    String expand_last(const Pattern& pat, const RangeMatch& rm) const;

    inline bool maintained() const;
    inline uint64_t staleness() const;
    void set_staleness(double sec);
    inline JoinValueType jvt() const;
    inline const Json& jvt_config() const;

    SourceRange* make_source(Server& server, const Match& m,
                             Str ibegin, Str iend, SinkRange* sink);

    bool assign_parse(Str str, ErrorHandler* errh = 0);

    Json unparse_json() const;
    String unparse() const;

    bool same_structure(const Join& x) const;

  private:
    enum { pcap = source_capacity + 1 };
    int npat_;
    int completion_source_;
    uint64_t staleness_;  // validated ranges can be used in this time window.
                        // staleness_ > 0 implies maintained_ == false
    bool maintained_;   // if the output is kept up to date with changes to the input
    uint8_t filters_;
    uint8_t slotlen_[slot_capacity];
    uint8_t pat_mask_[pcap];
    Server* server_;
    Pattern pat_[pcap];
    uint8_t context_mask_[pcap];
    uint8_t context_length_[1 << slot_capacity];
    mutable LocalStr<24> sink_key_;

    enum { stype_unknown = 0, stype_text, stype_decimal, stype_binary_number };
    String slotname_[slot_capacity];
    uint8_t slottype_[slot_capacity];
    int refcount_;
    int jvt_;
    Json jvtparam_;

    int parse_slot_name(Str word, ErrorHandler* errh);
    int parse_slot_names(Str word, String& out, ErrorHandler* errh);
    int hard_assign_parse(Str str, ErrorHandler* errh);
    int analyze(ErrorHandler* errh);
};


inline Match::Match() {
    clear();
}

inline void Match::clear() {
    memset(ms_.slotlen_, 0, sizeof(ms_.slotlen_));
}

inline bool Match::has_slot(int i) const {
    return ms_.slotlen_[i];
}

inline Str Match::slot(int i) const {
    return Str(slot_[i], ms_.slotlen_[i]);
}

inline int Match::known_length(int i) const {
    return ms_.slotlen_[i];
}

inline int Match::known_length(const state& state, int i) {
    return state.slotlen_[i];
}

inline const uint8_t* Match::data(int i) const {
    return slot_[i];
}

inline void Match::set_slot(int i, const uint8_t* data, int len) {
    slot_[i] = data;
    ms_.slotlen_[i] = len;
}

inline void Match::set_slot(int i, const char* data, int len) {
    set_slot(i, reinterpret_cast<const uint8_t*>(data), len);
}

inline const Match::state& Match::save() const {
    return ms_;
}

inline void Match::restore(const state& state) {
    ms_ = state;
}

inline RangeMatch::RangeMatch(Str f, Str l)
    : first(f), last(l), dangerous_slot(-1) {
}

inline RangeMatch::RangeMatch(Str f, Str l, const Match& m, int ds)
    : first(f), last(l), match(m), dangerous_slot(ds) {
}

inline int Pattern::key_length() const {
    return klen_;
}

inline Str Pattern::table_name() const {
    const uint8_t* p = pat_;
    for (; p != pat_ + plen_ && *p < 128; ++p)
        if (*p == '|')
            return Str(pat_, p);
    return Str();
}

/** @brief Return true iff this pattern has @a slot. */
inline bool Pattern::has_slot(int slot) const {
    return slotlen_[slot] != 0;
}

/** @brief Return the byte length of @a slot.

    Returns 0 if this pattern does not have @a slot. */
inline int Pattern::slot_length(int slot) const {
    return slotlen_[slot];
}

/** @brief Returns the first character position of @a slot in this pattern.

    Returns 0 if this pattern does not have @a slot. */
inline int Pattern::slot_position(int slot) const {
    return slotpos_[slot];
}

inline bool Pattern::match(Str str) const {
    if (str.length() != key_length())
	return false;
    const uint8_t* ss = str.udata(), *ess = str.udata() + str.length();
    for (const uint8_t* p = pat_; p != pat_ + plen_ && ss != ess; ++p)
	if (*p < 128) {
	    if (*ss != *p)
		return false;
	    ++ss;
	} else
	    ss += slotlen_[*p - 128];
    return true;
}

inline bool Pattern::match(Str s, Match& m) const {
    const uint8_t* ss = s.udata(), *ess = s.udata() + s.length();
    for (const uint8_t* p = pat_; p != pat_ + plen_ && ss != ess; ++p)
	if (*p < 128) {
	    if (*ss != *p)
		return false;
	    ++ss;
	} else {
	    int slotlen = m.known_length(*p - 128);
	    if (slotlen) {
		if (slotlen > ess - ss)
		    slotlen = ess - ss;
		if (memcmp(ss, m.data(*p - 128), slotlen) != 0)
		    return false;
	    }
	    if (slotlen < slotlen_[*p - 128] && slotlen < ess - ss) {
		slotlen = slotlen_[*p - 128];
		if (slotlen > ess - ss)
		    slotlen = ess - ss;
		m.set_slot(*p - 128, ss, slotlen);
	    }
	    ss += slotlen;
	}
    return true;
}

inline void Pattern::assign_optimized_match(Str str, int mopt, Match& m) const {
    for (int i = 0; mopt; ++i, mopt >>= 1)
        if (mopt & 1)
            m.set_slot(i, str.udata() + slotpos_[i], slotlen_[i]);
}

inline Join::Join()
    : npat_(0), staleness_(0), maintained_(true), refcount_(0),
      jvt_(jvt_copy_last), jvtparam_() {
}

inline void Join::ref() {
    ++refcount_;
}

inline void Join::deref() {
    if (--refcount_ == 0)
	delete this;
}

inline int Join::nsource() const {
    return npat_ - 1;
}

inline int Join::completion_source() const {
    return completion_source_;
}

inline bool Join::maintained() const {
    return maintained_;
}

inline uint64_t Join::staleness() const {
    return staleness_;
}

inline JoinValueType Join::jvt() const {
    return (JoinValueType) jvt_;
}

inline const Json& Join::jvt_config() const {
    return jvtparam_;
}

inline const Pattern& Join::sink() const {
    return pat_[0];
}

inline const Pattern& Join::source(int si) const {
    return pat_[si + 1];
}

inline const Pattern& Join::back_source() const {
    return pat_[npat_ - 1];
}

inline Server& Join::server() const {
    return *server_;
}

inline bool Join::source_is_filter(int si) const {
    return filters_ & (1 << si);
}

inline int Join::slot(Str name) const {
    for (int s = 0; s != slot_capacity; ++s)
        if (slotname_[s] == name)
            return s;
    return -1;
}

inline unsigned Join::known_mask(const Match& m) const {
    unsigned mask = 0;
    for (int s = 0; s != slot_capacity && slotlen_[s]; ++s)
        if (m.known_length(s) == slotlen_[s])
            mask |= 1 << s;
    return mask;
}

inline unsigned Join::known_mask(const Match::state& mstate) const {
    unsigned mask = 0;
    for (int s = 0; s != slot_capacity && slotlen_[s]; ++s)
        if (Match::known_length(mstate, s) == slotlen_[s])
            mask |= 1 << s;
    return mask;
}

inline unsigned Join::source_mask(int si) const {
    return pat_mask_[si + 1];
}

inline unsigned Join::context_mask(int si) const {
    return context_mask_[si + 1];
}

inline int Join::context_length(unsigned mask) const {
    assert(mask < (1 << slot_capacity));
    return context_length_[mask];
}

inline void Join::write_context(uint8_t* s, const Match& m, unsigned mask) const {
    for (int i = 0; mask; mask >>= 1, ++i)
        if (mask & 1) {
            assert(m.known_length(i) == slotlen_[i]);
            *s++ = i;
            memcpy(s, m.data(i), slotlen_[i]);
            s += slotlen_[i];
        }
}

template <int C>
inline void Join::make_context(LocalStr<C>& str, const Match& m, unsigned mask) const {
    str.assign_uninitialized(context_length(mask));
    write_context(str.mutable_udata(), m, mask);
}

inline void Join::assign_context(Match& m, Str context) const {
    const uint8_t* ends = context.udata() + context.length();
    for (const uint8_t* s = context.udata(); s != ends; ) {
        m.set_slot(*s, s + 1, slotlen_[*s]);
        s += slotlen_[*s] + 1;
    }
}

inline void Join::expand_sink_key_context(Str context) const {
    const uint8_t* ends = context.udata() + context.length();
    for (const uint8_t* s = context.udata(); s != ends; ) {
        if (pat_[0].has_slot(*s))
            memcpy(sink_key_.mutable_data() + pat_[0].slot_position(*s),
                   s + 1, slotlen_[*s]);
        s += slotlen_[*s] + 1;
    }
}

inline void Join::expand_sink_key_source(Str source_key, unsigned mask) const {
    mask = pat_mask_[0] & pat_mask_[npat_ - 1] & ~mask;
    for (int s = 0; mask; mask >>= 1, ++s)
        if (mask & 1)
            memcpy(sink_key_.mutable_data() + pat_[0].slot_position(s),
                   source_key.data() + pat_[npat_ - 1].slot_position(s),
                   slotlen_[s]);
}

inline Str Join::sink_key() const {
    return sink_key_;
}

bool operator==(const Pattern& a, const Pattern& b);
inline bool operator!=(const Pattern& a, const Pattern& b) {
    return !(a == b);
}

std::ostream& operator<<(std::ostream&, const Match&);
std::ostream& operator<<(std::ostream&, const Join&);

} // namespace
#endif
