#ifndef PEQUOD_PQJOIN_HH
#define PEQUOD_PQJOIN_HH 1
#include <stdint.h>
#include <string.h>
#include "str.hh"
#include "string.hh"
#include "json.hh"
template <typename K, typename V> class HashTable;
class Json;
class String;
class ErrorHandler;

namespace pq {
class Join;
class SourceRange;
class Server;

enum { slot_capacity = 5 };

class Match {
  public:
    class state {
      private:
        uint8_t slotlen_[(slot_capacity + 3) & ~3];
        friend class Match;
    };

    inline Match();

    inline int slotlen(int i) const;
    inline const uint8_t* slot(int i) const;
    inline void set_slot(int i, const uint8_t* data, int len);
    Match& operator&=(const Match& m);

    inline const state& save() const;
    inline void restore(const state& state);

    friend std::ostream& operator<<(std::ostream&, const Match&);

  private:
    state ms_;
    const uint8_t* slot_[slot_capacity];
};

class Pattern {
  public:
    inline Pattern();

    void append_literal(uint8_t ch);
    void append_slot(int si, int len);

    inline int key_length() const;
    inline Str table_name() const;
    inline bool has_slot(int si) const;
    inline int slot_length(int si) const;

    inline bool match_complete(const Match& m) const;
    inline bool match_same(Str str, const Match& m) const;

    inline bool match(Str str) const;
    inline bool match(Str str, Match& m) const;

    inline int expand_first(uint8_t* s, const Match& m) const;
    inline String expand_first(const Match& m) const;
    inline int expand_last(uint8_t* s, const Match& m) const;
    inline String expand_last(const Match& m) const;
    inline void expand(uint8_t* s, const Match& m) const;

    bool assign_parse(Str str, HashTable<Str, int>& slotmap, ErrorHandler* errh);
    bool assign_parse(Str str, ErrorHandler* errh);

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
    jvt_bounded_copy_last, jvt_bounded_count_match
};

class Join {
  public:
    inline Join();

    inline void ref();
    inline void deref();

    inline int nsource() const;
    inline int completion_source() const;
    inline const Pattern& sink() const;
    inline const Pattern& source(int i) const;
    inline const Pattern& back_source() const;
    inline void expand(uint8_t* out, Str str) const;

    inline bool maintained() const;
    inline void set_maintained(bool m);
    inline double staleness() const;
    inline void set_staleness(double s);
    inline void set_jvt(JoinValueType jvt);
    inline JoinValueType jvt() const;
    inline void set_jvt_config(const Json& param);
    inline const Json& jvt_config() const;

    SourceRange* make_source(Server& server, const Match& m,
                             Str ibegin, Str iend);

    bool assign_parse(Str str, ErrorHandler* errh = 0);

    Json unparse_json() const;
    String unparse() const;

    bool same_structure(const Join& x) const;

  private:
    enum { pcap = 5 };
    int npat_;
    int completion_source_;
    bool maintained_;   // if the output is kept up to date with changes to the input
    double staleness_;  // validated ranges can be used in this time window.
                        // staleness_ > 0 implies maintained_ == false
    Pattern pat_[pcap];
    int refcount_;
    JoinValueType jvt_;
    Json jvtparam_;

    bool analyze(HashTable<Str, int>& slotmap, ErrorHandler* errh);
};


inline Match::Match() {
    memset(ms_.slotlen_, 0, sizeof(ms_.slotlen_));
}

inline int Match::slotlen(int i) const {
    return ms_.slotlen_[i];
}

inline const uint8_t* Match::slot(int i) const {
    return slot_[i];
}

inline void Match::set_slot(int i, const uint8_t* data, int len) {
    slot_[i] = data;
    ms_.slotlen_[i] = len;
}

inline const Match::state& Match::save() const {
    return ms_;
}

inline void Match::restore(const state& state) {
    ms_ = state;
}

inline Pattern::Pattern()
    : plen_(0), klen_(0) {
    memset(pat_, 0, sizeof(pat_));
    for (int i = 0; i < slot_capacity; ++i)
        slotlen_[i] = slotpos_[i] = 0;
}

inline int Pattern::key_length() const {
    return klen_;
}

inline Str Pattern::table_name() const {
    const uint8_t* p = pat_;
    while (p != pat_ + plen_ && *p < 128 && *p != '|')
        ++p;
    return Str(pat_, p);
}

inline bool Pattern::has_slot(int si) const {
    return slotlen_[si] != 0;
}

inline int Pattern::slot_length(int si) const {
    return slotlen_[si];
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
	    int slotlen = m.slotlen(*p - 128);
	    if (slotlen) {
		if (slotlen > ess - ss)
		    slotlen = ess - ss;
		if (memcmp(ss, m.slot(*p - 128), slotlen) != 0)
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

inline bool Pattern::match_complete(const Match& m) const {
    for (int i = 0; i != slot_capacity; ++i)
	if (slotlen_[i] && m.slotlen(i) != slotlen_[i])
	    return false;
    return true;
}

inline bool Pattern::match_same(Str s, const Match& m) const {
    if (s.length() != key_length())
        return false;
    for (int i = 0; i != slot_capacity; ++i)
        if (slotlen_[i] && (m.slotlen(i) != slotlen_[i]
                            || memcmp(s.data() + slotpos_[i], m.slot(i), slotlen_[i]) != 0))
            return false;
    return true;
}

inline int Pattern::expand_first(uint8_t* s, const Match& m) const {
    uint8_t* first = s;
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
	if (*p < 128) {
	    *s = *p;
	    ++s;
	} else {
	    int slotlen = m.slotlen(*p - 128);
	    if (slotlen) {
		memcpy(s, m.slot(*p - 128), slotlen);
		s += slotlen;
	    }
	    if (slotlen != slotlen_[*p - 128])
		break;
	}
    return s - first;
}

inline String Pattern::expand_first(const Match& m) const {
    String str = String::make_uninitialized(key_length());
    int len = expand_first(str.mutable_udata(), m);
    return str.substring(0, len);
}

inline int Pattern::expand_last(uint8_t* s, const Match& m) const {
    uint8_t* first = s;
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
	if (*p < 128) {
	    *s = *p;
	    ++s;
	} else {
	    int slotlen = m.slotlen(*p - 128);
	    if (slotlen) {
		memcpy(s, m.slot(*p - 128), slotlen);
		s += slotlen;
	    } else {
		while (s != first) {
		    ++s[-1];
		    if (s[-1] != 0)
			break;
		    --s;
		}
	    }
	    if (slotlen != slotlen_[*p - 128])
		break;
	}
    return s - first;
}

inline String Pattern::expand_last(const Match& m) const {
    String str = String::make_uninitialized(key_length());
    int len = expand_last(str.mutable_udata(), m);
    return str.substring(0, len);
}

inline void Pattern::expand(uint8_t* s, const Match& m) const {
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
	if (*p < 128) {
	    *s = *p;
	    ++s;
	} else {
	    int slotlen = m.slotlen(*p - 128);
	    if (slotlen)
		memcpy(s, m.slot(*p - 128), slotlen);
	    s += slotlen_[*p - 128];
	}
}

inline Join::Join()
    : npat_(0), maintained_(true),
      staleness_(0), refcount_(0),
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

inline void Join::set_maintained(bool m) {
    if (m && staleness_)
        mandatory_assert(false && "We do not support temporary maintenance.");
    maintained_ = m;
}

inline double Join::staleness() const {
    return staleness_;
}

inline void Join::set_jvt(JoinValueType _jvt) {
    jvt_ = _jvt;
}

inline JoinValueType Join::jvt() const {
    return jvt_;
}

inline void Join::set_jvt_config(const Json& param) {
    jvtparam_.merge(param);
}

inline const Json& Join::jvt_config() const {
    return jvtparam_;
}

inline void Join::set_staleness(double s) {
    if (!s)
        mandatory_assert(false && "Cannot unset staleness.");
    staleness_ = s;
    maintained_ = false;
}

inline const Pattern& Join::sink() const {
    return pat_[0];
}

inline const Pattern& Join::source(int i) const {
    return pat_[i + 1];
}

inline const Pattern& Join::back_source() const {
    return pat_[npat_ - 1];
}

inline void Join::expand(uint8_t* s, Str str) const {
    const Pattern& last = back_source();
    const Pattern& first = sink();
    for (const uint8_t* p = last.pat_; p != last.pat_ + last.plen_; ++p)
	if (*p >= 128 && first.has_slot(*p - 128))
	    memcpy(s + first.slotpos_[*p - 128],
		   str.udata() + last.slotpos_[*p - 128],
		   last.slotlen_[*p - 128]);
}

bool operator==(const Pattern& a, const Pattern& b);
inline bool operator!=(const Pattern& a, const Pattern& b) {
    return !(a == b);
}

std::ostream& operator<<(std::ostream&, const Match&);
std::ostream& operator<<(std::ostream&, const Join&);

} // namespace
#endif
