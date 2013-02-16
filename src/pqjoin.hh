#ifndef PEQUOD_PQJOIN_HH
#define PEQUOD_PQJOIN_HH 1
#include <stdint.h>
#include <string.h>
#include "str.hh"
template <typename K, typename V> class HashTable;

namespace pq {

enum { slot_capacity = 5 };

class Match {
  public:
    Match() {
	memset(slotlen_, 0, sizeof(slotlen_));
    }

    int slotlen(int i) const {
	return slotlen_[i];
    }
    const uint8_t* slot(int i) const {
	return slot_[i];
    }
    void set_slot(int i, const uint8_t* data, int len) {
	slot_[i] = data;
	slotlen_[i] = len;
    }

  private:
    uint8_t slotlen_[(slot_capacity + 3) & ~3];
    const uint8_t* slot_[slot_capacity];
};

class Pattern {
  public:
    Pattern()
	: plen_(0), klen_(0) {
    }

    void append_literal(uint8_t ch);
    void append_slot(int si, int len);

    inline int length() const;
    bool has_slot(int si) const;

    inline bool match(Str s, Match& m) const;
    inline bool match_complete(const Match& m) const;
    inline int first(uint8_t* s, const Match& m) const;
    inline int last(uint8_t* s, const Match& m) const;

    bool assign_parse(Str str, HashTable<Str, int> &slotmap);
    bool assign_parse(Str str);

  private:
    enum { pcap = 9 };
    uint8_t plen_;
    uint8_t klen_;
    uint8_t pat_[pcap];
    uint8_t slotlen_[slot_capacity];
};

class Join {
  public:
    Join()
	: npat_(0) {
    }

    bool assign_parse(Str str);

  private:
    enum { pcap = 5 };
    int npat_;
    Pattern pat_[pcap];
};

inline int Pattern::length() const {
    return klen_;
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
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
	if (*p >= 128 && m.slotlen(*p - 128) != slotlen_[*p - 128])
	    return false;
    return true;
}

inline int Pattern::first(uint8_t* s, const Match& m) const {
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

inline int Pattern::last(uint8_t* s, const Match& m) const {
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

} // namespace
#endif
