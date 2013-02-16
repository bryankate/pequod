#include "pqjoin.hh"
#include "hashtable.hh"
namespace pq {

bool Pattern::has_slot(int si) const {
    for (const uint8_t* p = pat_; p != pat_ + plen_; ++p)
	if (*p == 128 + si)
	    return true;
    return false;
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
    int existing_len = has_slot(si) ? slotlen_[si] : 0;
    assert(len == 0 || existing_len == 0 || len == existing_len);
    len = len == 0 ? existing_len : len;
    slotlen_[si] = len;
    pat_[plen_] = 128 + si;
    ++plen_;
    klen_ += len;
}

bool Pattern::assign_parse(Str str, HashTable<Str, int> &slotmap) {
    plen_ = klen_ = 0;
    for (auto s = str.ubegin(); s != str.uend(); ) {
	if (plen_ == pcap)
	    return false;
	if (*s != '<') {
	    append_literal(*s);
	    ++s;
	} else {
	    ++s;
	    // parse name up to length description
	    auto name0 = s;
	    while (s != str.uend() && *s != ':' && *s != '>')
		++s;
	    if (s == name0)
		return false;
	    Str name(name0, s);
	    // parse name description
	    int len = 0;
	    if (*s == ':' && (s + 1 == str.uend() || !isdigit(s[1])))
		return false;
	    else if (*s == ':') {
		len = s[1] - '0';
		for (s += 2; s != str.uend() && isdigit(*s); ++s)
		    len = 10 * len + *s - '0';
	    }
	    if (s == str.uend() || *s != '>')
		return false;
	    ++s;
	    // look up slot, maybe store it in map
	    int slot = slotmap.get(name);
	    if (slot == -1) {
		if (len == 0 || slotmap.size() == slot_capacity)
		    return false;
		slot = len + 256 * slotmap.size();
		slotmap.set(name, slot);
	    } else if (len != 0 && len != (slot & 255))
		return false;
	    // add slot
	    append_slot(slot >> 8, slot & 255);
	}
    }
    return true;
}

bool Pattern::assign_parse(Str str) {
    HashTable<Str, int> slotmap(-1);
    return assign_parse(str, slotmap);
}

bool Join::assign_parse(Str str) {
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
	    return npat_ > 1;
	if (!pat_[npat_].assign_parse(Str(pbegin, s), slotmap))
	    return false;
	++npat_;
    }
}

} // namespace
