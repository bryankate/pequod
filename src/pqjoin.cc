#include "pqjoin.hh"
#include "hashtable.hh"
#include "json.hh"
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
    assert(!has_slot(si));
    assert(len != 0);
    slotlen_[si] = len;
    slotpos_[si] = klen_;
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

JoinState::JoinState(Join* join)
    : join_(join), joinpos_(0) {
    join_->ref();
    for (int i = 0; i < slot_capacity; ++i)
	slotpos_[i] = slotlen_[i] = 0;
}

JoinState::JoinState(Join* join, const Match& m)
    : join_(join), joinpos_(1) {
    join_->ref();
    assert(join_->size() > joinpos_ + 1);

    int pos = 0;
    for (int i = 0; i < slot_capacity; ++i) {
	slotpos_[i] = pos;
	if ((slotlen_[i] = m.slotlen(i)))
	    memcpy(&slots_[pos], m.slot(i), slotlen_[i]);
	pos += slotlen_[i];
    }
}

JoinState* Join::make_state(const Match& m) const {
    int pos = 0;
    for (int i = 0; i < slot_capacity; ++i)
	pos += m.slotlen(i);
    char* js = new char[sizeof(JoinState) + ((pos + 7) & ~7)];
    return new(js) JoinState(const_cast<Join*>(this), m);
}

JoinState::JoinState(const JoinState* js, const Match& m)
    : join_(js->join_), joinpos_(js->joinpos_ + 1) {
    assert(join_->size() > joinpos_ + 1);
    join_->ref();

    int pos = 0;
    for (int i = 0; i < slot_capacity; ++i) {
	slotpos_[i] = pos;
	if ((slotlen_[i] = m.slotlen(i)))
	    memcpy(&slots_[pos], m.slot(i), slotlen_[i]);
	else if ((slotlen_[i] = js->slotlen(i)))
	    memcpy(&slots_[pos], js->slot(i), slotlen_[i]);
	pos += slotlen_[i];
    }
}

JoinState* JoinState::make_state(const Match& m) const {
    int pos = 0;
    for (int i = 0; i < slot_capacity; ++i)
	pos += (m.slotlen(i) ? m.slotlen(i) : slotlen_[i]);
    char* js = new char[sizeof(JoinState) + ((pos + 7) & ~7)];
    return new(js) JoinState(this, m);
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

Json Join::unparse_json() const {
    Json j = Json::make_array();
    for (int i = 0; i < npat_; ++i)
	j.push_back(pat_[i].unparse_json());
    return j;
}

String Join::unparse() const {
    return unparse_json().unparse();
}

} // namespace
