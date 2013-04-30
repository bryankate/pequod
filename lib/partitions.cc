#include "partitions.hh"
#include "compiler.hh"
#include "straccum.hh"
#include "json.hh"
#include <assert.h>
#include <algorithm>
#include <utility>
#include <iostream>

using std::vector;
using std::sort;
using std::pair;
using std::make_pair;
using std::cout;
using std::endl;


namespace pq {

static const uint32_t powersof10[] = { 1, 10, 100, 1000, 10000, 100000,
				       1000000, 10000000, 100000000,
                                       1000000000 };

partition1::partition1(Str prefix, int server)
    : prefix_len_(prefix.length()),
      type_((partition_type) (binary_div | default_flag)),
      server_offset_(server), nservers_(1) {
    mandatory_assert(prefix.length() <= (int) sizeof(prefix_));
    memcpy(prefix_, prefix.data(), prefix.length());
}

partition1::partition1(Str prefix, partition_type type, int digits,
                       int server_offset, int nservers)
    : prefix_len_(prefix.length()), type_(type), digits_(digits),
      server_offset_(server_offset), nservers_(nservers < 1 ? 1 : nservers) {
    mandatory_assert(prefix.length() <= (int) sizeof(prefix_));
    memcpy(prefix_, prefix.data(), prefix.length());
    mandatory_assert(digits_ >= 0);

    switch (type_ & type_mask) {
    case decimal:
        mandatory_assert(digits_ < 10);
        digits_npos_ = powersof10[digits_];
        break;
    case binary:
        mandatory_assert(digits_ < 32);
        digits_npos_ = 1U << digits_;
        break;
    case text:
        digits_npos_ = 256;
        break;
    }

    if (digits_ == 0 || nservers_ <= 1) {
        type_ = binary;
        digits_npos_ = 1;
        nservers_ = 1;
    }
}

partition1 partition1::next_partition(int default_server) const {
    char buf[sizeof(prefix_)];
    int len = prefix_len_;
    memcpy(buf, prefix_, len);
    while (len > 0 && ++buf[len - 1] == 0)
        --len;
    return partition1(Str(buf, len), default_server);
}

uint32_t partition1::position(Str s) const {
    if (nservers_ <= 1 || digits_ == 0)
	return 0;

    const char *data = s.data();
    int len = s.length();
    uint32_t x = 0;
    if ((type_ & type_mask) == text) {
	if (len >= prefix_len_ + 1)
	    x = len <= prefix_len_ ? 0 : (unsigned char) data[prefix_len_];
    } else if ((type_ & type_mask) == binary) {
	if (len >= prefix_len_ + 4)
	    x = ntohl(*reinterpret_cast<const uint32_t *>(data + prefix_len_))
		>> (32 - digits_);
    } else {
	if (len >= prefix_len_ + digits_) {
	    const char *edata = data + prefix_len_ + digits_;
	    for (data += prefix_len_;
		 data != edata && likely(*data >= '0' && *data <= '9');
		 ++data)
		x = x * 10 + *data - '0';

	    // must be careful with unexpected keys to preserve order property
	    if (likely(data == edata))
		/* do nothing */;
	    else if ((unsigned char) *data > '9') {
		for (; data != edata; ++data)
		    x = x * 10 + 9;
	    } else if (x != 0) {
		for (; data != edata; ++data)
		    x = x * 10;
		--x;
	    }
	}
    }

    if (type_ & div)
	return (uint32_t) ((double) (x * nservers_) / digits_npos_);
    else
	return x;
}

int partition1::emit_pos(char *buf, uint32_t pp) const {
    if (pp == 0)
	return 0;
    else if (pp >= npos()) {
	++buf[-1];
	return 0;
    } else {
	if (type_ & div)
	    pp = (uint32_t) ((double) (pp * digits_npos_) / nservers_);
	if ((type_ & type_mask) == text) {
	    buf[0] = pp;
	    return 1;
	} else if ((type_ & type_mask) == binary) {
	    *reinterpret_cast<uint32_t *>(buf) = htonl(pp << (32 - digits_));
	    return 4;
	} else {
	    char *x = buf + digits_;
	    uint32_t v = pp;
	    for (int i = 0; i != digits_; ++i) {
		--x;
		*x = v % 10 + '0';
		v /= 10;
	    }
	    return digits_;
	}
    }
}

static const char * const type_names[] = {
    "decimal", "binary", "text", 0,
    "decimal_div", "binary_div", "text_div", 0
};

Json partition1::unparse_json() const {
    Json j;
    j.set("prefix", prefix());
    if (type_ & default_flag)
	j.set("is_default", true);
    else
	j.set("type", type_names[(int) type_]).set("digits", digits_);
    if (nservers() == 1)
	j.set("server", first_server());
    else
	j.set("server", Json::make_array(first_server(), last_server()));
    return j;
}

partition1 partition1::parse_json(const Json &j) {
    int first_server, last_server;
    String type;
    if (!j["prefix"].is_s())
	goto error;
    if (j["server"].is_i()) {
	first_server = j["server"].to_i();
	last_server = first_server + 1;
    } else if (j["server"].is_a() && j["server"].size() == 2
	       && j["server"][0].is_i() && j["server"][1].is_i()) {
	first_server = j["server"][0].to_i();
	last_server = j["server"][1].to_i();
    } else
	goto error;
    if (j.get("is_default"))
	return partition1(j.get("prefix").to_s(), first_server);
    type = j.get("type").to_s();
    for (int i = 0; i < 7; ++i)
	if (type_names[i] && type == type_names[i])
	    return partition1(j.get("prefix").to_s(),
			      (partition_type) i,
			      j.get("digits").to_i(),
			      first_server, last_server - first_server);
 error:
    return partition1("", -1);
}


partition_set::partition_set(int default_server)
    : default_server_(default_server) {
    p_.push_back(partition1(Str(), default_server));
}

partition_set::partition_set(int default_server, const Json &j)
    : default_server_(default_server) {
    p_.push_back(partition1(Str(), default_server));
    if (j.is_a())
	for (Json::size_type i = 0; i != j.size(); ++i) {
	    partition1 p = partition1::parse_json(j[i]);
	    if (!p.is_default())
		add(p);
	}
}

void partition_set::add(const partition1 &p) {
    auto it = std::lower_bound(p_.begin(), p_.end(), p);
    if (it != p_.end() && it->prefix() == p.prefix()) {
        // XXX warning?
        *it = p;
    } else
        it = p_.insert(it, p);

    partition1 next = p.next_partition(default_server_);
    ++it;
    while (it != p_.end() && *it < next)
        it = p_.erase(it);
    if (it == p_.end() || it->prefix() != next.prefix())
        p_.insert(it, next);

    // now set fingers
    unsigned i = 0;
    int ch = first_finger;
    while (i != p_.size() - 1 && i < 255 && ch != first_finger + nfingers)
        if ((unsigned char) p_[i+1].prefix()[0] < (unsigned) ch)
            ++i;
        else {
            finger_[ch - first_finger] = i;
            ++ch;
        }
    while (ch != first_finger + nfingers) {
        finger_[ch - first_finger] = i;
        ++ch;
    }
}

partition_iterator partition_set::find(Str s) const {
    int pi_idx = 0;
    unsigned char s0 = (unsigned char) s[0];
    if (s.length() && (unsigned) s0 - first_finger < (unsigned) nfingers)
        pi_idx = finger_[(unsigned) s0 - first_finger];
    partition_iterator pi(this, p_.data() + pi_idx);
    const partition1 *ep1 = end_p1();
    while (pi.p1_ + 1 != ep1 && s >= pi.p1_[1].prefix())
        pi.advance_group();
    pi.pp_ = pi.p1_->position(s);
    return pi;
}

void partition_set::analyze(const String &first, const String &last,
                            unsigned limit,
                            std::vector<keyrange> &result) const {
    const partition1 *ep1 = end_p1();
    result.clear();

    partition_iterator pi = find(first);
    result.push_back(keyrange(first, pi.server()));

    // remaining partitions
    while (!limit || result.size() != limit) {
	++pi;
	if (pi.p1_ == ep1 || pi.key() >= last)
	    break;
	int server = pi.server();
	if (server != result.back().owner)
	    result.push_back(keyrange(pi.key(), server));
    }
}


Str partition_iterator::key() const {
    if (buflen_ == -2) {
        if (p1_ == ps_->end_p1())
            return Str::maxkey;
        memcpy(buf_, p1_->prefix().data(), p1_->prefix().length());
    }
    if (buflen_ < 0) {
        buflen_ = p1_->prefix().length();
        buflen_ += p1_->emit_pos(buf_ + buflen_, pp_);
    }
    return Str(buf_, buflen_);
}


void partition_set::print_fail(int line, const std::vector<keyrange> &kr) const {
    fprintf(stderr, "Assertion failed: partitions.cc, line %d: %u elements\n", line,
	    (unsigned) kr.size());
    for (std::vector<keyrange>::size_type i = 0; i != kr.size(); ++i)
	fprintf(stderr, "  [%u] = \"%.*s\" -> %u\n", (unsigned) i,
		kr[i].key.length(), kr[i].key.data(), kr[i].owner);
    fprintf(stderr, "\n");
    int itn = 0;
    for (auto it = begin(); it != end(); it.advance_group(), ++itn)
        fprintf(stderr, "  %d \"%.*s\"\n", itn, it.key().length(), it.key().data());
    for (int ch = first_finger; ch != first_finger + nfingers; ++ch)
        fprintf(stderr, "  @%c %d", ch, finger_[ch - first_finger]);
    fprintf(stderr, "\n  <<%s>>\n", unparse().c_str());
    assert(0);
}

void partition_set::test() {
    partition_set ps(39);
    ps.add(partition1("a|", partition1::decimal, 1, 100, 2));
    ps.add(partition1("bc|", partition1::decimal, 1, 103, 3));
    ps.add(partition1("b|", partition1::decimal, 1, 105, 3));
    ps.add(partition1("d|", partition1::decimal_div, 3, 200, 5));
    ps.add(partition1("fa", partition1::decimal_div, 1, 300, 2));
    ps.add(partition1("fb", partition1::decimal_div, 1, 350, 2));

    std::vector<keyrange> kr;

    ps.analyze("a", "a|1", 0, kr);
    if (!(kr.size() == 2
	  && kr[0].key == "a" && kr[0].owner == 39
	  && kr[1].key == "a|" && kr[1].owner == 100))
	ps.print_fail(__LINE__, kr);

    ps.analyze("a", "a|1b", 0, kr);
    if (!(kr.size() == 3
	  && kr[0].key == "a" && kr[0].owner == 39
	  && kr[1].key == "a|" && kr[1].owner == 100
	  && kr[2].key == "a|1" && kr[2].owner == 101))
	ps.print_fail(__LINE__, kr);

    ps.analyze("a", "b", 0, kr);
    if (!(kr.size() == 12
	  && kr[0].key == "a" && kr[0].owner == 39
	  && kr[1].key == "a|" && kr[1].owner == 100
	  && kr[2].key == "a|1" && kr[2].owner == 101
	  && kr[3].key == "a|2" && kr[3].owner == 100
	  && kr[4].key == "a|3" && kr[4].owner == 101
	  && kr[5].key == "a|4" && kr[5].owner == 100
	  && kr[6].key == "a|5" && kr[6].owner == 101
	  && kr[7].key == "a|6" && kr[7].owner == 100
	  && kr[8].key == "a|7" && kr[8].owner == 101
	  && kr[9].key == "a|8" && kr[9].owner == 100
	  && kr[10].key == "a|9" && kr[10].owner == 101
	  && kr[11].key == "a}" && kr[11].owner == 39))
	ps.print_fail(__LINE__, kr);

    ps.analyze("ba", "c", 0, kr);
    if (!(kr.size() == 23
	  && kr[0].key == "ba" && kr[0].owner == 39
	  && kr[1].key == "bc|" && kr[1].owner == 103
	  && kr[2].key == "bc|1" && kr[2].owner == 104
	  && kr[3].key == "bc|2" && kr[3].owner == 105
	  && kr[4].key == "bc|3" && kr[4].owner == 103
	  && kr[10].key == "bc|9" && kr[10].owner == 103
	  && kr[11].key == "bc}" && kr[11].owner == 39
	  && kr[12].key == "b|" && kr[12].owner == 105
	  && kr[13].key == "b|1" && kr[13].owner == 106
	  && kr[14].key == "b|2" && kr[14].owner == 107
	  && kr[15].key == "b|3" && kr[15].owner == 105
	  && kr[21].key == "b|9" && kr[21].owner == 105
	  && kr[22].key == "b}" && kr[22].owner == 39))
	ps.print_fail(__LINE__, kr);

    ps.analyze("A", "C", 0, kr);
    if (!(kr.size() == 1
	  && kr[0].key == "A" && kr[0].owner == 39))
	ps.print_fail(__LINE__, kr);

    ps.analyze("d|0050", "d|600", 0, kr);
    if (!(kr.size() == 3
	  && kr[0].key == "d|0050" && kr[0].owner == 200
	  && kr[1].key == "d|200" && kr[1].owner == 201
	  && kr[2].key == "d|400" && kr[2].owner == 202))
	ps.print_fail(__LINE__, kr);

    ps.analyze("d|201", "d|601", 0, kr);
    if (!(kr.size() == 3
	  && kr[0].key == "d|201" && kr[0].owner == 201
	  && kr[1].key == "d|400" && kr[1].owner == 202
	  && kr[2].key == "d|600" && kr[2].owner == 203))
	ps.print_fail(__LINE__, kr);

    ps.analyze("e", "g", 0, kr);
    if (!(kr.size() == 6
	  && kr[0].key == "e" && kr[0].owner == 39
	  && kr[1].key == "fa" && kr[1].owner == 300
	  && kr[2].key == "fa5" && kr[2].owner == 301
	  && kr[3].key == "fb" && kr[3].owner == 350
	  && kr[4].key == "fb5" && kr[4].owner == 351
	  && kr[5].key == "fc" && kr[5].owner == 39))
	ps.print_fail(__LINE__, kr);

    assert(ps.find("d|201").server() == 201);
    assert(ps.find("d|400").server() == 202);
    assert(ps.find("e").server() == 39);
    assert(ps.find("fa").server() == 300);
    assert(ps.find("fa5").server() == 301);
    assert(ps.find("fb").server() == 350);
    assert(ps.find("fb5").server() == 351);
    assert(ps.find("fc").server() == 39);
}


Json partition_set::unparse_json() const {
    Json j = Json::make_array();
    for (const partition1 *p1 = begin_p1(); p1 != end_p1(); ++p1)
	if (!p1->is_default())
	    j.push_back(p1->unparse_json());
    return j;
}

String partition_set::unparse() const {
    return unparse_json().unparse();
    StringAccum sa;
    sa << default_server_;
    for (auto it = begin(); it != end(); it.advance_group())
        if (!it.is_default()) {
            if (sa)
                sa << ' ';
            sa << it.key() << '[' << it.group_first_server();
            if (it.group_last_server() > it.group_first_server() + 1)
                sa << '-' << (it.group_last_server() - 1);
            sa << ']';
        }
    return sa.take_string();
}

String Partitioner::unparse() const {
    return ps_.unparse();
}


namespace {
class DefaultPartitioner : public Partitioner {
  public:
    DefaultPartitioner(uint32_t nservers);
};

DefaultPartitioner::DefaultPartitioner(uint32_t nservers)
    : Partitioner(0, 0) {
    ps_.add(partition1("", partition1::text, 1, 0, nservers));
}


class UnitTestPartitioner : public Partitioner {
  public:
    UnitTestPartitioner(uint32_t nservers, int32_t default_owner);
  private:
    static const char prefixes_[];
};
const char UnitTestPartitioner::prefixes_[] =
    " !\"#$%&'()*+,-./0123456789:;<=>?@"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`"
    "abcdefghijklmnopqrstuvwxyz{|}";

UnitTestPartitioner::UnitTestPartitioner(uint32_t nservers, int default_owner)
    : Partitioner(default_owner, 0) {
    // make partitions, except leave '~' as default_owner.
    // Keys with the same first character are owned by the same server.
    for (size_t i = 0; prefixes_[i]; ++i)
        ps_.add(partition1(Str(&prefixes_[i], 1), partition1::text, 0, i % nservers, 1));
}

class TwitterPartitioner : public Partitioner {
  public:
    TwitterPartitioner(uint32_t nservers, uint32_t nbacking,
                       uint32_t default_owner, bool binary);
};

TwitterPartitioner::TwitterPartitioner(uint32_t nservers, uint32_t nbacking,
                                       uint32_t default_owner, bool newtwitter)
    : Partitioner(default_owner, nbacking) {

    ps_.add(partition1("c|", partition1::text, 0, 0, 1));

    if (newtwitter) {
        ps_.add(partition1("cp|", partition1::binary, 14, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("p|", partition1::binary, 14, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("s|", partition1::binary, 14, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("t|", partition1::binary, 14, nbacking, nservers - nbacking));
    }
    else {
        ps_.add(partition1("f|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("p|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("s|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("t|", partition1::decimal, 5, nbacking, nservers - nbacking));
    }
}

}


Partitioner *Partitioner::make(const String &name, uint32_t nservers,
                               uint32_t default_owner) {
    return Partitioner::make(name, 0, nservers, default_owner);
}

Partitioner *Partitioner::make(const String &name, uint32_t nbacking,
                               uint32_t nservers, uint32_t default_owner) {
    if (name == "default" || nservers == 1)
        return new DefaultPartitioner(nservers);
    else if (name == "unit")
        return new UnitTestPartitioner(nservers, default_owner);
    else if (name == "twitter")
        return new TwitterPartitioner(nservers, nbacking, default_owner, false);
    else if (name == "twitternew")
        return new TwitterPartitioner(nservers, nbacking, default_owner, true);
    else
        assert(0 && "Unknown partition name");
    return 0;
}

}
