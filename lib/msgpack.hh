#ifndef GSTORE_MSGPACK_HH
#define GSTORE_MSGPACK_HH 1
#include "string.hh"
#include "str.hh"
#include <vector>

class msgpack_compact_unparser {
  public:
    inline uint8_t* unparse_null(uint8_t* s) {
	*s++ = 0xC0;
	return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint32_t x) {
        if (x < 128)
            *s++ = x;
        else if (x < 256) {
            *s++ = 0xCC;
            *s++ = x;
        } else if (x < 65536) {
            *s++ = 0xCD;
            *reinterpret_cast<uint16_t*>(s) = host_to_net_order((uint16_t) x);
            s += 2;
        } else {
            *s++ = 0xCE;
            *reinterpret_cast<uint32_t*>(s) = host_to_net_order(x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse_small(uint8_t* s, uint32_t x) {
        return unparse(s, x);
    }
    inline uint8_t* unparse_tiny(uint8_t* s, int x) {
        assert((uint32_t) x + 32 < 160);
        *s++ = x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint64_t x) {
        if (x < 4294967296ULL)
            return unparse(s, (uint32_t) x);
        else {
            *s++ = 0xCF;
            *reinterpret_cast<uint64_t*>(s) = host_to_net_order(x);
            return s + 8;
        }
    }
    inline uint8_t* unparse(uint8_t* s, int32_t x) {
        if ((uint32_t) x + 32 < 160)
            *s++ = x;
        else if ((uint32_t) x + 128 < 256) {
            *s++ = 0xD0;
            *s++ = x;
        } else if ((uint32_t) x + 32768 < 65536) {
            *s++ = 0xD1;
            *reinterpret_cast<int16_t*>(s) = host_to_net_order((int16_t) x);
            s += 2;
        } else {
            *s++ = 0xD2;
            *reinterpret_cast<int32_t*>(s) = host_to_net_order(x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse_small(uint8_t* s, int32_t x) {
        return unparse(s, x);
    }
    inline uint8_t* unparse(uint8_t* s, int64_t x) {
        if (x + 2147483648ULL < 4294967296ULL)
            return unparse(s, (int32_t) x);
        else {
            *s++ = 0xD3;
            *reinterpret_cast<int64_t*>(s) = host_to_net_order(x);
            return s + 8;
        }
    }
    inline uint8_t* unparse(uint8_t* s, double x) {
        *s++ = 0xCB;
        *reinterpret_cast<double*>(s) = host_to_net_order(x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, bool x) {
        *s++ = 0xC2 + x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, const char *data, int len) {
        if (len < 32)
            *s++ = 0xA0 + len;
        else if (len < 65536) {
            *s++ = 0xDA;
            *reinterpret_cast<uint16_t*>(s) = host_to_net_order((uint16_t) len);
            s += 2;
        } else {
            *s++ = 0xDB;
            *reinterpret_cast<uint32_t*>(s) = host_to_net_order((uint32_t) len);
            s += 4;
        }
        memcpy(s, data, len);
        return s + len;
    }
    inline uint8_t* unparse(uint8_t* s, Str x) {
        return unparse(s, x.data(), x.length());
    }
    inline uint8_t* unparse(uint8_t* s, const String& x) {
        return unparse(s, x.data(), x.length());
    }
    template <typename T>
    inline uint8_t* unparse(uint8_t* s, const ::std::vector<T>& x) {
        if (x.size() < 16)
            *s++ = 0x90 + x.size();
        else if (x.size() < 65536) {
            *s++ = 0xDC;
            *reinterpret_cast<uint16_t*>(s) = host_to_net_order((uint16_t) x.size());
            s += 2;
        } else {
            *s++ = 0xDD;
            *reinterpret_cast<uint32_t*>(s) = host_to_net_order((uint32_t) x.size());
            s += 4;
        }
        for (typename ::std::vector<T>::const_iterator it = x.begin();
             it != x.end(); ++it)
            s = unparse(s, *it);
        return s;
    }
};

class msgpack_fast_unparser {
  public:
    inline uint8_t* unparse_null(uint8_t* s) {
	*s++ = 0xC0;
	return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint32_t x) {
        *s++ = 0xCE;
        *reinterpret_cast<uint32_t*>(s) = host_to_net_order(x);
        return s + 4;
    }
    inline uint8_t* unparse_small(uint8_t* s, uint32_t x) {
        if (x < 128)
            *s++ = x;
        else {
            *s++ = 0xCE;
            *reinterpret_cast<uint32_t*>(s) = host_to_net_order(x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse_tiny(uint8_t* s, int x) {
        assert((uint32_t) x + 32 < 160);
        *s++ = x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, uint64_t x) {
        *s++ = 0xCF;
        *reinterpret_cast<uint64_t*>(s) = host_to_net_order(x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, int32_t x) {
        *s++ = 0xD2;
        *reinterpret_cast<int32_t*>(s) = host_to_net_order(x);
        return s + 4;
    }
    inline uint8_t* unparse_small(uint8_t* s, int32_t x) {
        if ((uint32_t) x + 32 < 160)
            *s++ = x;
        else {
            *s++ = 0xD2;
            *reinterpret_cast<int32_t*>(s) = host_to_net_order(x);
            s += 4;
        }
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, int64_t x) {
        *s++ = 0xD3;
        *reinterpret_cast<int64_t*>(s) = host_to_net_order(x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, double x) {
        *s++ = 0xCB;
        *reinterpret_cast<double*>(s) = host_to_net_order(x);
        return s + 8;
    }
    inline uint8_t* unparse(uint8_t* s, bool x) {
        *s++ = 0xC2 + x;
        return s;
    }
    inline uint8_t* unparse(uint8_t* s, const char *data, int len) {
        if (len < 32)
            *s++ = 0xA0 + len;
        else {
            *s++ = 0xDB;
            *reinterpret_cast<uint32_t*>(s) = host_to_net_order((uint32_t) len);
            s += 4;
        }
        memcpy(s, data, len);
        return s + len;
    }
    inline uint8_t* unparse(uint8_t* s, Str x) {
        return unparse(s, x.data(), x.length());
    }
    inline uint8_t* unparse(uint8_t* s, const String& x) {
        return unparse(s, x.data(), x.length());
    }
    template <typename T>
    inline uint8_t* unparse(uint8_t* s, const ::std::vector<T>& x) {
        if (x.size() < 16)
            *s++ = 0x90 + x.size();
        else {
            *s++ = 0xDD;
            *reinterpret_cast<uint32_t*>(s) = host_to_net_order((uint32_t) x.size());
            s += 4;
        }
        for (typename ::std::vector<T>::const_iterator it = x.begin();
             it != x.end(); ++it)
            s = unparse(s, *it);
        return s;
    }
};

class msgpack_parser {
  public:
    explicit inline msgpack_parser(const uint8_t* s)
        : s_(s), str_() {
    }
    explicit inline msgpack_parser(const String& str)
        : s_(reinterpret_cast<const uint8_t*>(str.begin())), str_(str) {
    }
    inline const char* position() const {
        return reinterpret_cast<const char*>(s_);
    }
    inline bool try_parse_null() {
	if (*s_ == 0xC0) {
	    ++s_;
	    return true;
	} else
	    return false;
    }
    inline int parse_tiny_int() {
        assert((uint32_t) (int8_t) *s_ + 32 < 160);
        return (int8_t) *s_++;
    }
    inline msgpack_parser& parse_tiny_int(int& x) {
        x = parse_tiny_int();
        return *this;
    }
    template <typename T>
    inline msgpack_parser& parse_int(T& x) {
        if ((uint32_t) (int8_t) *s_ + 32 < 160) {
            x = (int8_t) *s_;
            ++s_;
        } else {
            assert((uint32_t) *s_ - 0xCC < 8);
            hard_parse_int(x);
        }
        return *this;
    }
    inline msgpack_parser& parse(int& x) { return parse_int(x); }
    inline msgpack_parser& parse(long& x) { return parse_int(x); }
    inline msgpack_parser& parse(long long& x) { return parse_int(x); }
    inline msgpack_parser& parse(unsigned& x) { return parse_int(x); }
    inline msgpack_parser& parse(unsigned long& x) { return parse_int(x); }
    inline msgpack_parser& parse(unsigned long long& x) { return parse_int(x); }
    inline msgpack_parser& parse(bool& x) {
        assert((uint32_t) *s_ - 0xC2 < 2);
        x = *s_ != 0xC2;
        ++s_;
        return *this;
    }
    inline msgpack_parser& parse(double& x) {
        assert(*s_ == 0xCB);
        x = net_to_host_order(*reinterpret_cast<const double*>(s_ + 1));
        s_ += 9;
        return *this;
    }
    msgpack_parser& parse(Str& x);
    msgpack_parser& parse(String& x);
    template <typename T> msgpack_parser& parse(::std::vector<T>& x);
  private:
    const uint8_t* s_;
    String str_;
    template <typename T>
    void hard_parse_int(T& x);
};

template <typename T>
void msgpack_parser::hard_parse_int(T& x) {
    switch (*s_) {
    case 0xCC:
        x = s_[1];
        s_ += 2;
        break;
    case 0xCD:
        x = net_to_host_order(*reinterpret_cast<const uint16_t*>(s_ + 1));
        s_ += 3;
        break;
    case 0xCE:
        x = net_to_host_order(*reinterpret_cast<const uint32_t*>(s_ + 1));
        s_ += 5;
        break;
    case 0xCF:
        x = net_to_host_order(*reinterpret_cast<const uint64_t*>(s_ + 1));
        s_ += 9;
        break;
    case 0xD0:
        x = (int8_t) s_[1];
        s_ += 2;
        break;
    case 0xD1:
        x = net_to_host_order(*reinterpret_cast<const int16_t*>(s_ + 1));
        s_ += 3;
        break;
    case 0xD2:
        x = net_to_host_order(*reinterpret_cast<const int32_t*>(s_ + 1));
        s_ += 5;
        break;
    case 0xD3:
        x = net_to_host_order(*reinterpret_cast<const int64_t*>(s_ + 1));
        s_ += 9;
        break;
    }
}

template <typename T>
msgpack_parser& msgpack_parser::parse(::std::vector<T>& x) {
    uint32_t sz;
    if ((uint32_t) *s_ - 0x90 < 16) {
        sz = *s_ - 0x90;
        ++s_;
    } else if (*s_ == 0xDC) {
        sz = net_to_host_order(*reinterpret_cast<const uint16_t*>(s_ + 1));
        s_ += 3;
    } else {
        assert(*s_ == 0xDD);
        sz = net_to_host_order(*reinterpret_cast<const uint32_t*>(s_ + 1));
        s_ += 5;
    }
    for (; sz != 0; --sz) {
        x.push_back(T());
        parse(x.back());
    }
    return *this;
}

#endif
