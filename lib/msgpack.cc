#include "msgpack.hh"
namespace msgpack {

parser& parser::parse(Str& x) {
    uint32_t len;
    if ((uint32_t) *s_ - 0xA0 < 32) {
        len = *s_ - 0xA0;
        ++s_;
    } else if (*s_ == 0xDA) {
        len = net_to_host_order(*reinterpret_cast<const uint16_t*>(s_ + 1));
        s_ += 3;
    } else {
        assert(*s_ == 0xDB);
        len = net_to_host_order(*reinterpret_cast<const uint32_t*>(s_ + 1));
        s_ += 5;
    }
    x.assign(reinterpret_cast<const char*>(s_), len);
    s_ += len;
    return *this;
}

parser& parser::parse(String& x) {
    Str s;
    parse(s);
    if (str_)
        x = str_.substring(s.begin(), s.end());
    else
        x.assign(s.begin(), s.end());
    return *this;
}

}
