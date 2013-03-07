#include "msgpack.hh"
namespace msgpack {

namespace {
const uint8_t nbytes[] = {
    /* 0xC0-0xC9 */ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    /* 0xCA float */ 5, /* 0xCB double */ 9,
    /* 0xCC-0xD3 ints */ 2, 3, 5, 9, 2, 3, 5, 9,
    /* 0xD4-0xD9 */ 0, 0, 0, 0, 0, 0,
    /* 0xDA-0xDF */ 3, 5, 3, 5, 3, 5
};

template <typename T> T grab(const uint8_t* x) {
    return net_to_host_order(*reinterpret_cast<const T*>(x));
}
}

const uint8_t* streaming_parser::consume(const uint8_t* first,
                                         const uint8_t* last,
                                         const String& str) {
    Json j;
    int n;

    if (state_ < 0)
        return first;
    if (state_ == st_partial || state_ == st_string) {
        int nneed;
        if (state_ == st_partial)
            nneed = nbytes[str_.udata()[0] - 0xC0];
        else
            nneed = stack_.back().size;
        const uint8_t* next;
        if (last - first < nneed - str_.length())
            next = last;
        else
            next = first + (nneed - str_.length());
        str_.append(first, next);
        if (str_.length() != nneed)
            return next;
        first = next;
        if (state_ == st_string) {
            j = Json(std::move(str_));
            stack_.pop_back();
            goto next;
        } else {
            state_ = st_normal;
            consume(str_.ubegin(), str_.uend(), str_);
            if (state_ != st_normal)
                return next;
        }
    }

    while (first != last) {
        if ((uint8_t) (*first + 0x20) < 0xA0) {
            j = Json(int(int8_t(*first)));
            ++first;
        } else if (*first == 0xC0) {
            j = Json();
            ++first;
        } else if ((*first | 1) == 0xC3) {
            j = Json(bool(*first == 0xC3));
            ++first;
        } else if ((uint8_t) (*first - 0x80) < 0x10) {
            n = *first - 0x80;
            ++first;
        map:
            j = Json::make_object();
        } else if ((uint8_t) (*first - 0x90) < 0x10) {
            n = *first - 0x90;
            ++first;
        array:
            j = Json::make_array_reserve(n);
        } else if ((uint8_t) (*first - 0xA0) < 0x20) {
            n = *first - 0xA0;
            ++first;
        raw:
            if (last - first < n) {
                str_ = String(first, last);
                stack_.push_back(selem{0, n});
                state_ = st_string;
                return last;
            }
            if (first < str.ubegin() || first + n >= str.uend())
                j = Json(String(first, n));
            else {
                const char* s = reinterpret_cast<const char*>(first);
                j = Json(str.fast_substring(s, s + n));
            }
            first += n;
        } else {
            uint8_t type = *first - 0xC0;
            if (!nbytes[type])
                goto error;
            if (last - first < nbytes[type]) {
                str_ = String(first, last);
                state_ = st_partial;
                return last;
            }
            first += nbytes[type];
            switch (type) {
            case 0x0A:
                j = Json(grab<float>(first - 4));
                break;
            case 0x0B:
                j = Json(grab<double>(first - 8));
                break;
            case 0x0C:
                j = Json(first[-1]);
                break;
            case 0x0D:
                j = Json(grab<uint16_t>(first - 2));
                break;
            case 0x0E:
                j = Json(grab<uint32_t>(first - 4));
                break;
            case 0x0F:
                j = Json(grab<uint64_t>(first - 8));
                break;
            case 0x10:
                j = Json(int8_t(first[-1]));
                break;
            case 0x11:
                j = Json(grab<int16_t>(first - 2));
                break;
            case 0x12:
                j = Json(grab<int32_t>(first - 4));
                break;
            case 0x13:
                j = Json(grab<int64_t>(first - 8));
                break;
            case 0x1A:
                n = grab<uint16_t>(first - 2);
                goto raw;
            case 0x1B:
                n = grab<uint32_t>(first - 4);
                goto raw;
            case 0x1C:
                n = grab<uint16_t>(first - 2);
                goto array;
            case 0x1D:
                n = grab<uint32_t>(first - 4);
                goto array;
            case 0x1E:
                n = grab<uint16_t>(first - 2);
                goto map;
            case 0x1F:
                n = grab<uint32_t>(first - 4);
                goto map;
            }
        }

        // Add it
    next:
        Json* jp = stack_.size() ? stack_.back().jp : &json_;
        if (jp->is_o()) {
            // Reading a key for some object Json
            if (!j.is_s() && !j.is_i())
                goto error;
            --stack_.back().size;
            stack_.push_back(selem{&jp->get_insert(j.to_s()), 0});
            continue;
        }

        if (jp->is_a()) {
            jp->push_back(std::move(j));
            jp = &jp->back();
            --stack_.back().size;
        } else
            swap(*jp, j);

        if ((jp->is_a() || jp->is_o()) && n != 0)
            stack_.push_back(selem{jp, n});
        else {
            while (!stack_.empty() && stack_.back().size == 0)
                stack_.pop_back();
            if (stack_.empty()) {
                state_ = st_final;
                return first;
            }
        }
    }

    state_ = st_normal;
    return first;

 error:
    state_ = st_error;
    return first;
}

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

void compact_unparser::unparse(StringAccum& sa, const Json& j) {
    if (j.is_null())
        sa << '\xC0';
    else if (j.is_b())
        sa << (char) ('\xC2' + j.as_b());
    else if (j.is_i()) {
        uint8_t* x = (uint8_t*) sa.reserve(9);
        sa.adjust_length(unparse(x, (int64_t) j.as_i()) - x);
    } else if (j.is_d()) {
        uint8_t* x = (uint8_t*) sa.reserve(9);
        sa.adjust_length(unparse(x, j.as_d()) - x);
    } else if (j.is_s()) {
        uint8_t* x = (uint8_t*) sa.reserve(j.as_s().length() + 5);
        sa.adjust_length(unparse(x, j.as_s()) - x);
    } else if (j.is_a()) {
        uint8_t* x = (uint8_t*) sa.reserve(5);
        if (j.size() < 16) {
            *x = 0x90 + j.size();
            sa.adjust_length(1);
        } else if (j.size() < 65536) {
            *x = 0xDC;
            *reinterpret_cast<uint16_t*>(x + 1) = host_to_net_order((uint16_t) j.size());
            sa.adjust_length(3);
        } else {
            *x = 0xDD;
            *reinterpret_cast<uint32_t*>(x + 1) = host_to_net_order((uint32_t) j.size());
            sa.adjust_length(5);
        }
        for (auto it = j.cabegin(); it != j.caend(); ++it)
            unparse(sa, *it);
    } else if (j.is_o()) {
        uint8_t* x = (uint8_t*) sa.reserve(5);
        if (j.size() < 16) {
            *x = 0x80 + j.size();
            sa.adjust_length(1);
        } else if (j.size() < 65536) {
            *x = 0xDE;
            *reinterpret_cast<uint16_t*>(x + 1) = host_to_net_order((uint16_t) j.size());
            sa.adjust_length(3);
        } else {
            *x = 0xDF;
            *reinterpret_cast<uint32_t*>(x + 1) = host_to_net_order((uint32_t) j.size());
            sa.adjust_length(5);
        }
        for (auto it = j.cobegin(); it != j.coend(); ++it) {
            uint8_t* x = (uint8_t*) sa.reserve(it.key().length() + 5);
            sa.adjust_length(unparse(x, it.key()) - x);
            unparse(sa, it.value());
        }
    } else
        sa << '\xC0';
}

}
