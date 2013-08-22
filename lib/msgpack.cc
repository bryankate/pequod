#include "msgpack.hh"
namespace msgpack {

namespace {
const uint8_t nbytes[] = {
    /* 0xC0-0xC3 */ 0, 0, 0, 0,
    /* 0xC4-0xC6 fbin8-fbin32 */ 2, 3, 5,
    /* 0xC7-0xC9 fext */ 0, 0, 0,
    /* 0xCA ffloat32 */ 5,
    /* 0xCB ffloat64 */ 9,
    /* 0xCC-0xD3 ints */ 2, 3, 5, 9, 2, 3, 5, 9,
    /* 0xD4-0xD8 ffixext */ 0, 0, 0, 0, 0,
    /* 0xD9-0xDB fstr8-fstr32 */ 2, 3, 5,
    /* 0xDC-0xDD farray16-farray32 */ 3, 5,
    /* 0xDE-0xDF fmap16-fmap32 */ 3, 5
};
}

static inline bool in_range(uint8_t x, unsigned low, unsigned n) {
    return (unsigned) x - low < n;
}

static inline bool in_wrapped_range(uint8_t x, unsigned low, unsigned n) {
    return (unsigned) (int8_t) x - low < n;
}

const uint8_t* streaming_parser::consume(const uint8_t* first,
                                         const uint8_t* last,
                                         const String& str) {
    using std::swap;
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
        if (in_wrapped_range(*first, -format::nfixnegint, format::nfixint)) {
            j = Json(int(int8_t(*first)));
            ++first;
        } else if (*first == format::fnull) {
            j = Json();
            ++first;
        } else if (in_range(*first, format::ffalse, 2)) {
            j = Json(bool(*first - format::ffalse));
            ++first;
        } else if (in_range(*first, format::ffixmap, format::nfixmap)) {
            n = *first - format::ffixmap;
            ++first;
        map:
            if (stack_.empty() && json_.is_o()) {
                swap(j, json_);
                j.clear();
            } else
                j = Json::make_object();
        } else if (in_range(*first, format::ffixarray, format::nfixarray)) {
            n = *first - format::ffixarray;
            ++first;
        array:
            if (stack_.empty() && json_.is_a()) {
                swap(j, json_);
                j.clear();
            } else
                j = Json::make_array_reserve(n);
        } else if (in_range(*first, format::ffixstr, format::nfixstr)) {
            n = *first - format::ffixstr;
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
            uint8_t type = *first - format::fnull;
            if (!nbytes[type])
                goto error;
            if (last - first < nbytes[type]) {
                str_ = String(first, last);
                state_ = st_partial;
                return last;
            }
            first += nbytes[type];
            switch (type) {
            case format::ffloat32 - format::fnull:
                j = Json(read_in_net_order<float>(first - 4));
                break;
            case format::ffloat64 - format::fnull:
                j = Json(read_in_net_order<double>(first - 8));
                break;
            case format::fuint8 - format::fnull:
                j = Json(first[-1]);
                break;
            case format::fuint16 - format::fnull:
                j = Json(read_in_net_order<uint16_t>(first - 2));
                break;
            case format::fuint32 - format::fnull:
                j = Json(read_in_net_order<uint32_t>(first - 4));
                break;
            case format::fuint64 - format::fnull:
                j = Json(read_in_net_order<uint64_t>(first - 8));
                break;
            case format::fint8 - format::fnull:
                j = Json(int8_t(first[-1]));
                break;
            case format::fint16 - format::fnull:
                j = Json(read_in_net_order<int16_t>(first - 2));
                break;
            case format::fint32 - format::fnull:
                j = Json(read_in_net_order<int32_t>(first - 4));
                break;
            case format::fint64 - format::fnull:
                j = Json(read_in_net_order<int64_t>(first - 8));
                break;
            case format::fbin8 - format::fnull:
            case format::fstr8 - format::fnull:
                n = first[-1];
                goto raw;
            case format::fbin16 - format::fnull:
            case format::fstr16 - format::fnull:
                n = read_in_net_order<uint16_t>(first - 2);
                goto raw;
            case format::fbin32 - format::fnull:
            case format::fstr32 - format::fnull:
                n = read_in_net_order<uint32_t>(first - 4);
                goto raw;
            case format::farray16 - format::fnull:
                n = read_in_net_order<uint16_t>(first - 2);
                goto array;
            case format::farray32 - format::fnull:
                n = read_in_net_order<uint32_t>(first - 4);
                goto array;
            case format::fmap16 - format::fnull:
                n = read_in_net_order<uint16_t>(first - 2);
                goto map;
            case format::fmap32 - format::fnull:
                n = read_in_net_order<uint32_t>(first - 4);
                goto map;
            }
        }

        // Add it
    next:
        Json* jp = stack_.size() ? stack_.back().jp : &json_;
        if (jp->is_o() && stack_.size()) {
            // Reading a key for some object Json
            if (!j.is_s() && !j.is_i())
                goto error;
            --stack_.back().size;
            stack_.push_back(selem{&jp->get_insert(j.to_s()), 0});
            continue;
        }

        if (jp->is_a() && stack_.size()) {
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
    if ((uint32_t) *s_ - format::ffixstr < format::nfixstr) {
        len = *s_ - format::ffixstr;
        ++s_;
    } else if (*s_ == format::fbin8 || *s_ == format::fstr8) {
        len = s_[1];
        s_ += 2;
    } else if (*s_ == format::fbin16 || *s_ == format::fstr16) {
        len = read_in_net_order<uint16_t>(s_ + 1);
        s_ += 3;
    } else {
        assert(*s_ == format::fbin32 || *s_ == format::fstr32);
        len = read_in_net_order<uint32_t>(s_ + 1);
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
        sa << (char) format::fnull;
    else if (j.is_b())
        sa << (char) (format::ffalse + j.as_b());
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
        if (j.size() < format::nfixarray) {
            *x = format::ffixarray + j.size();
            sa.adjust_length(1);
        } else if (j.size() < 65536) {
            *x = format::farray16;
            write_in_net_order<uint16_t>(x + 1, (uint16_t) j.size());
            sa.adjust_length(3);
        } else {
            *x = format::farray32;
            write_in_net_order<uint32_t>(x + 1, (uint32_t) j.size());
            sa.adjust_length(5);
        }
        for (auto it = j.cabegin(); it != j.caend(); ++it)
            unparse(sa, *it);
    } else if (j.is_o()) {
        uint8_t* x = (uint8_t*) sa.reserve(5);
        if (j.size() < format::nfixmap) {
            *x = format::ffixmap + j.size();
            sa.adjust_length(1);
        } else if (j.size() < 65536) {
            *x = format::fmap16;
            write_in_net_order<uint16_t>(x + 1, (uint16_t) j.size());
            sa.adjust_length(3);
        } else {
            *x = format::fmap32;
            write_in_net_order<uint32_t>(x + 1, (uint32_t) j.size());
            sa.adjust_length(5);
        }
        for (auto it = j.cobegin(); it != j.coend(); ++it) {
            uint8_t* x = (uint8_t*) sa.reserve(it.key().length() + 5);
            sa.adjust_length(unparse(x, it.key()) - x);
            unparse(sa, it.value());
        }
    } else
        sa << format::fnull;
}

} // namespace msgpack
