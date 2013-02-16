#ifndef LCDF_ENCODING_HH
#define LCDF_ENCODING_HH 1
#include "compiler.hh"
#include <string.h>
#include <assert.h>

namespace Encoding {

struct UTF8EncoderBase {

    UTF8EncoderBase()
	: _outbegin(0), _outend(0),
	  _overlapping(false), _replacement(false), _strip_bom(false),
	  _buflen(0) {
    }

    bool strip_bom() const {
	return _strip_bom;
    }
    void set_strip_bom(bool strip_bom) {
	_strip_bom = strip_bom;
    }

    bool buffer_empty() const {
	return _buflen == 0;
    }
    const unsigned char *buffer_begin() const {
	return _buf;
    }
    const unsigned char *buffer_end() const {
	return _buf + _buflen;
    }
    void set_buffer(const unsigned char *begin,
		    const unsigned char *end) {
	assert(end - begin <= 8);
	memcpy(_buf, begin, end - begin);
	_buflen = end - begin;
    }
    void append_buffer(unsigned char ch) {
	assert(_buflen < 8);
	_buf[_buflen++] = ch;
    }
    void consume_buffer(const unsigned char *pos) {
	assert(pos >= _buf && pos <= _buf + _buflen);
	int n = pos - _buf;
	if (n != _buflen)
	    memmove(_buf, pos, _buflen - n);
	_buflen -= n;
    }

    unsigned char *output_begin() const {
	return _outbegin;
    }
    unsigned char *output_end() const {
	return _outend;
    }
    void set_output(unsigned char *outbegin, unsigned char *outend,
		    const unsigned char *inbegin) {
	_outbegin = outbegin;
	_outend = outend;
	_overlapping = inbegin >= outbegin && inbegin < outend;
    }

    bool store_copy(const unsigned char *inbegin,
		    const unsigned char *inend) {
	unsigned char *outx = _outbegin + (inend - inbegin);
	if (unlikely(outx > _outend
		     || (_overlapping && outx > inend)))
	    return false;
	if (inbegin != _outbegin)
	    memmove(_outbegin, inbegin, inend - inbegin);
	_outbegin = outx;
	return true;
    }

    bool store_utf8(int ch,
		    const unsigned char *inend) {
	if (unlikely(ch == -2 && _replacement))
	    ch = 0xFFFD;
	else if (unlikely(ch < 0))
	    return true;
	int t = ch < 0x80 ? 1 : ch < 0x800 ? 2 : ch < 0x10000 ? 3 : 4;
	if (unlikely(_outbegin + t > _outend
		     || (_overlapping && _outbegin + t > inend)))
	    return false;
	if (t == 1)
	    *_outbegin++ = ch;
	else if (t == 2) {
	    *_outbegin++ = 0xC0 | (ch >> 6);
	lastchar:
	    *_outbegin++ = 0x80 | (ch & 0x3F);
	} else if (t == 3) {
	    *_outbegin++ = 0xE0 | (ch >> 12);
	last2char:
	    *_outbegin++ = 0x80 | ((ch >> 6) & 0x3F);
	    goto lastchar;
	} else {
	    *_outbegin++ = 0xF0 | (ch >> 18);
	    *_outbegin++ = 0x80 | ((ch >> 12) & 0x3F);
	    goto last2char;
	}
	return true;
    }

    const unsigned char *start(const unsigned char *begin,
			       const unsigned char *end) {
	if (_strip_bom && _buflen == 0 && begin != end)
	    _buf[_buflen++] = *begin++;
	return begin;
    }

  private:

    unsigned char *_outbegin;
    unsigned char *_outend;
    bool _overlapping;
    bool _replacement;
    bool _strip_bom;
    unsigned char _buf[8];
    int _buflen;

};

template <typename SOURCE_ENCODING>
struct UTF8Encoder : public UTF8EncoderBase, private SOURCE_ENCODING {
    UTF8Encoder() {
    }
    bool flush_clear();
    const unsigned char *flush(const unsigned char *begin,
			       const unsigned char *end);
    const unsigned char *encode(const unsigned char *begin,
				const unsigned char *end);
};

struct CharInfo {
    int ch;
    const unsigned char *pos;
    CharInfo() {
    }
    CharInfo(int ch_, const unsigned char *pos_)
	: ch(ch_), pos(pos_) {
    }
};

struct UTF8 {
    static bool invalid_utf8(const unsigned char *begin, int t) {
	return
	    /* overlong encoding (put first) */
	    (t == 2 && begin[0] < 0xC2)
	    /* missing continuations */
	    || (begin[1] < 0x80 || begin[1] >= 0xC0)
	    || (t > 2 && (begin[2] < 0x80 || begin[2] >= 0xC0))
	    || (t > 3 && (begin[3] < 0x80 || begin[3] >= 0xC0))
	    /* overlong encoding */
	    || (t == 3 && begin[0] == 0xE0 && begin[1] < 0xA0)
	    /* encoded surrogate */
	    || (t == 3 && begin[0] == 0xED && begin[1] >= 0xA0)
	    /* overlong encoding */
	    || (t == 4 && begin[0] == 0xF0 && begin[1] < 0x90)
	    /* character out of range */
	    || (t == 4 && begin[0] == 0xF4 && begin[1] >= 0x90)
	    || (t == 4 && begin[0] > 0xF4);
    }

    static const unsigned char *skip_utf8(const unsigned char *begin,
					  const unsigned char *end) {
	while (begin != end)
	    if (*begin < 0x80)
		++begin;
	    else {
		int t = *begin < 0xE0 ? 2 : *begin < 0xF0 ? 3 : 4;
		if (unlikely(begin + t > end || invalid_utf8(begin, t)))
		    break;
		begin += t;
	    }
	return begin;
    }

    static CharInfo decode_invalid_utf8(const unsigned char *begin,
					const unsigned char *end) {
	int t = 1;
	while (t < 5 && begin + t != end && begin[t] >= 0x80 && begin[t] < 0xC0)
	    ++t;
	if (begin + t == end)
	    return CharInfo(-1, begin);
	else
	    return CharInfo(-2, begin + t);
    }
};

struct UTF8NoNul {
    static const unsigned char *skip_utf8(const unsigned char *begin,
					  const unsigned char *end) {
	while (begin != end)
	    if (*begin > 0 && *begin < 0x80)
		++begin;
	    else {
		int t = *begin < 0xE0 ? 2 : *begin < 0xF0 ? 3 : 4;
		if (unlikely(begin + t > end || UTF8::invalid_utf8(begin, t)))
		    break;
		begin += t;
	    }
	return begin;
    }

    static CharInfo decode_invalid_utf8(const unsigned char *begin,
					const unsigned char *end) {
	if (*begin == 0)
	    return CharInfo(-3, begin + 1);
	int t = 1;
	while (t < 5 && begin + t != end && begin[t] >= 0x80 && begin[t] < 0xC0)
	    ++t;
	if (begin + t == end)
	    return CharInfo(-1, begin);
	else
	    return CharInfo(-2, begin + t);
    }
};

struct CESU8 {
    static const unsigned char *skip_utf8(const unsigned char *begin,
					  const unsigned char *end) {
	return UTF8::skip_utf8(begin, end);
    }

    static CharInfo decode_invalid_utf8(const unsigned char *begin,
					const unsigned char *end) {
	if (begin[0] == 0xED
	    && begin + 3 <= end
	    && begin[1] >= 0xA0 && begin[1] < 0xB0
	    && begin[2] >= 0x80 && begin[2] < 0xC0
	    && (begin + 4 > end || begin[3] == 0xED)
	    && (begin + 5 > end || (begin[4] >= 0xB0 && begin[4] < 0xC0))
	    && (begin + 6 > end || (begin[5] >= 0x80 && begin[5] < 0xC0))) {
	    if (begin + 6 > end)
		return CharInfo(-1, begin);
	    else
		return CharInfo(0x10000
				+ ((begin[1] - 0xA0) << 16)
				+ ((begin[2] - 0x80) << 10)
				+ ((begin[4] - 0xB0) << 6)
				+ (begin[5] - 0x80), begin + 6);
	} else
	    return UTF8::decode_invalid_utf8(begin, end);
    }
};

struct Windows1252 {
    static const uint16_t c1_mapping[32];

    static const unsigned char *skip_utf8(const unsigned char *begin,
					  const unsigned char *end) {
	while (begin != end && *begin < 0x80)
	    ++begin;
	return begin;
    }

    static CharInfo decode_invalid_utf8(const unsigned char *begin,
					const unsigned char *end) {
	(void) end;
	if (*begin >= 0x80 && *begin < 0xA0)
	    return CharInfo(c1_mapping[*begin - 0x80], begin + 1);
	else
	    return CharInfo(*begin, begin + 1);
    }
};

struct UTF16BE {
    static const unsigned char *skip_utf8(const unsigned char *begin,
					  const unsigned char *end) {
	(void) end;
	return begin;
    }

    static CharInfo decode_to_utf8(const unsigned char *begin,
				   const unsigned char *end) {
	if (begin + 1 >= end)
	    return CharInfo(-1, begin);
	int ch = begin[0] * 256 + begin[1];
	if (likely(ch < 0xD800 || ch >= 0xE000))
	    return CharInfo(ch, begin + 2);
	else if (begin + 3 >= end && ch < 0xDC00)
	    return CharInfo(-1, begin);
	else if (ch >= 0xDC00 || begin[2] < 0xDC || begin[2] >= 0xE0)
	    return CharInfo(-2, begin + 2);
	else
	    return CharInfo(0x10000
			    + ((ch - 0xD800) << 10)
			    + ((begin[2] - 0xDC) << 8)
			    + begin[3], begin + 4);
    }
};

struct UTF16LE {
    static const unsigned char *skip_utf8(const unsigned char *begin,
					  const unsigned char *end) {
	(void) end;
	return begin;
    }

    static CharInfo decode_invalid_utf8(const unsigned char *begin,
					const unsigned char *end) {
	if (begin + 1 >= end)
	    return CharInfo(-1, begin);
	int ch = begin[1] * 256 + begin[0];
	if (likely(ch < 0xD800 || ch >= 0xE000))
	    return CharInfo(ch, begin + 2);
	else if (begin + 3 >= end && ch < 0xDC00)
	    return CharInfo(-1, begin);
	else if (ch >= 0xDC00 || begin[3] < 0xDC || begin[3] >= 0xE0)
	    return CharInfo(-2, begin + 2);
	else
	    return CharInfo(0x10000
			    + ((ch - 0xD800) << 10)
			    + ((begin[3] - 0xDC) << 8)
			    + begin[2], begin + 4);
    }
};


template <typename SOURCE_ENCODING>
bool
UTF8Encoder<SOURCE_ENCODING>::flush_clear()
{
    const unsigned char *bufend = buffer_end();
    if (buffer_begin() != bufend) {
	if (!store_utf8(-2, output_end()))
	    return false;
	consume_buffer(bufend);
    }
    return true;
}

template <typename SOURCE_ENCODING>
const unsigned char *
UTF8Encoder<SOURCE_ENCODING>::flush(const unsigned char *begin,
				    const unsigned char *end)
{
    CharInfo ci;
    const unsigned char *buf = buffer_begin();
    const unsigned char *bufend;

    while ((bufend = buffer_end()) != buf) {
	ci.pos = this->skip_utf8(buf, bufend);
	if (ci.pos != buf) {
	    if (strip_bom() && ci.pos == buf + 3
		&& buf[0] == 0xEF && buf[1] == 0xBB && buf[2] == 0xBF)
		/* skip BOM */;
	    else if (!store_copy(buf, ci.pos))
		return begin;
	} else {
	    ci = this->decode_invalid_utf8(buf, bufend);
	    if (ci.ch != -1) {
		if (strip_bom() && ci.ch == 0xFEFF)
		    /* skip BOM */;
		else if (!store_utf8(ci.ch, end))
		    return begin;
	    } else if (begin != end)
		append_buffer(*begin++);
	    else
		return begin;
	}

	consume_buffer(ci.pos);
	set_strip_bom(false);
    }

    return begin;
}

template <typename SOURCE_ENCODING>
const unsigned char *
UTF8Encoder<SOURCE_ENCODING>::encode(const unsigned char *begin,
				     const unsigned char *end)
{
    while (1) {
	const unsigned char *s = this->skip_utf8(begin, end);
	if (s != begin) {
	    if (!store_copy(begin, s))
		return begin;
	    begin = s;
	}

	if (begin == end)
	    return begin;

	CharInfo ci = this->decode_invalid_utf8(begin, end);
	if (likely(ci.ch != -1)) {
	    if (!store_utf8(ci.ch, ci.pos))
		return begin;
	    begin = ci.pos;
	} else {
	    set_buffer(begin, end);
	    return end;
	}
    }
}

}

#endif
