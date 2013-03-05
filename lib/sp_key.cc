#include "sp_key.hh"
#include <string.h>

namespace pq {

String extract_spkey(int field, const String &spkey) {
    const char *s = spkey.begin(), *e = spkey.end();
    while (field > 0) {
	if ((s = reinterpret_cast<const char *>(memchr(s, '|', e - s)))) {
	    ++s;
	    --field;
	} else
	    return String();
    }
    const char *x = reinterpret_cast<const char *>(memchr(s, '|', e - s));
    return spkey.substring(s, x ? x : e);
}

String extract_spkey(int field, const std::string &spkey) {
    const char *s = spkey.data(), *e = spkey.data() + spkey.length();
    while (field > 0) {
	if ((s = reinterpret_cast<const char *>(memchr(s, '|', e - s)))) {
	    ++s;
	    --field;
	} else
	    return String();
    }
    const char *x = reinterpret_cast<const char *>(memchr(s, '|', e - s));
    return String(s, x ? x : e);
}

}
