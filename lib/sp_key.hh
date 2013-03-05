#ifndef SP_KEY_HH_
#define SP_KEY_HH_
#include "straccum.hh"
#include <vector>
#include <string>
#include <sstream>
#include <iostream>


namespace pq {

inline void append_spkey(StringAccum &) {
}

template <typename T, typename... ARGS>
inline void append_spkey(StringAccum &sa, const T &x, ARGS... args) {
    sa << '|' << x;
    append_spkey(sa, args...);
}

inline String make_spkey() {
    return String();
}

template <typename... ARGS>
inline String make_spkey(const String &prefix, ARGS... args) {
    StringAccum sa(prefix);
    append_spkey(sa, args...);
    return sa.take_string();
}

template <typename... ARGS>
inline String make_spkey_first(const String &prefix, ARGS... args) {
    StringAccum sa(prefix);
    append_spkey(sa, args...);
    sa << '|';
    return sa.take_string();
}

template <typename... ARGS>
inline String make_spkey_last(const String &prefix, ARGS... args) {
    StringAccum sa(prefix);
    append_spkey(sa, args...);
    sa << '}';
    return sa.take_string();
}

String extract_spkey(int field, const String &spkey);
String extract_spkey(int field, const std::string &spkey);


class SPKey {

    public:

        SPKey() { }

        SPKey(const String &key) : key_(key) {
            unmarshall();
        }

        template <typename... ARGS>
        SPKey(const String &prefix, ARGS... args) : prefix_(prefix) {
            set_args(args...);
            marshall();
        }

        SPKey(const String &prefix, const std::vector<String> &args) : prefix_(prefix), args_(args) {
            marshall();
        }

        const String &key() const {
            return key_;
        }

        const String &prefix() const {
            return prefix_;
        }

        const std::vector<String> &args() const {
            return args_;
        }

	template<typename... ARGS>
	static String construct(const String &prefix, ARGS... args) {
	    std::stringstream ss;
            ss << prefix;
            return append_args(ss, args...).str().c_str();
        }

	static String construct(const String &prefix, const std::vector<String> &args) {

	    std::stringstream ss;
            ss << prefix;

            for (auto a = args.begin(); a != args.end(); ++a) {
                ss << "|" << *a;
            }

            return ss.str().c_str();
        }

    private:

        void unmarshall() {

            mandatory_assert(!key_.empty());

            int32_t p = 0;
            int32_t r = key_.find_left("|");

            do {

                if (r < 0) {
                    r = key_.length();
                }

                if (prefix_.empty()) {
                   prefix_ = key_.substring(p, r-p);
                }
                else {
                    args_.push_back(key_.substring(p, r-p));
                }

                p = r + 1;
                r = key_.find_left("|", p);
            }
            while(p < key_.length());
        }

        void marshall() {

            mandatory_assert(!prefix_.empty());
            key_ = construct(prefix_, args_);
        }

        template <typename... ARGS>
        static std::stringstream &append_args(std::stringstream &ss, const String &arg, ARGS... args) {
            ss << "|" << arg;
            return append_args(ss, args...);
        }

	static std::stringstream &append_args(std::stringstream &ss) {
            return ss;
        }

        template <typename... ARGS>
        void set_args(const String &arg, ARGS... rest) {

            args_.push_back(arg);
            set_args(rest...);
        }

        void set_args() { }

        String key_;
        String prefix_;
        std::vector<String> args_;
};

}

#endif
