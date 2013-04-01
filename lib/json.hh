// -*- c-basic-offset: 4 -*-
#ifndef JSON_HH
#define JSON_HH 1
#include "straccum.hh"
#include "str.hh"
#include "hashtable.hh"
#include <vector>
#include <stdlib.h>

template <typename P> class Json_proxy_base;
template <typename T> class Json_object_proxy;
template <typename T> class Json_object_str_proxy;
template <typename T> class Json_array_proxy;
class Json_get_proxy;

template <typename T, size_t S = sizeof(String::rep_type) - sizeof(T)>
struct Json_rep_item;
template <typename T> struct Json_rep_item<T, 4> {
    T x;
    int type;
};
template <typename T> struct Json_rep_item<T, 8> {
    T x;
    int padding;
    int type;
};

class Json {
    enum json_type { // order matters
        j_string = -1, j_null = 0,
        j_array = 1, j_object = 2, j_int = 3, j_double = 4, j_bool = 5
    };

  public:
    static const Json null_json;

    typedef int size_type;

    typedef std::pair<const String, Json> object_value_type;
    class object_iterator;
    class const_object_iterator;

    typedef Json array_value_type;
    class array_iterator;
    class const_array_iterator;

    class iterator;
    class const_iterator;

    typedef bool (Json::*unspecified_bool_type)() const;
    class unparse_manipulator;

    // Constructors
    inline Json();
    inline Json(const Json& x);
    template <typename P> inline Json(const Json_proxy_base<P>& x);
#if HAVE_CXX_RVALUE_REFERENCES
    inline Json(Json&& x);
#endif
    inline Json(int x);
    inline Json(unsigned x);
    inline Json(long x);
    inline Json(unsigned long x);
    inline Json(long long x);
    inline Json(unsigned long long x);
    inline Json(double x);
    inline Json(bool x);
    inline Json(const String &x);
    inline Json(Str x);
    inline Json(const char* x);
    template <typename T> inline Json(const std::vector<T>& x);
    template <typename T> inline Json(T first, T last);
    template <typename T> inline Json(const HashTable<String, T>& x);
    inline ~Json();

    static inline const Json& make_null();
    static inline Json make_array();
    template <typename T, typename... U>
    static inline Json make_array(T first, U... rest);
    static inline Json make_array_reserve(int n);
    static inline Json make_object();
    static inline Json make_string(const String& x);
    static inline Json make_string(const char* s, int len);

    // Type information
    inline bool truthy() const;
    inline operator unspecified_bool_type() const;
    inline bool operator!() const;

    inline bool is_null() const;
    inline bool is_int() const;
    inline bool is_i() const;
    inline bool is_double() const;
    inline bool is_d() const;
    inline bool is_number() const;
    inline bool is_n() const;
    inline bool is_bool() const;
    inline bool is_b() const;
    inline bool is_string() const;
    inline bool is_s() const;
    inline bool is_array() const;
    inline bool is_a() const;
    inline bool is_object() const;
    inline bool is_o() const;
    inline bool is_primitive() const;

    inline bool empty() const;
    inline size_type size() const;
    inline bool shared() const;

    void clear();

    // Primitive extractors
    inline long to_i() const;
    inline uint64_t to_u64() const;
    inline bool to_i(int& x) const;
    inline bool to_i(unsigned& x) const;
    inline bool to_i(long& x) const;
    inline bool to_i(unsigned long& x) const;
    inline bool to_i(long long& x) const;
    inline bool to_i(unsigned long long& x) const;
    inline long as_i() const;
    inline long as_i(long default_value) const;

    inline double to_d() const;
    inline bool to_d(double& x) const;
    inline double as_d() const;
    inline double as_d(double default_value) const;

    inline bool to_b() const;
    inline bool to_b(bool& x) const;
    inline bool as_b() const;
    inline bool as_b(bool default_value) const;

    inline String to_s() const;
    inline bool to_s(Str& x) const;
    inline bool to_s(String& x) const;
    inline const String& as_s() const;
    inline const String& as_s(const String& default_value) const;

    // Object methods
    inline size_type count(Str key) const;
    inline const Json& get(Str key) const;
    inline Json& get_insert(const String& key);
    inline Json& get_insert(Str key);
    inline Json& get_insert(const char* key);

    inline long get_i(Str key) const;
    inline double get_d(Str key) const;
    inline bool get_b(Str key) const;
    inline String get_s(Str key) const;

    inline const Json_get_proxy get(Str key, Json& x) const;
    inline const Json_get_proxy get(Str key, int& x) const;
    inline const Json_get_proxy get(Str key, unsigned& x) const;
    inline const Json_get_proxy get(Str key, long& x) const;
    inline const Json_get_proxy get(Str key, unsigned long& x) const;
    inline const Json_get_proxy get(Str key, long long& x) const;
    inline const Json_get_proxy get(Str key, unsigned long long& x) const;
    inline const Json_get_proxy get(Str key, double& x) const;
    inline const Json_get_proxy get(Str key, bool& x) const;
    inline const Json_get_proxy get(Str key, Str& x) const;
    inline const Json_get_proxy get(Str key, String& x) const;

    const Json& operator[](Str key) const;
    inline Json_object_proxy<Json> operator[](const String& key);
    inline Json_object_str_proxy<Json> operator[](Str key);
    inline Json_object_str_proxy<Json> operator[](const char* key);

    inline const Json& at(Str key) const;
    inline Json& at_insert(const String& key);
    inline Json& at_insert(Str key);
    inline Json& at_insert(const char* key);

    template <typename T> inline Json& set(const String& key, T value);
#if HAVE_CXX_RVALUE_REFERENCES
    inline Json& set(const String& key, Json&& x);
#endif
    inline Json& unset(Str key);

    inline std::pair<object_iterator, bool> insert(const object_value_type& x);
    inline object_iterator insert(object_iterator position,
				  const object_value_type& x);
    inline object_iterator erase(object_iterator it);
    inline size_type erase(Str key);

    inline Json& merge(const Json& x);
    template <typename P> inline Json& merge(const Json_proxy_base<P>& x);

    // Array methods
    inline const Json& get(size_type x) const;
    inline Json& get_insert(size_type x);

    inline const Json& operator[](size_type x) const;
    inline Json_array_proxy<Json> operator[](size_type x);

    inline const Json& at(size_type x) const;
    inline Json& at_insert(size_type x);

    inline const Json& back() const;
    inline Json& back();

    template <typename T> inline Json& push_back(T x);
    template <typename P> inline Json& push_back(const Json_proxy_base<P>& x);
#if HAVE_CXX_RVALUE_REFERENCES
    inline Json& push_back(Json&& x);
#endif
    inline void pop_back();

    inline Json& insert_back();
    template <typename T, typename... U>
    inline Json& insert_back(T first, U... rest);

    inline Json* array_data();

    // Iteration
    inline const_object_iterator obegin() const;
    inline const_object_iterator oend() const;
    inline object_iterator obegin();
    inline object_iterator oend();
    inline const_object_iterator cobegin() const;
    inline const_object_iterator coend() const;

    inline const_array_iterator abegin() const;
    inline const_array_iterator aend() const;
    inline array_iterator abegin();
    inline array_iterator aend();
    inline const_array_iterator cabegin() const;
    inline const_array_iterator caend() const;

    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline iterator begin();
    inline iterator end();
    inline const_iterator cbegin() const;
    inline const_iterator cend() const;

    // Unparsing
    static inline unparse_manipulator indent_depth(int x);
    static inline unparse_manipulator tab_width(int x);
    static inline unparse_manipulator newline_terminator(bool x);
    static inline unparse_manipulator space_separator(bool x);

    inline String unparse() const;
    inline String unparse(const unparse_manipulator& m) const;
    inline void unparse(StringAccum& sa) const;
    inline void unparse(StringAccum& sa, const unparse_manipulator& m) const;

    // Parsing
    inline bool assign_parse(const String& str);
    inline bool assign_parse(const char* first, const char* last);

    static inline Json parse(const String& str);
    static inline Json parse(const char* first, const char* last);

    // Assignment
    inline Json& operator=(const Json& x);
#if HAVE_CXX_RVALUE_REFERENCES
    inline Json& operator=(Json&& x);
#endif
    template <typename P> inline Json& operator=(const Json_proxy_base<P>& x);

    inline Json& operator++();
    inline void operator++(int);
    inline Json& operator--();
    inline void operator--(int);
    inline Json& operator+=(int x);
    inline Json& operator+=(unsigned x);
    inline Json& operator+=(long x);
    inline Json& operator+=(unsigned long x);
    inline Json& operator+=(long long x);
    inline Json& operator+=(unsigned long long x);
    inline Json& operator+=(double x);
    inline Json& operator+=(const Json& x);
    inline Json& operator-=(int x);
    inline Json& operator-=(unsigned x);
    inline Json& operator-=(long x);
    inline Json& operator-=(unsigned long x);
    inline Json& operator-=(long long x);
    inline Json& operator-=(unsigned long long x);
    inline Json& operator-=(double x);
    inline Json& operator-=(const Json& x);

    friend bool operator==(const Json& a, const Json& b);

    inline void swap(Json& x);

  private:

    enum {
	st_initial = 0, st_array_initial = 1, st_array_delim = 2,
	st_array_value = 3, st_object_initial = 4, st_object_delim = 5,
	st_object_key = 6, st_object_colon = 7, st_object_value = 8,
	max_depth = 2048
    };

    struct ComplexJson {
	int refcount;
        int size;
	ComplexJson()
	    : refcount(1) {
	}
	inline void ref();
	inline void deref(json_type j);
      private:
	ComplexJson(const ComplexJson& x); // does not exist
    };

    struct ArrayJson;
    struct ObjectItem;
    struct ObjectJson;

    union rep_type {
        Json_rep_item<int64_t> i;
        Json_rep_item<uint64_t> u;
        Json_rep_item<double> d;
        String::rep_type str;
        struct {
            ArrayJson* a;
            int padding;
            int type;
        } a;
        struct {
            ObjectJson* o;
            int padding;
            int type;
        } o;
        struct {
            ComplexJson* c;
            int padding;
            int type;
        } x;
    } u_;

    inline ObjectJson* ojson() const;
    inline ArrayJson* ajson() const;

    long hard_to_i() const;
    uint64_t hard_to_u64() const;
    double hard_to_d() const;
    bool hard_to_b() const;
    String hard_to_s() const;
    inline void force_number();
    template <typename T> inline Json& add(T x);
    template <typename T> inline Json& subtract(T x);

    const Json& hard_get(Str key) const;
    const Json& hard_get(size_type x) const;
    Json& hard_get_insert(size_type x);

    inline void uniqueify_object(bool convert);
    void hard_uniqueify_object(bool convert);
    inline void uniqueify_array(bool convert, int ncap);
    void hard_uniqueify_array(bool convert, int ncap);

    static unparse_manipulator default_manipulator;
    bool unparse_is_complex() const;
    static void unparse_indent(StringAccum &sa, const unparse_manipulator &m, int depth);
    void hard_unparse(StringAccum &sa, const unparse_manipulator &m, int depth) const;

    static inline const char *skip_space(const char *s, const char *end);
    bool assign_parse(const String &str, const char *begin, const char *end);
    static const char *parse_string(String &result, const String &str, const char *s, const char *end);
    const char *parse_primitive(const String &str, const char *s, const char *end);

    friend class object_iterator;
    friend class const_object_iterator;
    friend class array_iterator;
    friend class const_array_iterator;
};


struct Json::ArrayJson : public ComplexJson {
    int capacity;
    Json a[0];

    inline ArrayJson(int cap)
        : capacity(cap) {
        size = 0;
    }
    static ArrayJson* make(int n);
    static void destroy(ArrayJson* a);
};

struct Json::ObjectItem {
    std::pair<const String, Json> v_;
    int next_;
    explicit ObjectItem(const String &key, const Json& value, int next)
	: v_(key, value), next_(next) {
    }
};

struct Json::ObjectJson : public ComplexJson {
    ObjectItem *os_;
    int n_;
    int capacity_;
    std::vector<int> hash_;
    ObjectJson()
	: os_(), n_(0), capacity_(0) {
        size = 0;
    }
    ObjectJson(const ObjectJson& x);
    ~ObjectJson();
    void grow(bool copy);
    int bucket(const char* s, int len) const {
	return String::hashcode(s, s + len) & (hash_.size() - 1);
    }
    ObjectItem& item(int p) const {
	return os_[p];
    }
    int find(const char* s, int len) const {
	if (hash_.size()) {
	    int p = hash_[bucket(s, len)];
	    while (p >= 0) {
		ObjectItem &oi = item(p);
		if (oi.v_.first.equals(s, len))
		    return p;
		p = oi.next_;
	    }
	}
	return -1;
    }
    int find_insert(const String& key, const Json& value);
    inline Json& get_insert(const String& key) {
	int p = find_insert(key, make_null());
	return item(p).v_.second;
    }
    Json& get_insert(Str key);
    void erase(int p);
    size_type erase(Str key);
    void rehash();
};

inline const Json& Json::make_null() {
    return null_json;
}

inline void Json::ComplexJson::ref() {
    if (refcount >= 0)
        ++refcount;
}

inline void Json::ComplexJson::deref(json_type j) {
    if (refcount >= 1 && --refcount == 0) {
	if (j == j_object)
	    delete static_cast<ObjectJson*>(this);
	else
            ArrayJson::destroy(static_cast<ArrayJson*>(this));
    }
}

inline Json::ArrayJson* Json::ajson() const {
    assert(u_.x.type == j_null || u_.x.type == j_array);
    return u_.a.a;
}

inline Json::ObjectJson* Json::ojson() const {
    assert(u_.x.type == j_null || u_.x.type == j_object);
    return u_.o.o;
}

inline void Json::uniqueify_array(bool convert, int ncap) {
    if (u_.x.type != j_array || !u_.a.a || u_.a.a->refcount != 1
        || (ncap > 0 && ncap > u_.a.a->capacity))
	hard_uniqueify_array(convert, ncap);
}

inline void Json::uniqueify_object(bool convert) {
    if (u_.x.type != j_object || !u_.o.o || u_.o.o->refcount != 1)
	hard_uniqueify_object(convert);
}


class Json::const_object_iterator { public:
    typedef std::pair<const String, Json> value_type;
    typedef const value_type* pointer_type;
    typedef const value_type& reference_type;
    typedef std::forward_iterator_tag iterator_category;

    const_object_iterator() {
    }
    typedef bool (const_object_iterator::*unspecified_bool_type)() const;
    operator unspecified_bool_type() const {
	return live() ? &const_object_iterator::live : 0;
    }
    bool live() const {
	return i_ >= 0;
    }
    const value_type& operator*() const {
	return j_->ojson()->item(i_).v_;
    }
    const value_type* operator->() const {
	return &(**this);
    }
    const String& key() const {
	return (**this).first;
    }
    const Json& value() const {
	return (**this).second;
    }
    void operator++() {
	++i_;
	fix();
    }
    void operator++(int) {
	++(*this);
    }
  private:
    const Json* j_;
    int i_;
    const_object_iterator(const Json* j, int i)
	: j_(j), i_(i) {
	if (i_ >= 0)
	    fix();
    }
    void fix() {
	ObjectJson* oj = j_->ojson();
    retry:
	if (!oj || i_ >= oj->n_)
	    i_ = -1;
	else if (oj->item(i_).next_ == -2) {
	    ++i_;
	    goto retry;
	}
    }
    friend class Json;
    friend bool operator==(const const_object_iterator&, const const_object_iterator&);
};

class Json::object_iterator : public const_object_iterator { public:
    typedef value_type* pointer_type;
    typedef value_type& reference_type;

    object_iterator() {
    }
    value_type& operator*() const {
	const_cast<Json*>(j_)->uniqueify_object(false);
	return j_->ojson()->item(i_).v_;
    }
    value_type* operator->() const {
	return &(**this);
    }
    Json& value() const {
	return (**this).second;
    }
  private:
    object_iterator(Json* j, int i)
	: const_object_iterator(j, i) {
    }
    friend class Json;
};

inline bool operator==(const Json::const_object_iterator& a, const Json::const_object_iterator& b) {
    return a.j_ == b.j_ && a.i_ == b.i_;
}

inline bool operator!=(const Json::const_object_iterator& a, const Json::const_object_iterator& b) {
    return !(a == b);
}

class Json::const_array_iterator { public:
    typedef Json::size_type difference_type;
    typedef Json value_type;
    typedef const Json* pointer_type;
    typedef const Json& reference_type;
    typedef std::random_access_iterator_tag iterator_category;

    const_array_iterator() {
    }
    typedef bool (const_array_iterator::*unspecified_bool_type)() const;
    operator unspecified_bool_type() const {
	return live() ? &const_array_iterator::live : 0;
    }
    bool live() const {
	ArrayJson* aj = j_->ajson();
	return aj && i_ < aj->size;
    }
    const Json& operator*() const {
	return j_->ajson()->a[i_];
    }
    const Json& operator[](difference_type i) const {
	return j_->ajson()->a[i_ + i];
    }
    const Json* operator->() const {
	return &(**this);
    }
    const Json& value() const {
	return **this;
    }
    void operator++(int) {
	++i_;
    }
    void operator++() {
	++i_;
    }
    void operator--(int) {
	--i_;
    }
    void operator--() {
	--i_;
    }
    const_array_iterator& operator+=(difference_type x) {
	i_ += x;
	return *this;
    }
    const_array_iterator& operator-=(difference_type x) {
	i_ -= x;
	return *this;
    }
  private:
    const Json* j_;
    int i_;
    const_array_iterator(const Json* j, int i)
	: j_(j), i_(i) {
    }
    friend class Json;
    friend class Json::array_iterator;
    friend bool operator==(const const_array_iterator&, const const_array_iterator&);
    friend bool operator<(const const_array_iterator&, const const_array_iterator&);
    friend difference_type operator-(const const_array_iterator&, const const_array_iterator&);
};

class Json::array_iterator : public const_array_iterator { public:
    typedef const Json* pointer_type;
    typedef const Json& reference_type;

    array_iterator() {
    }
    Json& operator*() const {
	const_cast<Json*>(j_)->uniqueify_array(false, 0);
	return j_->ajson()->a[i_];
    }
    Json& operator[](difference_type i) const {
	const_cast<Json*>(j_)->uniqueify_array(false, 0);
	return j_->ajson()->a[i_ + i];
    }
    Json* operator->() const {
	return &(**this);
    }
    Json& value() const {
	return **this;
    }
  private:
    array_iterator(Json* j, int i)
	: const_array_iterator(j, i) {
    }
    friend class Json;
};

inline bool operator==(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    return a.j_ == b.j_ && a.i_ == b.i_;
}

inline bool operator<(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    return a.j_ < b.j_ || (a.j_ == b.j_ && a.i_ < b.i_);
}

inline bool operator!=(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    return !(a == b);
}

inline bool operator<=(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    return !(b < a);
}

inline bool operator>(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    return b < a;
}

inline bool operator>=(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    return !(a < b);
}

inline Json::const_array_iterator operator+(Json::const_array_iterator a, Json::const_array_iterator::difference_type i) {
    return a += i;
}

inline Json::const_array_iterator operator-(Json::const_array_iterator a, Json::const_array_iterator::difference_type i) {
    return a -= i;
}

inline Json::const_array_iterator::difference_type operator-(const Json::const_array_iterator& a, const Json::const_array_iterator& b) {
    assert(a.j_ == b.j_);
    return a.i_ - b.i_;
}

class Json::const_iterator { public:
    typedef Pair<const String, Json&> value_type;
    typedef const value_type* pointer_type;
    typedef const value_type& reference_type;
#if CLICK_USERLEVEL
    typedef std::forward_iterator_tag iterator_category;
#endif

    const_iterator()
	: value_(String(), *(Json*) 0) {
    }
    typedef bool (const_iterator::*unspecified_bool_type)() const;
    operator unspecified_bool_type() const {
	return live() ? &const_iterator::live : 0;
    }
    bool live() const {
	return i_ >= 0;
    }
    const value_type& operator*() const {
	return value_;
    }
    const value_type* operator->() const {
	return &(**this);
    }
    const String& key() const {
	return (**this).first;
    }
    const Json& value() const {
	return (**this).second;
    }
    void operator++() {
	++i_;
	fix();
    }
    void operator++(int) {
	++(*this);
    }
  private:
    const Json* j_;
    int i_;
    value_type value_;
    const_iterator(const Json* j, int i)
	: j_(j), i_(i), value_(String(), *(Json*) 0) {
	if (i_ >= 0)
	    fix();
    }
    void fix() {
	if (j_->u_.x.type == j_object) {
	    ObjectJson* oj = j_->ojson();
	retry:
	    if (!oj || i_ >= oj->n_)
		i_ = -1;
	    else if (oj->item(i_).next_ == -2) {
		++i_;
		goto retry;
	    } else {
		value_.~Pair();
		new((void *) &value_) value_type(oj->item(i_).v_.first,
						 oj->item(i_).v_.second);
	    }
	} else {
	    ArrayJson *aj = j_->ajson();
	    if (!aj || unsigned(i_) >= unsigned(aj->size))
		i_ = -1;
	    else {
		value_.~Pair();
		new((void *) &value_) value_type(String(i_), aj->a[i_]);
	    }
	}
    }
    friend class Json;
    friend bool operator==(const const_iterator &, const const_iterator &);
};

class Json::iterator : public const_iterator { public:
    typedef value_type* pointer_type;
    typedef value_type& reference_type;

    iterator() {
    }
    value_type& operator*() const {
	if (j_->u_.x.c->refcount != 1)
            uniqueify();
	return const_cast<value_type&>(const_iterator::operator*());
    }
    value_type* operator->() const {
	return &(**this);
    }
    Json& value() const {
	return (**this).second;
    }
  private:
    iterator(Json *j, int i)
	: const_iterator(j, i) {
    }
    void uniqueify() const {
	if (j_->u_.x.type == j_object)
	    const_cast<Json*>(j_)->hard_uniqueify_object(false);
	else
	    const_cast<Json*>(j_)->hard_uniqueify_array(false, 0);
	const_cast<iterator*>(this)->fix();
    }
    friend class Json;
};

inline bool operator==(const Json::const_iterator& a, const Json::const_iterator& b) {
    return a.j_ == b.j_ && a.i_ == b.i_;
}

inline bool operator!=(const Json::const_iterator& a, const Json::const_iterator& b) {
    return !(a == b);
}


template <typename P>
class Json_proxy_base {
  public:
    const Json& cvalue() const {
	return static_cast<const P *>(this)->cvalue();
    }
    Json& value() {
	return static_cast<P *>(this)->value();
    }
    operator const Json&() const {
	return cvalue();
    }
    operator Json&() {
	return value();
    }
    bool truthy() const {
        return cvalue().truthy();
    }
    operator Json::unspecified_bool_type() const {
	return cvalue();
    }
    bool operator!() const {
	return !cvalue();
    }
    bool is_null() const {
	return cvalue().is_null();
    }
    bool is_int() const {
	return cvalue().is_int();
    }
    bool is_i() const {
	return cvalue().is_i();
    }
    bool is_double() const {
	return cvalue().is_double();
    }
    bool is_d() const {
	return cvalue().is_d();
    }
    bool is_number() const {
	return cvalue().is_number();
    }
    bool is_n() const {
	return cvalue().is_n();
    }
    bool is_bool() const {
	return cvalue().is_bool();
    }
    bool is_b() const {
	return cvalue().is_b();
    }
    bool is_string() const {
	return cvalue().is_string();
    }
    bool is_s() const {
	return cvalue().is_s();
    }
    bool is_array() const {
	return cvalue().is_array();
    }
    bool is_a() const {
	return cvalue().is_a();
    }
    bool is_object() const {
	return cvalue().is_object();
    }
    bool is_o() const {
	return cvalue().is_o();
    }
    bool is_primitive() const {
	return cvalue().is_primitive();
    }
    bool empty() const {
	return cvalue().empty();
    }
    Json::size_type size() const {
	return cvalue().size();
    }
    long to_i() const {
	return cvalue().to_i();
    }
    uint64_t to_u64() const {
	return cvalue().to_u64();
    }
    bool to_i(int& x) const {
	return cvalue().to_i(x);
    }
    bool to_i(unsigned& x) const {
	return cvalue().to_i(x);
    }
    bool to_i(long& x) const {
	return cvalue().to_i(x);
    }
    bool to_i(unsigned long& x) const {
	return cvalue().to_i(x);
    }
    bool to_i(long long& x) const {
	return cvalue().to_i(x);
    }
    bool to_i(unsigned long long& x) const {
	return cvalue().to_i(x);
    }
    long as_i() const {
	return cvalue().as_i();
    }
    long as_i(long default_value) const {
	return cvalue().as_i(default_value);
    }
    double to_d() const {
	return cvalue().to_d();
    }
    bool to_d(double& x) const {
	return cvalue().to_d(x);
    }
    double as_d() const {
	return cvalue().as_d();
    }
    double as_d(double default_value) const {
	return cvalue().as_d(default_value);
    }
    bool to_b() const {
	return cvalue().to_b();
    }
    bool to_b(bool& x) const {
	return cvalue().to_b(x);
    }
    bool as_b() const {
	return cvalue().as_b();
    }
    bool as_b(bool default_value) const {
	return cvalue().as_b(default_value);
    }
    String to_s() const {
	return cvalue().to_s();
    }
    bool to_s(Str& x) const {
	return cvalue().to_s(x);
    }
    bool to_s(String& x) const {
	return cvalue().to_s(x);
    }
    const String& as_s() const {
	return cvalue().as_s();
    }
    const String& as_s(const String& default_value) const {
	return cvalue().as_s(default_value);
    }
    Json::size_type count(Str key) const {
	return cvalue().count(key);
    }
    const Json& get(Str key) const {
	return cvalue().get(key);
    }
    Json& get_insert(const String& key) {
	return value().get_insert(key);
    }
    Json& get_insert(Str key) {
	return value().get_insert(key);
    }
    Json& get_insert(const char* key) {
	return value().get_insert(key);
    }
    long get_i(Str key) const {
	return cvalue().get_i(key);
    }
    double get_d(Str key) const {
	return cvalue().get_d(key);
    }
    bool get_b(Str key) const {
	return cvalue().get_b(key);
    }
    String get_s(Str key) const {
	return cvalue().get_s(key);
    }
    inline const Json_get_proxy get(Str key, Json& x) const;
    inline const Json_get_proxy get(Str key, int& x) const;
    inline const Json_get_proxy get(Str key, unsigned& x) const;
    inline const Json_get_proxy get(Str key, long& x) const;
    inline const Json_get_proxy get(Str key, unsigned long& x) const;
    inline const Json_get_proxy get(Str key, long long& x) const;
    inline const Json_get_proxy get(Str key, unsigned long long& x) const;
    inline const Json_get_proxy get(Str key, double& x) const;
    inline const Json_get_proxy get(Str key, bool& x) const;
    inline const Json_get_proxy get(Str key, Str& x) const;
    inline const Json_get_proxy get(Str key, String& x) const;
    const Json& operator[](Str key) const {
	return cvalue().get(key);
    }
    Json_object_proxy<P> operator[](const String& key) {
	return Json_object_proxy<P>(*static_cast<P*>(this), key);
    }
    Json_object_str_proxy<P> operator[](Str key) {
	return Json_object_str_proxy<P>(*static_cast<P*>(this), key);
    }
    Json_object_str_proxy<P> operator[](const char* key) {
	return Json_object_str_proxy<P>(*static_cast<P*>(this), key);
    }
    const Json& at(Str key) const {
	return cvalue().at(key);
    }
    Json& at_insert(const String& key) {
	return value().at_insert(key);
    }
    Json& at_insert(Str key) {
	return value().at_insert(key);
    }
    Json& at_insert(const char* key) {
	return value().at_insert(key);
    }
    template <typename T> inline Json& set(const String& key, T value) {
	return this->value().set(key, value);
    }
#if HAVE_CXX_RVALUE_REFERENCES
    inline Json& set(const String& key, Json&& value) {
	return this->value().set(key, std::move(value));
    }
#endif
    Json& unset(Str key) {
	return value().unset(key);
    }
    std::pair<Json::object_iterator, bool> insert(const Json::object_value_type &x) {
	return value().insert(x);
    }
    Json::object_iterator insert(Json::object_iterator position, const Json::object_value_type &x) {
	return value().insert(position, x);
    }
    Json::object_iterator erase(Json::object_iterator it) {
        return value().erase(it);
    }
    Json::size_type erase(Str key) {
	return value().erase(key);
    }
    Json& merge(const Json& x) {
	return value().merge(x);
    }
    template <typename P2> Json& merge(const Json_proxy_base<P2>& x) {
	return value().merge(x.cvalue());
    }
    const Json& get(Json::size_type x) const {
	return cvalue().get(x);
    }
    Json& get_insert(Json::size_type x) {
	return value().get_insert(x);
    }
    const Json& operator[](int key) const {
	return cvalue().at(key);
    }
    Json_array_proxy<P> operator[](int key) {
	return Json_array_proxy<P>(*static_cast<P*>(this), key);
    }
    const Json& at(Json::size_type x) const {
	return cvalue().at(x);
    }
    Json& at_insert(Json::size_type x) {
	return value().at_insert(x);
    }
    const Json& back() const {
	return cvalue().back();
    }
    Json& back() {
	return value().back();
    }
    template <typename T> Json& push_back(T x) {
	return value().push_back(x);
    }
    template <typename Q> inline Json& push_back(const Json_proxy_base<Q>& x) {
        return value().push_back(x);
    }
#if HAVE_CXX_RVALUE_REFERENCES
    Json& push_back(Json&& x) {
	return value().push_back(std::move(x));
    }
#endif
    void pop_back() {
	value().pop_back();
    }
    void unparse(StringAccum& sa) const {
	return cvalue().unparse(sa);
    }
    void unparse(StringAccum& sa, const Json::unparse_manipulator& m) const {
	return cvalue().unparse(sa, m);
    }
    String unparse() const {
	return cvalue().unparse();
    }
    String unparse(const Json::unparse_manipulator& m) const {
	return cvalue().unparse(m);
    }
    bool assign_parse(const String& str) {
	return value().assign_parse(str);
    }
    bool assign_parse(const char* first, const char* last) {
	return value().assign_parse(first, last);
    }
    Json& operator++() {
	return ++value();
    }
    void operator++(int) {
	value()++;
    }
    Json& operator--() {
	return --value();
    }
    void operator--(int) {
	value()--;
    }
    Json& operator+=(int x) {
	return value() += x;
    }
    Json& operator+=(unsigned x) {
	return value() += x;
    }
    Json& operator+=(long x) {
	return value() += x;
    }
    Json& operator+=(unsigned long x) {
	return value() += x;
    }
    Json& operator+=(long long x) {
	return value() += x;
    }
    Json& operator+=(unsigned long long x) {
	return value() += x;
    }
    Json& operator+=(double x) {
	return value() += x;
    }
    Json& operator+=(const Json& x) {
	return value() += x;
    }
    Json& operator-=(int x) {
	return value() -= x;
    }
    Json& operator-=(unsigned x) {
	return value() -= x;
    }
    Json& operator-=(long x) {
	return value() -= x;
    }
    Json& operator-=(unsigned long x) {
	return value() -= x;
    }
    Json& operator-=(long long x) {
	return value() -= x;
    }
    Json& operator-=(unsigned long long x) {
	return value() -= x;
    }
    Json& operator-=(double x) {
	return value() -= x;
    }
    Json& operator-=(const Json& x) {
	return value() -= x;
    }
    Json::const_object_iterator obegin() const {
	return cvalue().obegin();
    }
    Json::const_object_iterator oend() const {
	return cvalue().oend();
    }
    Json::object_iterator obegin() {
	return value().obegin();
    }
    Json::object_iterator oend() {
	return value().oend();
    }
    Json::const_object_iterator cobegin() const {
	return cvalue().cobegin();
    }
    Json::const_object_iterator coend() const {
	return cvalue().coend();
    }
    Json::const_array_iterator abegin() const {
	return cvalue().abegin();
    }
    Json::const_array_iterator aend() const {
	return cvalue().aend();
    }
    Json::array_iterator abegin() {
	return value().abegin();
    }
    Json::array_iterator aend() {
	return value().aend();
    }
    Json::const_array_iterator cabegin() const {
	return cvalue().cabegin();
    }
    Json::const_array_iterator caend() const {
	return cvalue().caend();
    }
    Json::const_iterator begin() const {
	return cvalue().begin();
    }
    Json::const_iterator end() const {
	return cvalue().end();
    }
    Json::iterator begin() {
	return value().begin();
    }
    Json::iterator end() {
	return value().end();
    }
    Json::const_iterator cbegin() const {
	return cvalue().cbegin();
    }
    Json::const_iterator cend() const {
	return cvalue().cend();
    }
};

template <typename T>
class Json_object_proxy : public Json_proxy_base<Json_object_proxy<T> > {
  public:
    const Json& cvalue() const {
	return base_.get(key_);
    }
    Json& value() {
	return base_.get_insert(key_);
    }
    Json& operator=(const Json& x) {
	return value() = x;
    }
#if HAVE_CXX_RVALUE_REFERENCES
    Json& operator=(Json&& x) {
	return value() = std::move(x);
    }
#endif
    Json& operator=(const Json_object_proxy<T>& x) {
	return value() = x.cvalue();
    }
    template <typename P> Json& operator=(const Json_proxy_base<P>& x) {
	return value() = x.cvalue();
    }
    Json_object_proxy(T& ref, const String& key)
	: base_(ref), key_(key) {
    }
    T &base_;
    String key_;
};

template <typename T>
class Json_object_str_proxy : public Json_proxy_base<Json_object_str_proxy<T> > {
  public:
    const Json& cvalue() const {
	return base_.get(key_);
    }
    Json& value() {
	return base_.get_insert(key_);
    }
    Json& operator=(const Json& x) {
	return value() = x;
    }
#if HAVE_CXX_RVALUE_REFERENCES
    Json& operator=(Json&& x) {
	return value() = std::move(x);
    }
#endif
    Json& operator=(const Json_object_str_proxy<T>& x) {
	return value() = x.cvalue();
    }
    template <typename P> Json& operator=(const Json_proxy_base<P>& x) {
	return value() = x.cvalue();
    }
    Json_object_str_proxy(T& ref, Str key)
	: base_(ref), key_(key) {
    }
    T &base_;
    Str key_;
};

template <typename T>
class Json_array_proxy : public Json_proxy_base<Json_array_proxy<T> > {
  public:
    const Json& cvalue() const {
	return base_.get(key_);
    }
    Json& value() {
	return base_.get_insert(key_);
    }
    Json& operator=(const Json& x) {
	return value() = x;
    }
#if HAVE_CXX_RVALUE_REFERENCES
    Json& operator=(Json&& x) {
	return value() = std::move(x);
    }
#endif
    Json& operator=(const Json_array_proxy<T>& x) {
	return value() = x.cvalue();
    }
    template <typename P> Json& operator=(const Json_proxy_base<P>& x) {
	return value() = x.cvalue();
    }
    Json_array_proxy(T& ref, int key)
	: base_(ref), key_(key) {
    }
    T &base_;
    int key_;
};

class Json_get_proxy : public Json_proxy_base<Json_get_proxy> {
  public:
    const Json& cvalue() const {
	return base_;
    }
    operator Json::unspecified_bool_type() const {
	return status_ ? &Json::is_null : 0;
    }
    bool operator!() const {
	return !status_;
    }
    bool status() const {
	return status_;
    }
    const Json_get_proxy& status(bool& x) const {
	x = status_;
	return *this;
    }
    Json_get_proxy(const Json& ref, bool status)
	: base_(ref), status_(status) {
    }
    const Json& base_;
    bool status_;
  private:
    Json_get_proxy& operator=(const Json_get_proxy& x);
};

template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, Json& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, int& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, unsigned& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, long& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, unsigned long& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, long long& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, unsigned long long& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, double& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, bool& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, Str& x) const {
    return cvalue().get(key, x);
}
template <typename T>
inline const Json_get_proxy Json_proxy_base<T>::get(Str key, String& x) const {
    return cvalue().get(key, x);
}


/** @brief Construct a null Json. */
inline Json::Json() {
    memset(&u_, 0, sizeof(u_));
}
/** @brief Construct a Json copy of @a x. */
inline Json::Json(const Json& x)
    : u_(x.u_) {
    if (u_.x.type < 0)
        u_.str.ref();
    if (u_.x.c && (u_.x.type == j_array || u_.x.type == j_object))
        u_.x.c->ref();
}
/** @overload */
template <typename P> inline Json::Json(const Json_proxy_base<P>& x)
    : u_(x.cvalue().u_) {
    if (u_.x.type < 0)
        u_.str.ref();
    if (u_.x.c && (u_.x.type == j_array || u_.x.type == j_object))
        u_.x.c->ref();
}
#if HAVE_CXX_RVALUE_REFERENCES
/** @overload */
inline Json::Json(Json&& x)
    : u_(std::move(x.u_)) {
    x.u_.x.type = 0;
}
#endif
/** @brief Construct simple Json values. */
inline Json::Json(int x) {
    u_.i.x = x;
    u_.i.type = j_int;
}
inline Json::Json(unsigned x) {
    u_.u.x = x;
    u_.u.type = j_int;
}
inline Json::Json(long x) {
    u_.i.x = x;
    u_.i.type = j_int;
}
inline Json::Json(unsigned long x) {
    u_.u.x = x;
    u_.u.type = j_int;
}
inline Json::Json(long long x) {
    u_.i.x = x;
    u_.i.type = j_int;
}
inline Json::Json(unsigned long long x) {
    u_.u.x = x;
    u_.u.type = j_int;
}
inline Json::Json(double x) {
    u_.d.x = x;
    u_.d.type = j_double;
}
inline Json::Json(bool x) {
    u_.i.x = x;
    u_.i.type = j_bool;
}
inline Json::Json(const String& x) {
    u_.str = x.internal_rep();
    u_.str.ref();
}
inline Json::Json(Str x) {
    u_.str.reset_ref();
    String str(x);
    str.swap(u_.str);
}
inline Json::Json(const char* x) {
    u_.str.reset_ref();
    String str(x);
    str.swap(u_.str);
}
/** @brief Construct an array Json containing the elements of @a x. */
template <typename T>
inline Json::Json(const std::vector<T> &x) {
    u_.a.type = j_array;
    u_.a.a = ArrayJson::make(int(x.size()));
    for (typename std::vector<T>::const_iterator it = x.begin();
         it != x.end(); ++it) {
	new((void*) &u_.a.a->a[u_.a.a->size]) Json(*it);
        ++u_.a.a->size;
    }
}
/** @brief Construct an array Json containing the elements in [@a first,
    @a last). */
template <typename T>
inline Json::Json(T first, T last) {
    u_.a.type = j_array;
    u_.a.a = ArrayJson::make(0);
    while (first != last) {
        if (u_.a.a->size == u_.a.a->capacity)
            hard_uniqueify_array(false, u_.a.a->size + 1);
        new((void*) &u_.a.a->a[u_.a.a->size]) Json(*first);
        ++u_.a.a->size;
	++first;
    }
}
/** @brief Construct an object Json containing the values in @a x. */
template <typename T>
inline Json::Json(const HashTable<String, T> &x) {
    u_.o.type = j_object;
    u_.o.o = new ObjectJson;
    for (typename HashTable<String, T>::const_iterator it = x.begin();
	 it != x.end(); ++it) {
	Json& x = ojson()->get_insert(it.key());
	x = Json(it.value());
    }
}
inline Json::~Json() {
    if (u_.x.type < 0)
        u_.str.deref();
    else if (u_.x.c && (u_.x.type == j_array || u_.x.type == j_object))
        u_.x.c->deref((json_type) u_.x.type);
}

/** @brief Return an empty array-valued Json. */
inline Json Json::make_array() {
    Json j;
    j.u_.x.type = j_array;
    return j;
}
/** @brief Return an array-valued Json containing [first, rest...]. */
template <typename T, typename... U>
inline Json Json::make_array(T first, U... rest) {
    Json j;
    j.u_.x.type = j_array;
    j.push_back(first);
    j.insert_back(rest...);
    return j;
}
/** @brief Return an empty array-valued Json with reserved space for @a n items. */
inline Json Json::make_array_reserve(int n) {
    Json j;
    j.u_.a.type = j_array;
    j.u_.a.a = n ? ArrayJson::make(n) : 0;
    return j;
}
/** @brief Return an empty object-valued Json. */
inline Json Json::make_object() {
    Json j;
    j.u_.o.type = j_object;
    return j;
}
/** @brief Return a string-valued Json. */
inline Json Json::make_string(const String &x) {
    return Json(x);
}
/** @overload */
inline Json Json::make_string(const char *s, int len) {
    return Json(String(s, len));
}

/** @brief Test if this Json is truthy. */
inline bool Json::truthy() const {
    return (u_.x.c ? u_.x.type >= 0 || u_.str.length
            : (unsigned) (u_.x.type - 1) < (unsigned) (j_int - 1));
}
/** @brief Test if this Json is truthy.
    @sa empty() */
inline Json::operator unspecified_bool_type() const {
    return truthy() ? &Json::is_null : 0;
}
/** @brief Return true if this Json is null. */
inline bool Json::operator!() const {
    return !truthy();
}

/** @brief Test this Json's type. */
inline bool Json::is_null() const {
    return !u_.x.c && u_.x.type == j_null;
}
inline bool Json::is_int() const {
    return u_.x.type == j_int;
}
inline bool Json::is_i() const {
    return u_.x.type == j_int;
}
inline bool Json::is_double() const {
    return u_.x.type == j_double;
}
inline bool Json::is_d() const {
    return u_.x.type == j_double;
}
inline bool Json::is_number() const {
    return is_int() || is_double();
}
inline bool Json::is_n() const {
    return is_int() || is_double();
}
inline bool Json::is_bool() const {
    return u_.x.type == j_bool;
}
inline bool Json::is_b() const {
    return u_.x.type == j_bool;
}
inline bool Json::is_string() const {
    return u_.x.c && u_.x.type <= 0;
}
inline bool Json::is_s() const {
    return u_.x.c && u_.x.type <= 0;
}
inline bool Json::is_array() const {
    return u_.x.type == j_array;
}
inline bool Json::is_a() const {
    return u_.x.type == j_array;
}
inline bool Json::is_object() const {
    return u_.x.type == j_object;
}
inline bool Json::is_o() const {
    return u_.x.type == j_object;
}
/** @brief Test if this Json is a primitive value, not including null. */
inline bool Json::is_primitive() const {
    return u_.x.type >= j_int || (u_.x.c && u_.x.type <= 0);
}

/** @brief Return true if this Json is null, an empty array, or an empty
    object. */
inline bool Json::empty() const {
    return unsigned(u_.x.type) < unsigned(j_int)
        && (!u_.x.c || u_.x.c->size == 0);
}
/** @brief Return the number of elements in this complex Json.
    @pre is_array() || is_object() || is_null() */
inline Json::size_type Json::size() const {
    assert(unsigned(u_.x.type) < unsigned(j_int));
    return u_.x.c ? u_.x.c->size : 0;
}
/** @brief Test if this complex Json is shared. */
inline bool Json::shared() const {
    return u_.x.c && (u_.x.type == j_array || u_.x.type == j_object)
        && u_.x.c->refcount != 1;
}

// Primitive methods

/** @brief Return this Json converted to an integer.

    Converts any Json to an integer value. Numeric Jsons convert as you'd
    expect. Null Jsons convert to 0; false boolean Jsons to 0 and true
    boolean Jsons to 1; string Jsons to a number parsed from their initial
    portions; and array and object Jsons to size().
    @sa as_i() */
inline long Json::to_i() const {
    if (is_int())
	return u_.i.x;
    else
	return hard_to_i();
}

/** @brief Extract this integer Json's value into @a x.
    @param[out] x value storage
    @return True iff this Json stores an integer value.

    If false is returned (!is_number() or the number is not parseable as a
    pure integer), @a x remains unchanged. */
inline bool Json::to_i(int &x) const {
    if (is_int()) {
        x = u_.i.x;
        return true;
    } else if (is_double() && u_.d.x == double(int(u_.d.x))) {
        x = int(u_.d.x);
        return true;
    } else
        return false;
}

/** @overload */
inline bool Json::to_i(unsigned& x) const {
    if (is_int()) {
        x = u_.u.x;
        return true;
    } else if (is_double() && u_.d.x == double(unsigned(u_.d.x))) {
        x = unsigned(u_.d.x);
        return true;
    } else
        return false;
}

/** @overload */
inline bool Json::to_i(long& x) const {
    if (is_int()) {
        x = u_.i.x;
        return true;
    } else if (is_double() && u_.d.x == double(long(u_.d.x))) {
        x = long(u_.d.x);
        return true;
    } else
        return false;
}

/** @overload */
inline bool Json::to_i(unsigned long& x) const {
    if (is_int()) {
        x = u_.u.x;
        return true;
    } else if (is_double() && u_.d.x == double((unsigned long) u_.d.x)) {
        x = (unsigned long) u_.d.x;
        return true;
    } else
        return false;
}

/** @overload */
inline bool Json::to_i(long long& x) const {
    if (is_int()) {
        x = u_.i.x;
        return true;
    } else if (is_double() && u_.d.x == double((long long) u_.d.x)) {
        x = (long long) u_.d.x;
        return true;
    } else
        return false;
}

/** @overload */
inline bool Json::to_i(unsigned long long& x) const {
    if (is_int()) {
        x = u_.u.x;
        return true;
    } else if (is_double() && u_.d.x == double((unsigned long long) u_.d.x)) {
        x = (unsigned long long) u_.d.x;
        return true;
    } else
        return false;
}

/** @brief Return this Json converted to a 64-bit unsigned integer.

    See to_i() for the conversion rules. */
inline uint64_t Json::to_u64() const {
    if (is_int())
	return u_.i.x;
    else
	return hard_to_u64();
}

/** @brief Return the integer value of this numeric Json.
    @pre is_number()
    @sa to_i() */
inline long Json::as_i() const {
    assert(is_int() || is_double());
    return is_int() ? u_.i.x : long(u_.d.x);
}

/** @brief Return the integer value of this numeric Json or @a default_value. */
inline long Json::as_i(long default_value) const {
    if (is_int() || is_double())
        return as_i();
    else
        return default_value;
}

/** @brief Return this Json converted to a double.

    Converts any Json to an integer value. Numeric Jsons convert as you'd
    expect. Null Jsons convert to 0; false boolean Jsons to 0 and true
    boolean Jsons to 1; string Jsons to a number parsed from their initial
    portions; and array and object Jsons to size().
    @sa as_d() */
inline double Json::to_d() const {
    if (is_double())
        return u_.d.x;
    else
        return hard_to_d();
}

/** @brief Extract this numeric Json's value into @a x.
    @param[out] x value storage
    @return True iff is_number().

    If !is_number(), @a x remains unchanged. */
inline bool Json::to_d(double& x) const {
    if (is_double() || is_int()) {
	x = to_d();
	return true;
    } else
	return false;
}

/** @brief Return the double value of this numeric Json.
    @pre is_number()
    @sa to_d() */
inline double Json::as_d() const {
    assert(is_double() || is_int());
    return is_double() ? u_.d.x : double(u_.i.x);
}

/** @brief Return the double value of this numeric Json or @a default_value. */
inline double Json::as_d(double default_value) const {
    if (!is_double() && !is_int())
        return default_value;
    else
        return as_d();
}

/** @brief Return this Json converted to a boolean.

    Converts any Json to a boolean value. Boolean Jsons convert as you'd
    expect. Null Jsons convert to false; zero-valued numeric Jsons to false,
    and other numeric Jsons to true; empty string Jsons to false, and other
    string Jsons to true; and array and object Jsons to !empty().
    @sa as_b() */
inline bool Json::to_b() const {
    if (is_bool())
	return u_.i.x;
    else
	return hard_to_b();
}

/** @brief Extract this boolean Json's value into @a x.
    @param[out] x value storage
    @return True iff is_bool().

    If !is_bool(), @a x remains unchanged. */
inline bool Json::to_b(bool& x) const {
    if (is_bool()) {
	x = u_.i.x;
	return true;
    } else
	return false;
}

/** @brief Return the value of this boolean Json.
    @pre is_bool()
    @sa to_b() */
inline bool Json::as_b() const {
    assert(is_bool());
    return u_.i.x;
}

/** @brief Return the boolean value of this numeric Json or @a default_value. */
inline bool Json::as_b(bool default_value) const {
    if (is_bool())
        return as_b();
    else
        return default_value;
}

/** @brief Return this Json converted to a string.

    Converts any Json to a string value. String Jsons convert as you'd expect.
    Null Jsons convert to the empty string; numeric Jsons to their string
    values; boolean Jsons to "false" or "true"; and array and object Jsons to
    "[Array]" and "[Object]", respectively.
    @sa as_s() */
inline String Json::to_s() const {
    if (u_.x.type <= 0 && u_.x.c)
	return String(u_.str);
    else
	return hard_to_s();
}

/** @brief Extract this string Json's value into @a x.
    @param[out] x value storage
    @return True iff is_string().

    If !is_string(), @a x remains unchanged. */
inline bool Json::to_s(Str& x) const {
    if (u_.x.type <= 0 && u_.x.c) {
	x.assign(u_.str.data, u_.str.length);
	return true;
    } else
	return false;
}

/** @brief Extract this string Json's value into @a x.
    @param[out] x value storage
    @return True iff is_string().

    If !is_string(), @a x remains unchanged. */
inline bool Json::to_s(String& x) const {
    if (u_.x.type <= 0 && u_.x.c) {
        x.assign(u_.str);
	return true;
    } else
	return false;
}

/** @brief Return the value of this string Json.
    @pre is_string()
    @sa to_s() */
inline const String& Json::as_s() const {
    assert(u_.x.type <= 0 && u_.x.c);
    return reinterpret_cast<const String&>(u_.str);
}

/** @brief Return the value of this string Json or @a default_value. */
inline const String& Json::as_s(const String& default_value) const {
    if (u_.x.type > 0 || !u_.x.c)
        return default_value;
    else
        return as_s();
}

inline void Json::force_number() {
    assert((u_.x.type == j_null && !u_.x.c) || u_.x.type == j_int || u_.x.type == j_double);
    if (u_.x.type == j_null && !u_.x.c)
	u_.x.type = j_int;
}


// Object methods

/** @brief Return 1 if this object Json contains @a key, 0 otherwise.

    Returns 0 if this is not an object Json. */
inline Json::size_type Json::count(Str key) const {
    assert(u_.x.type == j_null || u_.x.type == j_object);
    return u_.o.o ? ojson()->find(key.data(), key.length()) >= 0 : 0;
}

/** @brief Return the value at @a key in an object or array Json.

    If this is an array Json, and @a key is the simplest base-10
    representation of an integer <em>i</em>, then returns get(<em>i</em>). If
    this is neither an array nor an object, returns a null Json. */
inline const Json& Json::get(Str key) const {
    int i;
    ObjectJson *oj;
    if (is_object() && (oj = ojson())
	&& (i = oj->find(key.data(), key.length())) >= 0)
	return oj->item(i).v_.second;
    else
	return hard_get(key);
}

/** @brief Return a reference to the value of @a key in an object Json.

    This Json is first converted to an object. Arrays are converted to objects
    with numeric keys. Other types of Json are converted to empty objects.
    If !count(@a key), then a null Json is inserted at @a key. */
inline Json& Json::get_insert(const String &key) {
    uniqueify_object(true);
    return ojson()->get_insert(key);
}

/** @overload */
inline Json& Json::get_insert(Str key) {
    uniqueify_object(true);
    return ojson()->get_insert(key);
}

/** @overload */
inline Json& Json::get_insert(const char *key) {
    uniqueify_object(true);
    return ojson()->get_insert(Str(key));
}

/** @brief Return get(@a key).to_i(). */
inline long Json::get_i(Str key) const {
    return get(key).to_i();
}

/** @brief Return get(@a key).to_d(). */
inline double Json::get_d(Str key) const {
    return get(key).to_d();
}

/** @brief Return get(@a key).to_b(). */
inline bool Json::get_b(Str key) const {
    return get(key).to_b();
}

/** @brief Return get(@a key).to_s(). */
inline String Json::get_s(Str key) const {
    return get(key).to_s();
}

/** @brief Extract this object Json's value at @a key into @a x.
    @param[out] x value storage
    @return proxy for *this

    @a x is assigned iff contains(@a key). The return value is a proxy
    object that mostly behaves like *this. However, the proxy is "truthy"
    iff contains(@a key) and @a x was assigned. The proxy also has status()
    methods that return the extraction status. For example:

    <code>
    Json j = Json::parse("{\"a\":1,\"b\":2}"), x;
    assert(j.get("a", x));            // extraction succeeded
    assert(x == Json(1));
    assert(!j.get("c", x));           // no "c" key
    assert(x == Json(1));             // x remains unchanged
    assert(!j.get("c", x).status());  // can use ".status()" for clarity

    // Can chain .get() methods to extract multiple values
    Json a, b, c;
    j.get("a", a).get("b", b);
    assert(a == Json(1) && b == Json(2));

    // Use .status() to return or assign extraction status
    bool a_status, b_status, c_status;
    j.get("a", a).status(a_status)
     .get("b", b).status(b_status)
     .get("c", c).status(c_status);
    assert(a_status && b_status && !c_status);
    </code>

    Overloaded versions of @a get() can extract integer, double, boolean,
    and string values for specific keys. These versions succeed iff
    contains(@a key) and the corresponding value has the expected type. For
    example:

    <code>
    Json j = Json::parse("{\"a\":1,\"b\":\"\"}");
    int a, b;
    bool a_status, b_status;
    j.get("a", a).status(a_status).get("b", b).status(b_status);
    assert(a_status && a == 1 && !b_status);
    </code> */
inline const Json_get_proxy Json::get(Str key, Json& x) const {
    int i;
    ObjectJson *oj;
    if (is_object() && (oj = ojson())
	&& (i = oj->find(key.data(), key.length())) >= 0) {
	x = oj->item(i).v_.second;
	return Json_get_proxy(*this, true);
    } else
	return Json_get_proxy(*this, false);
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, int &x) const {
    return Json_get_proxy(*this, get(key).to_i(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, unsigned& x) const {
    return Json_get_proxy(*this, get(key).to_i(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, long& x) const {
    return Json_get_proxy(*this, get(key).to_i(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, unsigned long& x) const {
    return Json_get_proxy(*this, get(key).to_i(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, long long& x) const {
    return Json_get_proxy(*this, get(key).to_i(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, unsigned long long& x) const {
    return Json_get_proxy(*this, get(key).to_i(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, double& x) const {
    return Json_get_proxy(*this, get(key).to_d(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, bool& x) const {
    return Json_get_proxy(*this, get(key).to_b(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, Str& x) const {
    return Json_get_proxy(*this, get(key).to_s(x));
}

/** @overload */
inline const Json_get_proxy Json::get(Str key, String& x) const {
    return Json_get_proxy(*this, get(key).to_s(x));
}


/** @brief Return the value at @a key in an object or array Json.
    @sa Json::get() */
inline const Json& Json::operator[](Str key) const {
    return get(key);
}

/** @brief Return a proxy reference to the value at @a key in an object Json.

    Returns the current @a key value if it exists. Otherwise, returns a proxy
    that acts like a null Json. If this proxy is assigned, this Json is
    converted to an object as by get_insert(), and then extended as necessary
    to contain the new value. */
inline Json_object_proxy<Json> Json::operator[](const String& key) {
    return Json_object_proxy<Json>(*this, key);
}

/** @overload */
inline Json_object_str_proxy<Json> Json::operator[](Str key) {
    return Json_object_str_proxy<Json>(*this, key);
}

/** @overload */
inline Json_object_str_proxy<Json> Json::operator[](const char* key) {
    return Json_object_str_proxy<Json>(*this, key);
}

/** @brief Return the value at @a key in an object Json.
    @pre is_object() && count(@a key) */
inline const Json& Json::at(Str key) const {
    assert(is_object() && u_.o.o);
    ObjectJson *oj = ojson();
    int i = oj->find(key.data(), key.length());
    assert(i >= 0);
    return oj->item(i).v_.second;
}

/** @brief Return a reference to the value at @a key in an object Json.
    @pre is_object()

    Returns a newly-inserted null Json if !count(@a key). */
inline Json& Json::at_insert(const String &key) {
    assert(is_object());
    return get_insert(key);
}

/** @overload */
inline Json& Json::at_insert(Str key) {
    assert(is_object());
    return get_insert(key);
}

/** @overload */
inline Json& Json::at_insert(const char *key) {
    assert(is_object());
    return get_insert(Str(key));
}

/** @brief Set the value of @a key to @a value in this object Json.
    @return this Json

    An array Json is converted to an object Json with numeric keys. Other
    non-object Jsons are converted to empty objects. */
template <typename T> inline Json& Json::set(const String& key, T value) {
    uniqueify_object(true);
    ojson()->get_insert(key) = Json(value);
    return *this;
}

#if HAVE_CXX_RVALUE_REFERENCES
/** @overload */
inline Json& Json::set(const String& key, Json&& value) {
    uniqueify_object(true);
    ojson()->get_insert(key) = std::move(value);
    return *this;
}
#endif

/** @brief Remove the value of @a key from an object Json.
    @return this Json
    @sa erase() */
inline Json& Json::unset(Str key) {
    if (is_object()) {
	uniqueify_object(true);
	ojson()->erase(key);
    }
    return *this;
}

/** @brief Insert element @a x in this object Json.
    @param x Pair of key and value.
    @return Pair of iterator pointing to key's value and bool indicating
    whether the value is newly inserted.
    @pre is_object()

    An existing element with key @a x.first is not replaced. */
inline std::pair<Json::object_iterator, bool> Json::insert(const object_value_type& x) {
    assert(is_object());
    uniqueify_object(false);
    ObjectJson *oj = ojson();
    int n = oj->n_, i = oj->find_insert(x.first, x.second);
    return std::make_pair(object_iterator(this, i), i == n);
}

/** @brief Insert element @a x in this object Json.
    @param position Ignored.
    @param x Pair of key and value.
    @return Pair of iterator pointing to key's value and bool indicating
    whether the value is newly inserted.
    @pre is_object()

    An existing element with key @a x.first is not replaced. */
inline Json::object_iterator Json::insert(object_iterator position, const object_value_type& x) {
    (void) position;
    return insert(x).first;
}

/** @brief Remove the value pointed to by @a it from an object Json.
    @pre is_object()
    @return Next iterator */
inline Json::object_iterator Json::erase(Json::object_iterator it) {
    assert(is_object() && it.live() && it.j_ == this);
    uniqueify_object(false);
    ojson()->erase(it.i_);
    ++it;
    return it;
}

/** @brief Remove the value of @a key from an object Json.
    @pre is_object()
    @return Number of items removed */
inline Json::size_type Json::erase(Str key) {
    assert(is_object());
    uniqueify_object(false);
    return ojson()->erase(key);
}

/** @brief Merge the values of object Json @a x into this object Json.
    @pre (is_object() || is_null()) && (x.is_object() || x.is_null())
    @return this Json

    The key-value pairs in @a x are assigned to this Json. Null Jsons are
    silently converted to empty objects, except that if @a x and this Json are
    both null, then this Json is left as null. */
inline Json& Json::merge(const Json& x) {
    assert(is_object() || is_null());
    assert(x.is_object() || x.is_null());
    if (x.u_.o.o) {
	uniqueify_object(false);
	ObjectJson *oj = ojson(), *xoj = x.ojson();
	const ObjectItem *xb = xoj->os_, *xe = xb + xoj->n_;
	for (; xb != xe; ++xb)
	    if (xb->next_ > -2)
		oj->get_insert(xb->v_.first) = xb->v_.second;
    }
    return *this;
}

/** @cond never */
template <typename U>
inline Json& Json::merge(const Json_proxy_base<U>& x) {
    return merge(x.cvalue());
}
/** @endcond never */


// ARRAY METHODS

/** @brief Return the @a x th array element.

    If @a x is out of range of this array, returns a null Json. If this is an
    object Json, then returns get(String(@a x)). If this is neither an object
    nor an array, returns a null Json. */
inline const Json& Json::get(size_type x) const {
    ArrayJson *aj;
    if (u_.x.type == j_array && (aj = ajson()) && unsigned(x) < unsigned(aj->size))
	return aj->a[x];
    else
	return hard_get(x);
}

/** @brief Return a reference to the @a x th array element.

    If this Json is an object, returns get_insert(String(x)). Otherwise this
    Json is first converted to an array; non-arrays are converted to empty
    arrays. The array is extended if @a x is out of range. */
inline Json& Json::get_insert(size_type x) {
    ArrayJson *aj;
    if (u_.x.type == j_array && (aj = ajson()) && aj->refcount == 1
	&& unsigned(x) < unsigned(aj->size))
	return aj->a[x];
    else
	return hard_get_insert(x);
}

/** @brief Return the @a x th element in an array Json.
    @pre is_array()

    A null Json is treated like an empty array. */
inline const Json& Json::at(size_type x) const {
    assert(is_array());
    return get(x);
}

/** @brief Return a reference to the @a x th element in an array Json.
    @pre is_array()

    The array is extended if @a x is out of range. */
inline Json& Json::at_insert(size_type x) {
    assert(is_array());
    return get_insert(x);
}

/** @brief Return the @a x th array element.

    If @a x is out of range of this array, returns a null Json. If this is an
    object Json, then returns get(String(@a x)). If this is neither an object
    nor an array, returns a null Json. */
inline const Json& Json::operator[](size_type x) const {
    return get(x);
}

/** @brief Return a proxy reference to the @a x th array element.

    If this Json is an object, returns operator[](String(x)). If this Json is
    an array and @a x is in range, returns that element. Otherwise, returns a
    proxy that acts like a null Json. If this proxy is assigned, this Json is
    converted to an array, and then extended as necessary to contain the new
    value. */
inline Json_array_proxy<Json> Json::operator[](size_type x) {
    return Json_array_proxy<Json>(*this, x);
}

/** @brief Return the last array element.
    @pre is_array() && !empty() */
inline const Json& Json::back() const {
    assert(is_array() && u_.a.a && u_.a.a->size > 0);
    return u_.a.a->a[u_.a.a->size - 1];
}

/** @brief Return a reference to the last array element.
    @pre is_array() && !empty() */
inline Json& Json::back() {
    assert(is_array() && u_.a.a && u_.a.a->size > 0);
    uniqueify_array(false, 0);
    return u_.a.a->a[u_.a.a->size - 1];
}

/** @brief Push an element onto the back of the array.
    @pre is_array() || is_null()
    @return this Json

    A null Json is promoted to an array. */
template <typename T> inline Json& Json::push_back(T x) {
    uniqueify_array(false, u_.a.a ? u_.a.a->size + 1 : 1);
    new((void*) &u_.a.a->a[u_.a.a->size]) Json(x);
    ++u_.a.a->size;
    return *this;
}

/** @overload */
template <typename P> inline Json& Json::push_back(const Json_proxy_base<P>& x) {
    uniqueify_array(false, u_.a.a ? u_.a.a->size + 1 : 1);
    new((void*) &u_.a.a->a[u_.a.a->size]) Json(x.cvalue());
    ++u_.a.a->size;
    return *this;
}

#if HAVE_CXX_RVALUE_REFERENCES
/** @overload */
inline Json& Json::push_back(Json&& x) {
    uniqueify_array(false, u_.a.a ? u_.a.a->size + 1 : 1);
    new((void*) &u_.a.a->a[u_.a.a->size]) Json(std::move(x));
    ++u_.a.a->size;
    return *this;
}
#endif

/** @brief Remove the last element from an array.
    @pre is_array() && !empty() */
inline void Json::pop_back() {
    assert(is_array() && u_.a.a && u_.a.a->size > 0);
    uniqueify_array(false, 0);
    --u_.a.a->size;
    u_.a.a->a[u_.a.a->size].~Json();
}

inline Json& Json::insert_back() {
    return *this;
}

/** @brief Insert the items [first, rest...] onto the back of this array.
    @pre is_array() || is_null()
    @return this Json

    A null Json is promoted to an array. */
template <typename T, typename... U>
inline Json& Json::insert_back(T first, U... rest) {
    push_back(first);
    insert_back(rest...);
    return *this;
}


inline Json* Json::array_data() {
    assert(is_null() || is_array());
    return u_.a.a ? u_.a.a->a : 0;
}


inline Json::const_object_iterator Json::cobegin() const {
    assert(is_null() || is_object());
    return const_object_iterator(this, 0);
}

inline Json::const_object_iterator Json::coend() const {
    assert(is_null() || is_object());
    return const_object_iterator(this, -1);
}

inline Json::const_object_iterator Json::obegin() const {
    return cobegin();
}

inline Json::const_object_iterator Json::oend() const {
    return coend();
}

inline Json::object_iterator Json::obegin() {
    assert(is_null() || is_object());
    return object_iterator(this, 0);
}

inline Json::object_iterator Json::oend() {
    assert(is_null() || is_object());
    return object_iterator(this, -1);
}

inline Json::const_array_iterator Json::cabegin() const {
    assert(is_null() || is_array());
    return const_array_iterator(this, 0);
}

inline Json::const_array_iterator Json::caend() const {
    assert(is_null() || is_array());
    ArrayJson *aj = ajson();
    return const_array_iterator(this, aj ? aj->size : 0);
}

inline Json::const_array_iterator Json::abegin() const {
    return cabegin();
}

inline Json::const_array_iterator Json::aend() const {
    return caend();
}

inline Json::array_iterator Json::abegin() {
    assert(is_null() || is_array());
    return array_iterator(this, 0);
}

inline Json::array_iterator Json::aend() {
    assert(is_null() || is_array());
    ArrayJson *aj = ajson();
    return array_iterator(this, aj ? aj->size : 0);
}

inline Json::const_iterator Json::cbegin() const {
    return const_iterator(this, 0);
}

inline Json::const_iterator Json::cend() const {
    return const_iterator(this, -1);
}

inline Json::iterator Json::begin() {
    return iterator(this, 0);
}

inline Json::iterator Json::end() {
    return iterator(this, -1);
}

inline Json::const_iterator Json::begin() const {
    return cbegin();
}

inline Json::const_iterator Json::end() const {
    return cend();
}


// Unparsing
class Json::unparse_manipulator {
  public:
    unparse_manipulator()
	: indent_depth_(0), tab_width_(0), newline_terminator_(false),
          space_separator_(false) {
    }
    int indent_depth() const {
	return indent_depth_;
    }
    unparse_manipulator indent_depth(int x) const {
	unparse_manipulator m(*this);
	m.indent_depth_ = x;
	return m;
    }
    int tab_width() const {
	return tab_width_;
    }
    unparse_manipulator tab_width(int x) const {
	unparse_manipulator m(*this);
	m.tab_width_ = x;
	return m;
    }
    bool newline_terminator() const {
        return newline_terminator_;
    }
    unparse_manipulator newline_terminator(bool x) const {
        unparse_manipulator m(*this);
        m.newline_terminator_ = x;
        return m;
    }
    bool space_separator() const {
        return space_separator_;
    }
    unparse_manipulator space_separator(bool x) const {
        unparse_manipulator m(*this);
        m.space_separator_ = x;
        return m;
    }
    bool empty() const {
        return !indent_depth_ && !newline_terminator_ && !space_separator_;
    }
  private:
    int indent_depth_;
    int tab_width_;
    bool newline_terminator_;
    bool space_separator_;
};

inline Json::unparse_manipulator Json::indent_depth(int x) {
    return unparse_manipulator().indent_depth(x);
}
inline Json::unparse_manipulator Json::tab_width(int x) {
    return unparse_manipulator().tab_width(x);
}
inline Json::unparse_manipulator Json::newline_terminator(bool x) {
    return unparse_manipulator().newline_terminator(x);
}
inline Json::unparse_manipulator Json::space_separator(bool x) {
    return unparse_manipulator().space_separator(x);
}

/** @brief Return the string representation of this Json. */
inline String Json::unparse() const {
    StringAccum sa;
    hard_unparse(sa, default_manipulator, 0);
    return sa.take_string();
}

/** @brief Return the string representation of this Json.
    @param add_newline If true, add a final newline. */
inline String Json::unparse(const unparse_manipulator &m) const {
    StringAccum sa;
    hard_unparse(sa, m, 0);
    return sa.take_string();
}

/** @brief Unparse the string representation of this Json into @a sa. */
inline void Json::unparse(StringAccum &sa) const {
    hard_unparse(sa, default_manipulator, 0);
}

/** @brief Unparse the string representation of this Json into @a sa. */
inline void Json::unparse(StringAccum &sa, const unparse_manipulator &m) const {
    hard_unparse(sa, m, 0);
}


// Parsing

/** @brief Parse @a str as UTF-8 JSON into this Json object.
    @return true iff the parse succeeded.

    An unsuccessful parse does not modify *this. */
inline bool Json::assign_parse(const String &str) {
    return assign_parse(str, str.begin(), str.end());
}

/** @brief Parse [@a first, @a last) as UTF-8 JSON into this Json object.
    @return true iff the parse succeeded.

    An unsuccessful parse does not modify *this. */
inline bool Json::assign_parse(const char *first, const char *last) {
    return assign_parse(String(), first, last);
}

/** @brief Return @a str parsed as UTF-8 JSON.

    Returns a null JSON object if the parse fails. */
inline Json Json::parse(const String &str) {
    Json j;
    (void) j.assign_parse(str);
    return j;
}

/** @brief Return [@a first, @a last) parsed as UTF-8 JSON.

    Returns a null JSON object if the parse fails. */
inline Json Json::parse(const char *first, const char *last) {
    Json j;
    (void) j.assign_parse(first, last);
    return j;
}


// Assignment

inline Json& Json::operator=(const Json& x) {
    if (x.u_.x.type < 0)
        x.u_.str.ref();
    else if (x.u_.x.c && (x.u_.x.type == j_array || x.u_.x.type == j_object))
        x.u_.x.c->ref();
    if (u_.x.type < 0)
        u_.str.deref();
    else if (u_.x.c && (u_.x.type == j_array || u_.x.type == j_object))
        u_.x.c->deref((json_type) u_.x.type);
    u_ = x.u_;
    return *this;
}

#if HAVE_CXX_RVALUE_REFERENCES
inline Json& Json::operator=(Json&& x) {
    using std::swap;
    swap(u_, x.u_);
    return *this;
}
#endif

/** @cond never */
template <typename U>
inline Json& Json::operator=(const Json_proxy_base<U> &x) {
    return *this = x.cvalue();
}
/** @endcond never */

inline Json& Json::operator++() {
    return *this += 1;
}
inline void Json::operator++(int) {
    ++(*this);
}
inline Json& Json::operator--() {
    return *this += -1;
}
inline void Json::operator--(int) {
    --(*this);
}
template <typename T>
inline Json& Json::add(T x) {
    force_number();
    if (u_.x.type == j_int)
        u_.i.x += x;
    else
        u_.d.x += x;
    return *this;
}
template <typename T>
inline Json& Json::subtract(T x) {
    force_number();
    if (u_.x.type == j_int)
        u_.i.x -= x;
    else
        u_.d.x -= x;
    return *this;
}
inline Json& Json::operator+=(int x) {
    return add(x);
}
inline Json& Json::operator+=(unsigned x) {
    return add(x);
}
inline Json& Json::operator+=(long x) {
    return add(x);
}
inline Json& Json::operator+=(unsigned long x) {
    return add(x);
}
inline Json& Json::operator+=(long long x) {
    return add(x);
}
inline Json& Json::operator+=(unsigned long long x) {
    return add(x);
}
inline Json& Json::operator+=(double x) {
    return add(x);
}
inline Json& Json::operator-=(int x) {
    return subtract(x);
}
inline Json& Json::operator-=(unsigned x) {
    return subtract(x);
}
inline Json& Json::operator-=(long x) {
    return subtract(x);
}
inline Json& Json::operator-=(unsigned long x) {
    return subtract(x);
}
inline Json& Json::operator-=(long long x) {
    return subtract(x);
}
inline Json& Json::operator-=(unsigned long long x) {
    return subtract(x);
}
inline Json& Json::operator-=(double x) {
    return subtract(x);
}
inline Json& Json::operator+=(const Json& x) {
    if (!x.is_null()) {
        // XXX what if both are integers
        force_number();
        u_.d.x = as_d() + x.as_d();
        u_.d.type = j_double;
    }
    return *this;
}
inline Json& Json::operator-=(const Json& x) {
    if (!x.is_null()) {
        // XXX what if both are integers
        force_number();
        u_.d.x = as_d() - x.as_d();
        u_.d.type = j_double;
    }
    return *this;
}

/** @brief Swap this Json with @a x. */
inline void Json::swap(Json& x) {
    using std::swap;
    swap(u_, x.u_);
}


inline StringAccum &operator<<(StringAccum &sa, const Json& json) {
    json.unparse(sa);
    return sa;
}

template <typename P>
inline StringAccum &operator<<(StringAccum &sa, const Json_proxy_base<P> &json) {
    return (sa << json.cvalue());
}

inline std::ostream &operator<<(std::ostream& f, const Json& json) {
    StringAccum sa;
    json.unparse(sa);
    return f.write(sa.data(), sa.length());
}

template <typename P>
inline std::ostream &operator<<(std::ostream& f, const Json_proxy_base<P>& json) {
    return (f << json.cvalue());
}

bool operator==(const Json& a, const Json& b);

template <typename T>
inline bool operator==(const Json_proxy_base<T>& a, const Json& b) {
    return a.cvalue() == b;
}

template <typename T>
inline bool operator==(const Json& a, const Json_proxy_base<T>& b) {
    return a == b.cvalue();
}

template <typename T, typename U>
inline bool operator==(const Json_proxy_base<T>& a,
		       const Json_proxy_base<U>& b) {
    return a.cvalue() == b.cvalue();
}

inline bool operator!=(const Json& a, const Json& b) {
    return !(a == b);
}

template <typename T>
inline bool operator!=(const Json_proxy_base<T>& a, const Json& b) {
    return !(a == b);
}

template <typename T>
inline bool operator!=(const Json& a, const Json_proxy_base<T>& b) {
    return !(a == b);
}

template <typename T, typename U>
inline bool operator!=(const Json_proxy_base<T>& a,
		       const Json_proxy_base<U>& b) {
    return !(a == b);
}

inline void swap(Json& a, Json& b) {
    a.swap(b);
}

#endif
