#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH 1
#include <boost/intrusive/set.hpp>
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "interval_tree.hh"
#include "local_vector.hh"
#include "hashtable.hh"
#include "pqbase.hh"
#include "pqjoin.hh"
class Json;

namespace pq {
class Join;
class Match;
class Server;
class Table;

namespace bi = boost::intrusive;
typedef bi::set_base_hook<bi::link_mode<bi::normal_link>,
			  bi::optimize_size<true> > pequod_set_base_hook;
typedef bi::set_member_hook<bi::link_mode<bi::normal_link>,
			    bi::optimize_size<true> > pequod_set_member_hook;

class Datum : public pequod_set_base_hook {
  public:
    explicit Datum(const String& key)
	: key_(key) {
    }
    Datum(const String& key, const String& value)
	: key_(key), value_(value) {
    }

    const String& key() const {
	return key_;
    }
    const String& value() const {
        return value_;
    }

  private:
    String key_;
  public:
    String value_;
    pequod_set_member_hook member_hook_;
};

struct DatumCompare {
    template <typename T>
    inline bool operator()(const Datum& a, const String_base<T>& b) const {
	return a.key() < b;
    }
    inline bool operator()(const Datum& a, Str b) const {
	return a.key() < b;
    }
    template <typename T>
    inline bool operator()(const String_base<T>& a, const Datum& b) const {
	return a < b.key();
    }
    inline bool operator()(Str a, const Datum& b) const {
	return a < b.key();
    }
};

struct DatumDispose {
    inline void operator()(Datum* ptr) {
	delete ptr;
    }
};

struct JoinValue {
    inline JoinValue(JoinValueType jvt) : jvt_(jvt) {
    }
    inline void reset() {
        string_value_.reset();
        key_.reset();
    }
    inline bool copy_last() const {
        return jvt_ == jvt_copy_last;
    }
    inline void operator()(Datum* d, bool insert) {
        if (insert) {
            d->value_ = value();
            return;
        }
        switch (jvt_) {
        case jvt_copy_last:
            d->value_ = string_value_.ref_;
            break;
        case jvt_min_last:
            if (d->value_ > string_value_.ref_)
                d->value_ = string_value_.ref_;
            break;
        case jvt_max_last:
            if (d->value_ < string_value_.ref_)
                d->value_ = string_value_.ref_;
            break;
        case jvt_count_match:
            d->value_ = String(int_value_ + atoi(d->value_.c_str()));
            break;
        default:
            mandatory_assert(0, "bad JoinValueType");
        }
    }
    inline void accum(const Str &key, const String &v, bool key_safe, bool value_safe) {
        switch (jvt_) {
        case jvt_copy_last:
            string_value_.update(v, value_safe);
            break;
        case jvt_min_last:
            if (unlikely(!has_value()) || v < string_value_.ref_)
                string_value_.update(v, value_safe);
            break;
        case jvt_max_last:
            if (unlikely(!has_value()) || v > string_value_.ref_)
                string_value_.update(v, value_safe);
            break;
        case jvt_count_match:
            if (unlikely(!has_value()))
                int_value_ = 1;
            else
                ++int_value_;
            break;
        default:
            mandatory_assert(0, "bad JoinValueType");
        }
        if (unlikely(!has_value()))
            key_.update(key, key_safe);
    }
    inline bool has_value() const {
        return !!key_;
    }
    inline const Str &key() const {
        return key_.ref_;
    }
    inline const Str &value() {
        if (jvt_ == jvt_count_match && !string_value_)
            string_value_.update(String(int_value_), false);
        return string_value_.ref_;
    }
  private:
    struct safe_string {
        Str ref_;
        String buffer_;
        inline void update(const Str v, bool safe) {
            if (safe)
                ref_ = v;
            else {
                buffer_ = v;
                ref_.assign(buffer_);
            }
        }
        inline void reset() {
            ref_.assign(NULL, 0);
        }
        inline bool operator!() const {
            return ref_.s == NULL;
        }
    };
    JoinValueType jvt_;
    int64_t int_value_;
    safe_string key_;
    safe_string string_value_;
};

typedef bi::set<Datum> ServerStore;

class SourceRange {
  public:
    SourceRange(Server& server, Join* join, const Match& m,
                Str ibegin, Str iend);
    virtual ~SourceRange();

    typedef Str endpoint_type;
    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;
    inline Str subtree_iend() const;
    inline void set_subtree_iend(Str subtree_iend);

    inline Join* join() const;
    void add_sinks(const SourceRange& r);

    enum notify_type {
	notify_erase = -1, notify_update = 0, notify_insert = 1
    };
    virtual void notify(const Datum* d, int notifier) const = 0;

    friend std::ostream& operator<<(std::ostream&, const SourceRange&);

  private:
    Str ibegin_;
    Str iend_;
    Str subtree_iend_;
  public:
    rblinks<SourceRange> rblinks_;
  protected:
    Join* join_;
    Table* dst_table_;
    // XXX?????    uint64_t expires_at_;
    mutable local_vector<String, 4> resultkeys_;
  private:
    char buf_[32];
};

class CopySourceRange : public SourceRange {
  public:
    inline CopySourceRange(Server& server, Join* join, const Match& m,
                           Str ibegin, Str iend);
    virtual void notify(const Datum* d, int notifier) const;
};

class CountSourceRange : public SourceRange {
  public:
    inline CountSourceRange(Server& server, Join* join, const Match& m,
                            Str ibegin, Str iend);
    virtual void notify(const Datum* d, int notifier) const;
};

class JVSourceRange : public SourceRange {
  public:
    inline JVSourceRange(Server& server, Join* join, const Match& m,
                         Str ibegin, Str iend);
    virtual void notify(const Datum* d, int notifier) const;
};

class ServerRange {
  public:
    enum range_type {
        joinsink = 2, validjoin = 4
    };
    static ServerRange* make(Str first, Str last, range_type type, Join *join = 0);

    typedef Str endpoint_type;
    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;
    inline Str subtree_iend() const;
    inline void set_subtree_iend(Str subtree_iend);

    inline range_type type() const;
    inline Join* join() const;
    inline bool expired_at(uint64_t t) const;

    void validate(Str first, Str last, Server& server);

    friend std::ostream& operator<<(std::ostream&, const ServerRange&);

  private:
    int ibegin_len_;
    int iend_len_;
    Str subtree_iend_;
  public:
    rblinks<ServerRange> rblinks_;
  private:
    range_type type_;
    Join* join_;
    uint64_t expires_at_;
    mutable local_vector<String, 4> resultkeys_;
    char keys_[0];

    inline ServerRange(Str first, Str last, range_type type, Join* join);
    ~ServerRange() = default;
    void validate(Match& mf, Match& ml, int joinpos, Server& server);
};

class ServerRangeSet {
  public:
    inline ServerRangeSet(Str first, Str last, int types);

    inline void push_back(ServerRange* r);

    void validate(Server& server);

    friend std::ostream& operator<<(std::ostream&, const ServerRangeSet&);
    inline int total_size() const;

  private:
    local_vector<ServerRange*, 5> r_;
    Str first_;
    Str last_;
    int types_;

    void validate_join(ServerRange* jr, Server& server);
};

class Table : public pequod_set_base_hook {
  public:
    typedef ServerStore store_type;

    Table(Str name);
    static const Table empty_table;

    typedef Str key_type;
    typedef Str key_const_reference;
    inline Str name() const;
    inline Str hashkey() const;
    inline Str key() const;

    typedef store_type::const_iterator const_iterator;
    inline const_iterator begin() const;
    inline const_iterator end() const;

    inline void validate(Str first, Str last);

    void add_copy(SourceRange* r);
    void add_join(Str first, Str last, Join* j);
    inline void add_validjoin(Str first, Str last, Join* j);

    void insert(const String& key, const String& value);
    template <typename F>
    void modify(const String& key, F& func);
    template <typename F>
    void modify(const String& key, const F& func);
    void erase(const String& key);

  private:
    store_type store_;
    interval_tree<SourceRange> source_ranges_;
    interval_tree<ServerRange> sink_ranges_;
    int namelen_;
    char name_[32];
  public:
    pequod_set_member_hook member_hook_;
  private:
    Server* server_;
    inline void notify_insert(Datum* d, SourceRange::notify_type notifier);

    friend class Server;
};

class Server {
  public:
    typedef ServerStore store_type;

    Server() {
    }

    class const_iterator;
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const Datum *find(Str str) const;
    inline store_type::const_iterator lower_bound(Str str) const;
    inline size_t count(Str first, Str last) const;

    Table& make_table(Str name);

    inline void insert(const String& key, const String& value);
    template <typename F>
    inline void modify(const String& key, F& func);
    template <typename F>
    inline void modify(const String& key, const F& func);
    inline void erase(const String& key);

#if 0
    template <typename I>
    void replace_range(Str first, Str last, I first_value, I last_value);
#endif

    inline void add_copy(SourceRange* r);
    inline void add_join(Str first, Str last, Join* j);
    inline void add_validjoin(Str first, Str last, Join* j);

    inline void validate(Str first, Str last);
    inline size_t validate_count(Str first, Str last);

    Json stats() const;
    void print(std::ostream& stream);

  private:
    HashTable<Table> tables_;
    bi::set<Table> tables_by_name_;
    friend class const_iterator;
};

inline bool operator<(const Datum& a, const Datum& b) {
    return a.key() < b.key();
}
inline bool operator==(const Datum& a, const Datum& b) {
    return a.key() == b.key();
}
inline bool operator>(const Datum& a, const Datum& b) {
    return a.key() > b.key();
}

inline bool operator<(const Table& a, const Table& b) {
    return a.key() < b.key();
}
inline bool operator==(const Table& a, const Table& b) {
    return a.key() == b.key();
}
inline bool operator>(const Table& a, const Table& b) {
    return a.key() > b.key();
}

inline Str Table::name() const {
    return Str(name_, namelen_);
}

inline Str Table::hashkey() const {
    return Str(name_, namelen_);
}

inline Str Table::key() const {
    return Str(name_, namelen_);
}

inline Table::const_iterator Table::begin() const {
    return store_.begin();
}

inline Table::const_iterator Table::end() const {
    return store_.end();
}

inline Table& Server::make_table(Str name) {
    bool inserted;
    auto it = tables_.find_insert(name, inserted);
    if (inserted) {
        it->server_ = this;
        tables_by_name_.insert(*it);
    }
    return *it;
}

inline const Datum *Server::find(Str str) const {
    auto& store = tables_.get(table_name(str), Table::empty_table).store_;
    auto it = store.find(str, DatumCompare());
    return it == store.end() ? NULL : it.operator->();
}

inline ServerStore::const_iterator Server::lower_bound(Str str) const {
    return tables_.get(table_name(str), Table::empty_table).store_.lower_bound(str, DatumCompare());
}

inline size_t Server::count(Str first, Str last) const {
    return std::distance(lower_bound(first), lower_bound(last));
}

inline Str SourceRange::ibegin() const {
    return ibegin_;
}

inline Str SourceRange::iend() const {
    return iend_;
}

inline Join* SourceRange::join() const {
    return join_;
}

inline interval<Str> SourceRange::interval() const {
    return make_interval(ibegin(), iend());
}

inline Str SourceRange::subtree_iend() const {
    return subtree_iend_;
}

inline void SourceRange::set_subtree_iend(Str subtree_iend) {
    subtree_iend_ = subtree_iend;
}

inline CopySourceRange::CopySourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline CountSourceRange::CountSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline JVSourceRange::JVSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline Str ServerRange::ibegin() const {
    return Str(keys_, ibegin_len_);
}

inline Str ServerRange::iend() const {
    return Str(keys_ + ibegin_len_, iend_len_);
}

inline interval<Str> ServerRange::interval() const {
    return make_interval(ibegin(), iend());
}

inline Str ServerRange::subtree_iend() const {
    return subtree_iend_;
}

inline void ServerRange::set_subtree_iend(Str subtree_iend) {
    subtree_iend_ = subtree_iend;
}

inline ServerRange::range_type ServerRange::type() const {
    return type_;
}

inline Join* ServerRange::join() const {
    return join_;
}

inline bool ServerRange::expired_at(uint64_t t) const {
    return expires_at_ && (expires_at_ < t);
}

inline ServerRangeSet::ServerRangeSet(Str first, Str last, int types)
    : first_(first), last_(last), types_(types) {
}

inline void ServerRangeSet::push_back(ServerRange* r) {
    if (r->type() & types_)
        r_.push_back(r);
}

inline int ServerRangeSet::total_size() const {
    return r_.size();
}

inline void Table::notify_insert(Datum* d, SourceRange::notify_type notifier) {
    for (auto it = source_ranges_.begin_contains(Str(d->key()));
         it != source_ranges_.end(); ++it)
        it->notify(d, notifier);
}

template <typename F>
void Table::modify(const String& key, F& func) {
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
        d = new Datum(key, String());
        store_.insert_commit(*d, cd);
    } else
        d = p.first.operator->();
    func(d, p.second);
    notify_insert(d, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
}

template <typename F>
void Table::modify(const String& key, const F& func) {
    F func_copy(func);
    modify(key, func_copy);
}

inline void Table::add_validjoin(Str first, Str last, Join* join) {
    ServerRange* r = ServerRange::make(first, last, ServerRange::validjoin, join);
    sink_ranges_.insert(r);
}

inline void Table::validate(Str first, Str last) {
    ServerRangeSet srs(first, last,
                       ServerRange::joinsink | ServerRange::validjoin);
    for (auto it = sink_ranges_.begin_overlaps(interval<Str>(first, last));
	 it != sink_ranges_.end(); ++it)
	srs.push_back(it.operator->());
    srs.validate(*server_);
}

inline void Server::insert(const String& key, const String& value) {
    if (Str tname = table_name(key))
        make_table(tname).insert(key, value);
}

template <typename F>
inline void Server::modify(const String& key, F& func) {
    if (Str tname = table_name(key))
        make_table(tname).modify(key, func);
}

template <typename F>
inline void Server::modify(const String& key, const F& func) {
    F func_copy(func);
    modify(key, func_copy);
}

inline void Server::erase(const String& key) {
    auto tit = tables_.find(table_name(Str(key)));
    if (tit)
        tit->erase(key);
}

inline void Server::add_copy(SourceRange* r) {
    Str tname = table_name(r->ibegin());
    assert(tname);
    make_table(tname).add_copy(r);
}

inline void Server::add_join(Str first, Str last, Join* join) {
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_join(first, last, join);
}

inline void Server::add_validjoin(Str first, Str last, Join* join) {
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_validjoin(first, last, join);
}

#if 0
template <typename I>
void Server::replace_range(Str first, Str last, I first_value, I last_value) {
#if 0
    auto it = store_.bounded_range(first, last, DatumCompare(), true, false);
#else
    auto it = std::make_pair(store_.lower_bound(first, DatumCompare()),
			     store_.lower_bound(last, DatumCompare()));
#endif
    ServerRangeSet srs(this, first, last, ServerRange::copy);
    for (auto it = source_ranges_.begin_overlaps(interval<Str>(first, last));
	 it != source_ranges_.end(); ++it)
	srs.push_back(it.operator->());

    while (first_value != last_value && it.first != it.second) {
	int cmp = it.first->key().compare(first_value->first);
	Datum* d;
	if (cmp > 0) {
	    d = new Datum(first_value->first, first_value->second);
	    (void) store_.insert(it.first, *d);
	} else if (cmp == 0) {
	    d = it.first.operator->();
	    d->value_ = first_value->second;
	    ++it.first;
	} else {
	    d = it.first.operator->();
	    it.first = store_.erase(it.first);
	}
	srs.notify(d, cmp);
	if (cmp >= 0)
	    ++first_value;
	if (cmp < 0)
	    delete d;
    }
    while (first_value != last_value) {
	Datum* d = new Datum(first_value->first, first_value->second);
	(void) store_.insert(it.first, *d);
	srs.notify(d, 1);
	++first_value;
    }
    while (it.first != it.second) {
	Datum* d = it.first.operator->();
	it.first = store_.erase(it.first);
	srs.notify(d, -1);
	delete d;
    }
}
#endif

inline void Server::validate(Str first, Str last) {
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).validate(first, last);
}

inline size_t Server::validate_count(Str first, Str last) {
    validate(first, last);
    return count(first, last);
}

class Server::const_iterator {
  public:
    const_iterator() = default;

    inline const Datum& operator*() const {
        return *si_;
    }
    inline const Datum* operator->() const {
        return si_.operator->();
    }

    inline void operator++() {
        ++si_;
        fix();
    }

    friend inline bool operator==(const const_iterator& a,
                                  const const_iterator& b) {
        return a.si_ == b.si_;
    }
    friend inline bool operator!=(const const_iterator& a,
                                  const const_iterator& b) {
        return a.si_ != b.si_;
    }

  private:
    store_type::const_iterator si_;
    bi::set<Table>::const_iterator ti_;
    const Server* s_;
    inline const_iterator(const Server* s, bool begin)
        : ti_(begin ? s->tables_by_name_.begin() : s->tables_by_name_.end()),
          s_(s) {
        if (begin && ti_ != s->tables_by_name_.end()) {
            si_ = ti_->begin();
            fix();
        } else
            si_ = Table::empty_table.end();
    }

    inline void fix() {
        while (si_ == ti_->end()) {
            ++ti_;
            if (ti_ == s_->tables_by_name_.end()) {
                si_ = Table::empty_table.end();
                break;
            } else
                si_ = ti_->begin();
        }
    }
    friend class Server;
};

inline Server::const_iterator Server::begin() const {
    return const_iterator(this, true);
}

inline Server::const_iterator Server::end() const {
    return const_iterator(this, false);
}

} // namespace
#endif
