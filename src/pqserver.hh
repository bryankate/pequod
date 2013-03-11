#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH
#include "pqdatum.hh"
#include "interval_tree.hh"
#include "local_vector.hh"
#include "hashtable.hh"
#include "pqbase.hh"
#include "pqjoin.hh"
#include "pqsource.hh"
#include "pqsink.hh"
class Json;

namespace pq {
namespace bi = boost::intrusive;

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
    void add_join(Str first, Str last, Join* j, ErrorHandler* errh);
    inline void add_validjoin(Str first, Str last, Join* j);

    void insert(const String& key, String value);
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
    inline void notify(Datum* d, const String& old_value, SourceRange::notify_type notifier);

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
    inline void modify(const String& key, const F& func);
    inline void erase(const String& key);

#if 0
    template <typename I>
    void replace_range(Str first, Str last, I first_value, I last_value);
#endif

    inline void add_copy(SourceRange* r);
    inline void add_join(Str first, Str last, Join* j, ErrorHandler* errh = 0);
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

inline void Table::notify(Datum* d, const String& old_value, SourceRange::notify_type notifier) {
    for (auto it = source_ranges_.begin_contains(Str(d->key()));
         it != source_ranges_.end(); ++it)
        it->notify(d, old_value, notifier);
}

template <typename F>
void Table::modify(const String& key, const F& func) {
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d = p.second ? new Datum(key, String()) : p.first.operator->();
    String value = func(d, p.second);
    if (!is_unchanged_marker(value)) {
        d->value_.swap(value);
        if (p.second)
            store_.insert_commit(*d, cd);
        notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    } else if (p.second)
        delete d;
}

inline void Table::add_validjoin(Str first, Str last, Join* join) {
    sink_ranges_.insert(new ServerRange(first, last, ServerRange::validjoin, join));
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
inline void Server::modify(const String& key, const F& func) {
    if (Str tname = table_name(key))
        make_table(tname).modify(key, func);
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

inline void Server::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_join(first, last, join, errh);
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
