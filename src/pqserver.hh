#ifndef PEQUOD_SERVER_HH
#define PEQUOD_SERVER_HH
#include "local_vector.hh"
#include "hashtable.hh"
#include "pqsource.hh"
#include "pqsink.hh"
#include "time.hh"
class Json;

namespace pq {
namespace bi = boost::intrusive;

class Table : public pequod_set_base_hook {
  public:
    typedef ServerStore store_type;

    Table(Str name);
    ~Table();
    static const Table empty_table;

    typedef Str key_type;
    typedef Str key_const_reference;
    inline Str name() const;
    inline Str hashkey() const;
    inline Str key() const;

    typedef store_type::const_iterator const_iterator;
    typedef store_type::iterator iterator;
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const_iterator lower_bound(Str key) const;
    inline iterator lower_bound(Str key);
    inline size_t size() const;

    inline void validate(Str first, Str last, uint64_t now);

    void add_source(SourceRange* r);
    void remove_source(Str first, Str last, SinkRange* sink, Str context);
    void add_join(Str first, Str last, Join* j, ErrorHandler* errh);

    void insert(Str key, String value);
    template <typename F>
    void modify(Str key, const SinkRange* sink, const F& func);
    void erase(Str key);
    inline iterator erase(iterator it);

    void clear();

    uint64_t ninsert_;
    uint64_t nmodify_;
    uint64_t nerase_;

  private:
    store_type store_;
    interval_tree<SourceRange> source_ranges_;
    interval_tree<JoinRange> join_ranges_;
    int namelen_;
    char name_[28];
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

    inline Server();

    class const_iterator;
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const Datum* find(Str str) const;
    inline store_type::const_iterator lower_bound(Str str) const;
    inline const Datum& operator[](Str str) const;
    inline size_t count(Str first, Str last) const;

    Table& make_table(Str name);

    inline void insert(Str key, const String& value);
    inline void erase(Str key);

    inline void add_source(SourceRange* r);
    inline void remove_source(Str first, Str last, SinkRange* sink, Str context);
    void add_join(Str first, Str last, Join* j, ErrorHandler* errh = 0);

    inline void validate(Str key);
    inline void validate(Str first, Str last);
    inline size_t validate_count(Str first, Str last);

    Json stats() const;
    void print(std::ostream& stream);

  private:
    HashTable<Table> tables_;
    bi::set<Table> tables_by_name_;
    uint64_t last_validate_at_;
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

inline auto Table::begin() const -> const_iterator {
    return store_.begin();
}

inline auto Table::end() const -> const_iterator {
    return store_.end();
}

inline auto Table::lower_bound(Str str) const -> const_iterator {
    return store_.lower_bound(str, DatumCompare());
}

inline auto Table::lower_bound(Str str) -> iterator {
    return store_.lower_bound(str, DatumCompare());
}

inline size_t Table::size() const {
    return store_.size();
}

inline Server::Server()
    : last_validate_at_(0) {
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

inline const Datum* Server::find(Str str) const {
    auto& store = tables_.get(table_name(str), Table::empty_table).store_;
    auto it = store.find(str, DatumCompare());
    return it == store.end() ? NULL : it.operator->();
}

inline const Datum& Server::operator[](Str str) const {
    auto& store = tables_.get(table_name(str), Table::empty_table).store_;
    auto it = store.find(str, DatumCompare());
    return it == store.end() ? Datum::empty_datum : *it;
}

inline auto Server::lower_bound(Str str) const -> Table::const_iterator {
    return tables_.get(table_name(str), Table::empty_table).lower_bound(str);
}

inline size_t Server::count(Str first, Str last) const {
    return std::distance(lower_bound(first), lower_bound(last));
}

inline void Table::notify(Datum* d, const String& old_value, SourceRange::notify_type notifier) {
    Str key(d->key());
    for (auto it = source_ranges_.begin_contains(key);
         it != source_ranges_.end(); ++it)
        if (it->check_match(key))
            it->notify(d, old_value, notifier);
}

template <typename F>
void Table::modify(Str key, const SinkRange* sink, const F& func) {
    store_type::insert_commit_data cd;
    std::pair<ServerStore::iterator, bool> p;
    Datum* hint = sink ? sink->hint() : 0;
    if (!hint || !hint->valid())
        p = store_.insert_check(key, DatumCompare(), cd);
    else {
        p.first = store_.iterator_to(*hint);
        if (hint->key() == key)
            p.second = false;
        else {
            ++p.first;
            p = store_.insert_check(p.first, key, DatumCompare(), cd);
        }
    }
    Datum* d = p.second ? NULL : p.first.operator->();
    String value = func(d);
    if (is_erase_marker(value)) {
        mandatory_assert(!p.second);
        p.first = store_.erase(p.first);
        if (sink)
            sink->update_hint(store_, p.first);
        notify(d, String(), SourceRange::notify_erase);
        d->invalidate();
    } else if (!is_unchanged_marker(value)) {
        if (p.second)
            d = new Datum(key, sink);
        d->value().swap(value);
        if (p.second)
            p.first = store_.insert_commit(*d, cd);
        if (sink && d != hint)
            sink->update_hint(store_, p.first);
        notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    }
    ++nmodify_;
}

inline auto Table::erase(iterator it) -> iterator {
    Datum* d = it.operator->();
    it = store_.erase(it);
    notify(d, String(), SourceRange::notify_erase);
    d->invalidate();
    return it;
}

inline void Table::validate(Str first, Str last, uint64_t now) {
    for (auto it = join_ranges_.begin_overlaps(first, last);
	 it != join_ranges_.end(); ++it)
        it->validate(first, last, *server_, now);
}

inline void Server::insert(Str key, const String& value) {
    if (Str tname = table_name(key))
        make_table(tname).insert(key, value);
}

inline void Server::erase(Str key) {
    auto tit = tables_.find(table_name(key));
    if (tit)
        tit->erase(key);
}

inline void Server::add_source(SourceRange* r) {
    Str tname = table_name(r->ibegin());
    assert(tname);
    make_table(tname).add_source(r);
}

inline void Server::remove_source(Str first, Str last, SinkRange* sink, Str context) {
    Str tname = table_name(first);
    assert(tname);
    make_table(tname).remove_source(first, last, sink, context);
}

inline void Server::validate(Str first, Str last) {
    uint64_t now = tstamp();
    now += now == last_validate_at_;
    last_validate_at_ = now;
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).validate(first, last, now);
}

inline void Server::validate(Str key) {
    LocalStr<24> next_key;
    next_key.assign_uninitialized(key.length() + 1);
    memcpy(next_key.mutable_data(), key.data(), key.length());
    next_key.mutable_data()[key.length()] = 0;
    validate(key, next_key);
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
