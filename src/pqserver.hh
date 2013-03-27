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
    static Table empty_table;

    typedef Str key_type;
    typedef Str key_const_reference;
    inline Str name() const;
    inline Str hashkey() const;
    inline Str key() const;

    typedef store_type::iterator iterator;
    inline iterator begin();
    inline iterator end();
    inline iterator lower_bound_hint(Str key);
    inline iterator lower_bound(Str key);
    inline size_t size() const;
    inline const Datum& operator[](Str key) const;
    inline size_t count(Str key) const;

    iterator validate(Str first, Str last, uint64_t now);
    inline iterator validate(Str key, uint64_t now);
    inline void invalidate_dependents(Str key);
    inline void invalidate_dependents(Str first, Str last);

    void add_source(SourceRange* r);
    inline void unlink_source(SourceRange* r);
    void remove_source(Str first, Str last, SinkRange* sink, Str context);
    void add_join(Str first, Str last, Join* j, ErrorHandler* errh);

    void insert(Str key, String value);
    template <typename F>
    inline void modify(Str key, const SinkRange* sink, const F& func);
    void erase(Str key);
    inline iterator erase(iterator it);
    inline void invalidate_erase(Datum* d);

    inline bool flush_for_pull(uint64_t now);

    void add_stats(Json& j) const;
    void print_sources(std::ostream& stream) const;

    uint64_t ninsert_;
    uint64_t nmodify_;
    uint64_t nmodify_nohint_;
    uint64_t nerase_;
    uint64_t nvalidate_;

  private:
    store_type store_;
    interval_tree<SourceRange> source_ranges_;
    interval_tree<JoinRange> join_ranges_;
    unsigned njoins_;
    uint64_t flush_at_;
    bool all_pull_;
    int namelen_;
    char name_[28];
  public:
    pequod_set_member_hook member_hook_;
  private:
    Server* server_;

    std::pair<store_type::iterator, bool> prepare_modify(Str key, const SinkRange* sink, store_type::insert_commit_data& cd);
    void finish_modify(std::pair<store_type::iterator, bool> p, const store_type::insert_commit_data& cd, Datum* d, Str key, const SinkRange* sink, String value);
    inline void notify(Datum* d, const String& old_value, SourceRange::notify_type notifier);
    bool hard_flush_for_pull(uint64_t now);

    friend class Server;
};

class Server {
  public:
    typedef ServerStore store_type;

    inline Server();

    class const_iterator;
    inline const_iterator begin() const;
    inline const_iterator end() const;
    inline const Datum* find(Str key) const;
    inline const Datum& operator[](Str key) const;
    inline size_t count(Str first, Str last) const;

    Table& table(Str tname) const;
    Table& make_table(Str tname);

    Table& table_for(Str key) const;
    Table& table_for(Str first, Str last) const;
    Table& make_table_for(Str key);
    Table& make_table_for(Str first, Str last);

    inline void insert(Str key, const String& value);
    inline void erase(Str key);

    void add_join(Str first, Str last, Join* j, ErrorHandler* errh = 0);

    inline uint64_t next_validate_at();
    inline Table::iterator validate(Str key);
    inline Table::iterator validate(Str first, Str last);
    size_t validate_count(Str first, Str last);

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

inline auto Table::begin() -> iterator {
    return store_.begin();
}

inline auto Table::end() -> iterator {
    return store_.end();
}

inline auto Table::lower_bound_hint(Str str) -> iterator {
    return store_.lower_bound(str, DatumCompare());
}

inline auto Table::lower_bound(Str str) -> iterator {
    return store_.lower_bound(str, DatumCompare());
}

inline size_t Table::size() const {
    return store_.size();
}

inline const Datum& Table::operator[](Str key) const {
    auto it = store_.find(key, DatumCompare());
    return it == store_.end() ? Datum::empty_datum : *it;
}

inline size_t Table::count(Str key) const {
    return store_.count(key, DatumCompare());
}

inline Server::Server()
    : last_validate_at_(0) {
}

inline Table& Server::table(Str tname) const {
    return const_cast<Table&>(tables_.get(tname, Table::empty_table));
}

inline Table& Server::table_for(Str key) const {
    Str tname = table_name(key);
    return const_cast<Table&>(tables_.get(tname, Table::empty_table));
}

inline Table& Server::table_for(Str first, Str last) const {
    Str tname = table_name(first, last);
    return const_cast<Table&>(tables_.get(tname, Table::empty_table));
}

inline Table& Server::make_table(Str tname) {
    bool inserted;
    auto it = tables_.find_insert(tname, inserted);
    if (inserted) {
        it->server_ = this;
        tables_by_name_.insert(*it);
    }
    return *it;
}

inline Table& Server::make_table_for(Str key) {
    Str tname = table_name(key);
    assert(tname);
    bool inserted;
    auto it = tables_.find_insert(tname, inserted);
    if (inserted) {
        it->server_ = this;
        tables_by_name_.insert(*it);
    }
    return *it;
}

inline Table& Server::make_table_for(Str first, Str last) {
    Str tname = table_name(first, last);
    assert(tname);
    bool inserted;
    auto it = tables_.find_insert(tname, inserted);
    if (inserted) {
        it->server_ = this;
        tables_by_name_.insert(*it);
    }
    return *it;
}

inline const Datum* Server::find(Str key) const {
    auto& store = table_for(key).store_;
    auto it = store.find(key, DatumCompare());
    return it == store.end() ? NULL : it.operator->();
}

inline const Datum& Server::operator[](Str key) const {
    return table_for(key)[key];
}

inline size_t Server::count(Str first, Str last) const {
    Table& t = table_for(first);
    return std::distance(t.lower_bound(first), t.lower_bound(last));
}

inline void Table::notify(Datum* d, const String& old_value, SourceRange::notify_type notifier) {
    Str key(d->key());
    for (auto it = source_ranges_.begin_contains(key);
         it != source_ranges_.end(); ) {
        // SourceRange::notify() might remove the SourceRange from the tree
        SourceRange* source = it.operator->();
        ++it;
        if (source->check_match(key))
            source->notify(d, old_value, notifier);
    }
}

inline auto Table::validate(Str key, uint64_t now) -> iterator {
    LocalStr<24> next_key;
    next_key.assign_uninitialized(key.length() + 1);
    memcpy(next_key.mutable_data(), key.data(), key.length());
    next_key.mutable_data()[key.length()] = 0;
    return validate(key, next_key, now);
}

inline void Table::invalidate_dependents(Str key) {
    for (auto it = source_ranges_.begin_contains(key);
         it != source_ranges_.end(); ) {
        // simple, but obviously we could do better
        SourceRange* source = it.operator->();
        ++it;
        source->invalidate();
    }
}

inline void Table::invalidate_dependents(Str first, Str last) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
         it != source_ranges_.end(); ) {
        // simple, but obviously we could do better
        SourceRange* source = it.operator->();
        ++it;
        source->invalidate();
    }
}

inline void Table::unlink_source(SourceRange* r) {
    source_ranges_.erase(*r);
}

template <typename F>
inline void Table::modify(Str key, const SinkRange* sink, const F& func) {
    store_type::insert_commit_data cd;
    std::pair<ServerStore::iterator, bool> p = prepare_modify(key, sink, cd);
    Datum* d = p.second ? NULL : p.first.operator->();
    finish_modify(p, cd, d, key, sink, func(d));
}

inline auto Table::erase(iterator it) -> iterator {
    Datum* d = it.operator->();
    it = store_.erase(it);
    if (d->owner())
        d->owner()->remove_datum(d);
    String old_value = erase_marker();
    std::swap(d->value(), old_value);
    notify(d, old_value, SourceRange::notify_erase);
    d->invalidate();
    return it;
}

inline void Table::invalidate_erase(Datum* d) {
    store_.erase(store_.iterator_to(*d));
    invalidate_dependents(d->key());
    d->invalidate();
}

inline bool Table::flush_for_pull(uint64_t now) {
    return all_pull_ && flush_at_ != now && hard_flush_for_pull(now);
}

inline void Server::insert(Str key, const String& value) {
    make_table_for(key).insert(key, value);
}

inline void Server::erase(Str key) {
    auto tit = tables_.find(table_name(key));
    if (tit)
        tit->erase(key);
}

inline uint64_t Server::next_validate_at() {
    uint64_t now = tstamp();
    now += now <= last_validate_at_;
    return last_validate_at_ = now;
}

inline Table::iterator Server::validate(Str first, Str last) {
    Str tname = table_name(first, last);
    assert(tname);
    return make_table(tname).validate(first, last, next_validate_at());
}

inline Table::iterator Server::validate(Str key) {
    return make_table_for(key).validate(key, next_validate_at());
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
            si_ = const_cast<Table&>(*ti_).begin();
            fix();
        } else
            si_ = Table::empty_table.end();
    }

    inline void fix() {
        while (si_ == const_cast<Table&>(*ti_).end()) {
            ++ti_;
            if (ti_ == s_->tables_by_name_.end()) {
                si_ = Table::empty_table.end();
                break;
            } else
                si_ = const_cast<Table&>(*ti_).begin();
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
