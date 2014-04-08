#ifndef PEQUOD_SINK_HH
#define PEQUOD_SINK_HH
#include <boost/intrusive/list.hpp>
#include "pqjoin.hh"
#include "interval.hh"
#include "local_vector.hh"
#include "local_str.hh"
#include "interval_tree.hh"
#include "pqdatum.hh"
#include <tamer/tamer.hh>
#include <list>

namespace pq {
class Server;
class Match;
class RangeMatch;
class JoinRange;
class Sink;
class Interconnect;

class ServerRangeBase {
  public:
    inline ServerRangeBase(Str first, Str last);

    typedef Str endpoint_type;
    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;
    inline Str subtree_iend() const;
    inline void set_subtree_iend(Str subtree_iend);

    static uint64_t allocated_key_bytes;

  protected:
    LocalStr<24> ibegin_;
    LocalStr<24> iend_;
    Str subtree_iend_;
};

namespace bi = boost::intrusive;
typedef bi::list_base_hook<bi::link_mode<bi::auto_unlink>> lru_hook;

class Evictable : public lru_hook {
  public:
    Evictable();
    virtual ~Evictable();

    enum { pri_none = 0, pri_persistent, pri_remote, pri_sink, pri_max };

    virtual void evict() = 0;
    virtual uint32_t priority() const;

    inline void mark_evicted();
    inline bool evicted() const;
    inline uint64_t last_access() const;
    inline void set_last_access(uint64_t now);
    void unlink();
    bool is_linked() const;

  private:
    bool evicted_;
    uint64_t last_access_;
};

class Loadable {
  public:
    Loadable(Table* table);
    virtual ~Loadable();

    inline bool pending() const;
    inline void add_waiting(tamer::event<> w);
    inline void notify_waiting();
    inline Table* table() const;

  protected:
    Table* table_;
  private:
    std::list<tamer::event<>> waiting_;
};

class IntermediateUpdate : public ServerRangeBase {
  public:
    IntermediateUpdate(Str first, Str last, Sink* sink, int joinpos, const Match& m, int notifier);

    typedef Str endpoint_type;
    inline Str context() const;
    inline int notifier() const;

    friend std::ostream& operator<<(std::ostream&, const IntermediateUpdate&);

  public:
    rblinks<IntermediateUpdate> rblinks_;
  private:
    LocalStr<12> context_;
    int joinpos_;
    int notifier_;

    friend class Sink;
};

class Restart {
  public:
    Restart(Sink* sink, int joinpos, const Match& match, int notifier);
    inline Str context() const;
    inline int notifier() const;

  private:
    LocalStr<12> context_;
    int joinpos_;
    int notifier_;

    friend class Sink;
};

class SinkRange : public ServerRangeBase, public Evictable {
  public:
    SinkRange(Str first, Str last, Table* table);
    ~SinkRange();

    bool validate(Str first, Str last, Server& server,
                  uint64_t now, uint32_t& log,
                  tamer::gather_rendezvous& gr);

    bool add_sink(JoinRange* jr, Server& server,
                  uint64_t now, uint32_t& log,
                  tamer::gather_rendezvous& gr);

    inline bool valid(uint64_t now) const;

    virtual void evict();
    virtual uint32_t priority() const;

  public:
    rblinks<SinkRange> rblinks_;
  private:
    Table* table_;
    local_vector<Sink*, 4> sinks_;

    inline bool validate_one(Str first, Str last,
                             Sink* sink, Server& server,
                             uint64_t now, uint32_t& log,
                             tamer::gather_rendezvous& gr);
    struct validate_args;
    bool validate_step(validate_args& va, int joinpos);
    bool validate_filters(validate_args& va);

    friend class Sink;
};

class Sink {
  public:
    Sink(JoinRange* jr, SinkRange* sr);
    ~Sink();

    inline void ref();
    inline void deref();

    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;

    inline bool valid() const;
    inline void set_valid();
    void invalidate();
    inline void set_validating(bool validating);

    inline Join* join() const;
    inline SinkRange* range() const;
    inline void prefetch() const;
    inline Table* table() const;
    inline Table& make_table_for(Str key) const;
    inline unsigned context_mask() const;
    inline Str context() const;

    inline bool has_expired(uint64_t now) const;
    inline void set_expiration(uint64_t from);

    inline void add_datum(Datum* d) const;
    inline void remove_datum(Datum* d) const;

    inline void clear_updates();
    void add_update(int joinpos, Str context, Str key, int notifier);
    void add_invalidate(Str key);
    void add_invalidate(Str first, Str last);
    void add_restart(int joinpos, const Match& match, int notifier);
    inline bool need_update() const;
    inline bool need_restart() const;
    bool update(Str first, Str last, Server& server,
                uint64_t now, uint32_t& log, tamer::gather_rendezvous& gr);
    bool restart(Str first, Str last, Server& server,
                 uint64_t now, uint32_t& log, tamer::gather_rendezvous& gr);

    inline void update_hint(const ServerStore& store, ServerStore::iterator hint) const;
    inline Datum* hint() const;

    friend std::ostream& operator<<(std::ostream&, const Sink&);

    static uint64_t invalidate_hit_keys;
    static uint64_t invalidate_miss_keys;

  private:
    bool valid_;
    bool validating_;
    Table* table_;
    mutable Datum* hint_;
    unsigned context_mask_;
    int dangerous_slot_;
    LocalStr<12> context_;
    uint64_t expires_at_;
    interval_tree<IntermediateUpdate> updates_;
    std::list<Restart*> restarts_;
    int refcount_;
    mutable uintptr_t data_free_;
    mutable local_vector<Datum*, 12> data_;
  protected:
    JoinRange* jr_;
    SinkRange* sr_;

    bool update_iu(Str first, Str last, IntermediateUpdate* iu, bool& remaining,
                   Server& server, uint64_t now, uint32_t& log,
                   tamer::gather_rendezvous& gr);
};

class JoinRange : public ServerRangeBase {
  public:
    JoinRange(Str first, Str last, Join* join);

    inline Join* join() const;

  public:
    rblinks<JoinRange> rblinks_;
  private:
    Join* join_;
};

class PersistedRange : public ServerRangeBase, public Loadable, public Evictable {
  public:
    PersistedRange(Table* table, Str first, Str last);

    virtual void evict();
    virtual uint32_t priority() const;

  public:
    rblinks<PersistedRange> rblinks_;
};

class RemoteRange : public ServerRangeBase, public Loadable, public Evictable {
  public:
    RemoteRange(Table* table, Str first, Str last, int32_t owner);

    inline int32_t owner() const;
    virtual void evict();
    virtual uint32_t priority() const;

  public:
    rblinks<RemoteRange> rblinks_;
  private:
    int32_t owner_;
};

/*
 * A fake sink that represents a remote server. This way the existing
 * source/sink architecture can be used to notify remote servers of
 * changes to data. It has no connection to an actual table or join.
 */
class RemoteSink : public Sink {
  public:
    RemoteSink(Interconnect* conn, uint32_t peer);
    ~RemoteSink();

    inline Interconnect* conn() const;
    inline uint32_t peer() const;

  private:
    Interconnect* conn_;
    uint32_t peer_;
};


inline ServerRangeBase::ServerRangeBase(Str first, Str last)
    : ibegin_(first), iend_(last) {
    if (!ibegin_.is_local())
        allocated_key_bytes += ibegin_.length();
    if (!iend_.is_local())
        allocated_key_bytes += iend_.length();
}

inline Str ServerRangeBase::ibegin() const {
    return ibegin_;
}

inline Str ServerRangeBase::iend() const {
    return iend_;
}

inline interval<Str> ServerRangeBase::interval() const {
    return make_interval(ibegin(), iend());
}

inline Str ServerRangeBase::subtree_iend() const {
    return subtree_iend_;
}

inline void ServerRangeBase::set_subtree_iend(Str subtree_iend) {
    subtree_iend_ = subtree_iend;
}

inline bool Loadable::pending() const {
    return !waiting_.empty();
}

inline void Loadable::add_waiting(tamer::event<> w) {
    waiting_.push_back(w);
}

inline void Loadable::notify_waiting() {
    while(!waiting_.empty()) {
        waiting_.front().operator()();
        waiting_.pop_front();
    }
}

inline Table* Loadable::table() const {
    return table_;
}

inline void Evictable::mark_evicted() {
    evicted_ = true;
}

inline bool Evictable::evicted() const {
    return evicted_;
}

inline uint64_t Evictable::last_access() const {
    return last_access_;
}

inline void Evictable::set_last_access(uint64_t now) {
    last_access_ = now;
}

inline bool SinkRange::valid(uint64_t now) const {

    for (auto sit = sinks_.begin(); sit != sinks_.end(); ++sit) {
        Sink* sink = *sit;

        if (!sink->valid() || sink->need_restart() || 
                sink->need_update() || sink->has_expired(now))
            return false;
    }

    return true;
}

inline Join* JoinRange::join() const {
    return join_;
}

inline Str Sink::ibegin() const {
    return sr_->ibegin();
}

inline Str Sink::iend() const {
    return sr_->iend();
}

inline ::interval<Str> Sink::interval() const {
    return sr_->interval();
}

inline void Sink::ref() {
    ++refcount_;
}

inline void Sink::deref() {
    if (--refcount_ == 0 && !valid())
        delete this;
}

inline bool Sink::valid() const {
    return valid_;
}

inline void Sink::set_valid() {
    valid_ = true;
}

inline void Sink::set_validating(bool validating) {
    validating_ = validating;
}

inline Join* Sink::join() const {
    return jr_->join();
}

inline SinkRange* Sink::range() const {
    return sr_;
}

inline Table* Sink::table() const {
    return table_;
}

inline unsigned Sink::context_mask() const {
    return context_mask_;
}

inline Str Sink::context() const {
    return context_;
}

inline void Sink::clear_updates() {
    while (IntermediateUpdate* iu = updates_.unlink_leftmost_without_rebalance())
        delete iu;
    for (auto it = restarts_.begin(); it != restarts_.end(); ++it)
        delete *it;
    restarts_.clear();
}

inline bool Sink::has_expired(uint64_t now) const {
    return expires_at_ && expires_at_ < now;
}

inline void Sink::set_expiration(uint64_t from) {
    if (!jr_->join()->maintained())
        expires_at_ = from + jr_->join()->staleness();
}

inline void Sink::add_datum(Datum* d) const {
    assert(d->owner() == this);
    uintptr_t pos = data_free_;
    if (pos == uintptr_t(-1)) {
        pos = data_.size();
        data_.push_back(d);
    } else {
        data_free_ = (uintptr_t) data_[pos];
        data_[pos] = d;
    }
    d->owner_position_ = pos;
}

inline void Sink::remove_datum(Datum* d) const {
    assert(d->owner() == this
           && (size_t) d->owner_position_ < (size_t) data_.size());
    data_[d->owner_position_] = (Datum*) data_free_;
    data_free_ = d->owner_position_;
}

inline bool Sink::need_update() const {
    return !updates_.empty();
}

inline bool Sink::need_restart() const {
    return !restarts_.empty();
}

inline void Sink::update_hint(const ServerStore& store, ServerStore::iterator hint) const {
#if HAVE_HINT_ENABLED
    Datum* hd = hint == store.end() ? 0 : hint.operator->();
    if (hd)
        hd->ref();
    if (hint_)
        hint_->deref();
    hint_ = hd;
#else
    (void)store;
    (void)hint;
#endif
}

inline Datum* Sink::hint() const {
    return hint_ && hint_->valid() ? hint_ : 0;
}

inline bool operator<(const Sink& a, const Sink& b) {
    return a.ibegin() < b.ibegin();
}

inline Str IntermediateUpdate::context() const {
    return context_;
}

inline int IntermediateUpdate::notifier() const {
    return notifier_;
}

inline Str Restart::context() const {
    return context_;
}

inline int Restart::notifier() const {
    return notifier_;
}

inline int32_t RemoteRange::owner() const {
    return owner_;
}

inline Interconnect* RemoteSink::conn() const {
    return conn_;
}

inline uint32_t RemoteSink::peer() const {
    return peer_;
}

} // namespace pq
#endif
