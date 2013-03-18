#ifndef PEQUOD_SINK_HH
#define PEQUOD_SINK_HH
#include "pqjoin.hh"
#include "interval.hh"
#include "local_vector.hh"
#include "local_str.hh"
#include "interval_tree.hh"
#include "pqdatum.hh"

namespace pq {
class Server;
class Match;
class JoinRange;

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

class IntermediateUpdate : public ServerRangeBase {
  public:
    IntermediateUpdate(Str first, Str last, SinkRange* sink, int joinpos, const Match& m, int notifier);

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

    friend class SinkRange;
};

class SinkRange : public ServerRangeBase {
  public:
    SinkRange(Str first, Str last, JoinRange* jr, uint64_t now);
    ~SinkRange();

    inline void ref();
    inline void deref();

    inline bool valid() const;
    void invalidate();

    inline Join* join() const;
    inline Table* table() const;
    inline unsigned context_mask() const;
    inline Str context() const;

    inline bool has_expired(uint64_t now) const;

    void add_update(int joinpos, Str context, Str key, int notifier);
    inline bool need_update() const;
    void update(Str first, Str last, Server& server, uint64_t now);

    inline void update_hint(const ServerStore& store, ServerStore::iterator hint) const;
    inline Datum* hint() const;

    friend std::ostream& operator<<(std::ostream&, const SinkRange&);

  public:
    rblinks<SinkRange> rblinks_;
  private:
    JoinRange* jr_;
    int refcount_;
    unsigned context_mask_;
    LocalStr<12> context_;
    mutable Datum* hint_;
    uint64_t expires_at_;
    interval_tree<IntermediateUpdate> updates_;

    bool update_iu(Str first, Str last, IntermediateUpdate* iu, Server& server,
                   uint64_t now);
};

class JoinRange : public ServerRangeBase {
  public:
    JoinRange(Str first, Str last, Join* join);
    ~JoinRange();

    inline Join* join() const;
    inline size_t valid_ranges_size() const;

    void validate(Str first, Str last, Server& server, uint64_t now);
    void pull_flush();

  public:
    rblinks<JoinRange> rblinks_;
  private:
    Join* join_;
    interval_tree<SinkRange> valid_ranges_;

    inline void validate_one(Str first, Str last, Server& server, uint64_t now);
    struct validate_args;
    void validate_step(validate_args& va, int joinpos);

    friend class SinkRange;
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

inline Join* JoinRange::join() const {
    return join_;
}

inline size_t JoinRange::valid_ranges_size() const {
    return valid_ranges_.size();
}

inline void SinkRange::ref() {
    ++refcount_;
}

inline void SinkRange::deref() {
    if (--refcount_ == 0 && !valid())
        delete this;
}

inline bool SinkRange::valid() const {
    return !ibegin_.empty();
}

inline Join* SinkRange::join() const {
    return jr_->join();
}

inline Table* SinkRange::table() const {
    return jr_->join()->sink_table();
}

inline unsigned SinkRange::context_mask() const {
    return context_mask_;
}

inline Str SinkRange::context() const {
    return context_;
}

inline bool SinkRange::has_expired(uint64_t now) const {
    return expires_at_ && expires_at_ < now;
}

inline bool SinkRange::need_update() const {
    return !updates_.empty();
}

inline void SinkRange::update_hint(const ServerStore& store, ServerStore::iterator hint) const {
    Datum* hd = hint == store.end() ? 0 : hint.operator->();
    if (hd)
        hd->ref();
    if (hint_)
        hint_->deref();
    hint_ = hd;
}

inline Datum* SinkRange::hint() const {
    return hint_ && hint_->valid() ? hint_ : 0;
}

inline Str IntermediateUpdate::context() const {
    return context_;
}

inline int IntermediateUpdate::notifier() const {
    return notifier_;
}

} // namespace pq
#endif
