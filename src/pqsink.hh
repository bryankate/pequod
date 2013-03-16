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
class ValidJoinRange;

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
    IntermediateUpdate(Str first, Str last, Str context, Str key, int joinpos, int notifier);

    typedef Str endpoint_type;
    inline Str key() const;
    inline int notifier() const;

    friend std::ostream& operator<<(std::ostream&, const IntermediateUpdate&);

  public:
    rblinks<IntermediateUpdate> rblinks_;
  private:
    LocalStr<12> context_;
    LocalStr<24> key_;
    int joinpos_;
    int notifier_;

    friend class ValidJoinRange;
};

class ValidJoinRange : public ServerRangeBase {
  public:
    inline ValidJoinRange(Str first, Str last, JoinRange* jr, uint64_t now);
    ~ValidJoinRange();

    inline void ref();
    inline void deref();

    inline Join* join() const;
    inline Table* table() const;

    inline bool has_expired(uint64_t now) const;

    void add_update(int joinpos, const String& context, Str key, int notifier);
    inline bool need_update() const;
    void update(Str first, Str last, Server& server);

    inline void update_hint(const ServerStore& store, ServerStore::iterator hint) const;
    inline Datum* hint() const;

    void flush();

  public:
    rblinks<ValidJoinRange> rblinks_;
  private:
    JoinRange* jr_;
    int refcount_;
    mutable Datum* hint_;
    uint64_t expires_at_;
    interval_tree<IntermediateUpdate> updates_;

    bool update_iu(Str first, Str last, IntermediateUpdate* iu, Server& server);
};

class JoinRange : public ServerRangeBase {
  public:
    JoinRange(Str first, Str last, Join* join);
    ~JoinRange();

    inline Join* join() const;
    inline size_t valid_ranges_size() const;

    void validate(Str first, Str last, Server& server);

  public:
    rblinks<JoinRange> rblinks_;
  private:
    Join* join_;
    interval_tree<ValidJoinRange> valid_ranges_;
    std::vector<ValidJoinRange*> flushables_;

    inline void validate_one(Str first, Str last, Server& server, uint64_t now);
    struct validate_args;
    void validate_step(validate_args& va, int joinpos);

    friend class ValidJoinRange;
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

inline ValidJoinRange::ValidJoinRange(Str first, Str last, JoinRange* jr,
                                      uint64_t now)
    : ServerRangeBase(first, last), jr_(jr), refcount_(1), hint_{nullptr} {
    if (jr_->join()->staleness())
        expires_at_ = now + jr_->join()->staleness();
    else
        expires_at_ = 0;
}

inline void ValidJoinRange::ref() {
    ++refcount_;
}

inline void ValidJoinRange::deref() {
    if (--refcount_ == 0)
        delete this;            // XXX
}

inline Join* ValidJoinRange::join() const {
    return jr_->join();
}

inline Table* ValidJoinRange::table() const {
    return jr_->join()->sink_table();
}

inline bool ValidJoinRange::has_expired(uint64_t now) const {
    return expires_at_ && expires_at_ < now;
}

inline bool ValidJoinRange::need_update() const {
    return !updates_.empty();
}

inline void ValidJoinRange::update_hint(const ServerStore& store, ServerStore::iterator hint) const {
    Datum* hd = hint == store.end() ? 0 : hint.operator->();
    if (hd)
        hd->ref();
    if (hint_)
        hint_->deref();
    hint_ = hd;
}

inline Datum* ValidJoinRange::hint() const {
    return hint_ && hint_->valid() ? hint_ : 0;
}

inline Str IntermediateUpdate::key() const {
    return key_;
}

inline int IntermediateUpdate::notifier() const {
    return notifier_;
}

} // namespace pq
#endif
