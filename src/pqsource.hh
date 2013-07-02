#ifndef PEQUOD_PQSOURCE_HH
#define PEQUOD_PQSOURCE_HH
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "rb.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "pqsink.hh"
#include "local_vector.hh"
#include "local_str.hh"
#include <iostream>

namespace pq {
class Server;
class Match;
class Datum;
class Table;

class SourceRange {
  public:
    struct parameters {
        Server& server;
        Join* join;
        int joinpos;
        const Match& match;
        Str first;
        Str last;
        SinkRange* sink;
    };

    SourceRange(const parameters& p);
    virtual ~SourceRange();

    typedef Str endpoint_type;
    inline Str ibegin() const;
    inline Str iend() const;
    inline ::interval<Str> interval() const;
    inline Str subtree_iend() const;
    inline void set_subtree_iend(Str subtree_iend);

    inline bool empty() const;

    virtual void invalidate();
    inline void clear_without_deref();

    inline Join* join() const;
    inline int joinpos() const;
    void take_results(SourceRange& r);
    void remove_sink(SinkRange* sink, Str context);
    virtual bool can_evict() const;

    enum notify_type {
	notify_erase = -1, notify_update = 0, notify_insert = 1
    };

    virtual bool check_match(Str key) const;
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;

    friend std::ostream& operator<<(std::ostream&, const SourceRange&);

    static uint64_t allocated_key_bytes;

  private:
    LocalStr<24> ibegin_;
    LocalStr<24> iend_;
    Str subtree_iend_;
  public:
    rblinks<SourceRange> rblinks_;
  protected:
    struct result {
        LocalStr<12> context;
        SinkRange* sink;
    };

    Join* join_;
    int joinpos_;
    mutable local_vector<result, 4> results_;

    virtual void kill();
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const = 0;
};


class InvalidatorRange : public SourceRange {
  public:
    inline InvalidatorRange(const parameters& p);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
    virtual bool can_evict() const;
  protected:
    virtual void notify(Str, SinkRange*, const Datum*, const String&, int) const {}
};


class SubscribedRange : public SourceRange {
  public:
    inline SubscribedRange(const parameters& p);

    virtual void invalidate();
    virtual bool check_match(Str key) const;
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
    virtual bool can_evict() const;
  protected:
    virtual void kill();
    virtual void notify(Str, SinkRange*, const Datum*, const String&, int) const {}
  private:
    Server& server_;
};


class CopySourceRange : public SourceRange {
  public:
    inline CopySourceRange(const parameters& p);
    virtual bool can_evict() const;
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
};


class CountSourceRange : public SourceRange {
  public:
    inline CountSourceRange(const parameters& p);
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
};

class MinSourceRange : public SourceRange {
  public:
    inline MinSourceRange(const parameters& p);
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
};

class MaxSourceRange : public SourceRange {
  public:
    inline MaxSourceRange(const parameters& p);
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
};

class SumSourceRange : public SourceRange {
  public:
    inline SumSourceRange(const parameters& p);
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
};

class Bounds {
  public:
    inline Bounds(const Json& param);

    inline bool has_bounds() const;
    inline bool in_bounds(long val) const;
    inline bool check_bounds(const String&src, const String&old,
                             int& notifier) const;

  private:
    bool has_lower_;
    bool has_upper_;
    long lower_;
    long upper_;
};

class BoundedCopySourceRange : public SourceRange {
  public:
    inline BoundedCopySourceRange(const parameters& p);
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
  private:
    Bounds bounds_;
};


class BoundedCountSourceRange : public SourceRange {
  public:
    inline BoundedCountSourceRange(const parameters& p);
  protected:
    virtual void notify(Str sink_key, SinkRange* sink, const Datum* src, const String& old_value, int notifier) const;
  private:
    Bounds bounds_;
};

inline Str SourceRange::ibegin() const {
    return ibegin_;
}

inline Str SourceRange::iend() const {
    return iend_;
}

inline Join* SourceRange::join() const {
    return join_;
}

inline int SourceRange::joinpos() const {
    return joinpos_;
}

inline bool SourceRange::empty() const {
    return results_.empty();
}

inline void SourceRange::clear_without_deref() {
    results_.clear();
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

inline InvalidatorRange::InvalidatorRange(const parameters& p)
    : SourceRange(p) {
}

inline SubscribedRange::SubscribedRange(const parameters& p)
    : SourceRange(p), server_(p.server) {
}

inline CopySourceRange::CopySourceRange(const parameters& p)
    : SourceRange(p) {
}

inline CountSourceRange::CountSourceRange(const parameters& p)
    : SourceRange(p) {
}

inline MinSourceRange::MinSourceRange(const parameters& p)
    : SourceRange(p) {
}

inline MaxSourceRange::MaxSourceRange(const parameters& p)
    : SourceRange(p) {
}

inline SumSourceRange::SumSourceRange(const parameters& p)
    : SourceRange(p) {
}

inline Bounds::Bounds(const Json& param)
    : has_lower_(!param.get("lbound").is_null()),
      has_upper_(!param.get("ubound").is_null()),
      lower_(param["lbound"].as_i(0)),
      upper_(param["ubound"].as_i(0)) {
}

inline bool Bounds::has_bounds() const {
    return has_lower_ || has_upper_;
}

inline bool Bounds::in_bounds(long val) const {
    if ((has_lower_ && val < lower_) ||
        (has_upper_ && val > upper_))
        return false;

    return true;
}

inline bool Bounds::check_bounds(const String& src, const String& old,
                                 int& notifier) const {
    if (!has_bounds())
        return true;

    long isrc = src.to_i();
    if ((notifier != 0 && !in_bounds(isrc)))
        return false;
    else if (notifier == 0) {
        long iold = old.to_i();
        if (!in_bounds(iold)) {
            if (in_bounds(isrc))
                notifier = 1;
        }
        else if (!in_bounds(isrc))
            notifier = -1;
    }

    return true;
}


inline BoundedCopySourceRange::BoundedCopySourceRange(const parameters& p)
    : SourceRange(p), bounds_(p.join->jvt_config()) {
}


inline BoundedCountSourceRange::BoundedCountSourceRange(const parameters& p)
    : SourceRange(p), bounds_(p.join->jvt_config()) {
}

} // namespace pq
#endif
