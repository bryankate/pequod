#ifndef PEQUOD_PQSOURCE_HH
#define PEQUOD_PQSOURCE_HH
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "rb.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "local_vector.hh"
#include "pqdatum.hh"
#include <iostream>
namespace pq {
class Server;
class Match;
class Datum;
class Table;

struct SinkBound {
    SinkBound(Table* t, bool single_sink);
    void update(StoreIterator it, Table* t, bool insert);
    inline StoreIterator hint();
    inline bool single_sink() const;
  private:
    StoreIterator first_;
    StoreIterator last_;
    bool single_sink_;
};

typedef std::pair<String, SinkBound*> resultkey_type;

class SourceRange {
  public:
    SourceRange(Server& server, Join* join, const Match& m,
                Str ibegin, Str iend, SinkBound* sb);
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
    virtual void notify(const Datum* src, const String& old_value, int notifier,
                        bool known_match);

    friend std::ostream& operator<<(std::ostream&, const SourceRange&);

    static uint64_t allocated_key_bytes;

  private:
    Str ibegin_;
    Str iend_;
    Str subtree_iend_;
  public:
    rblinks<SourceRange> rblinks_;
  protected:
    virtual void notify(resultkey_type& r, const Datum* src,
                        const String& old_value, int notifier) = 0;

    Join* join_;
    Table* dst_table_;  // todo: move this to the join?
    mutable local_vector<resultkey_type, 4> resultkeys_;
  private:
    char buf_[32];
};

class Bounded {
  public:
    inline Bounded(const Json& param);

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

class CopySourceRange : public SourceRange, public Bounded {
  public:
    inline CopySourceRange(Server& server, Join* join, const Match& m,
                           Str ibegin, Str iend, SinkBound* sb);
  protected:
    virtual void notify(resultkey_type& r, const Datum* src,
                        const String& old_value, int notifier);
};


class CountSourceRange : public SourceRange, public Bounded {
  public:
    inline CountSourceRange(Server& server, Join* join, const Match& m,
                            Str ibegin, Str iend, SinkBound* sb);
  protected:
    virtual void notify(resultkey_type& r, const Datum* src,
                        const String& old_value, int notifier);
};

class MinSourceRange : public SourceRange {
  public:
    inline MinSourceRange(Server& server, Join* join, const Match& m,
                          Str ibegin, Str iend, SinkBound* sb);
  protected:
    virtual void notify(resultkey_type& r, const Datum* src,
                        const String& old_value, int notifier);
};

class MaxSourceRange : public SourceRange {
  public:
    inline MaxSourceRange(Server& server, Join* join, const Match& m,
                          Str ibegin, Str iend, SinkBound* sb);
  protected:
    virtual void notify(resultkey_type& r, const Datum* src,
                        const String& old_value, int notifier);
};

class SumSourceRange : public SourceRange {
  public:
    inline SumSourceRange(Server& server, Join* join, const Match& m,
                          Str ibegin, Str iend, SinkBound* sb);
  protected:
    virtual void notify(resultkey_type& r, const Datum* src,
                        const String& old_value, int notifier);
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

inline interval<Str> SourceRange::interval() const {
    return make_interval(ibegin(), iend());
}

inline Str SourceRange::subtree_iend() const {
    return subtree_iend_;
}

inline void SourceRange::set_subtree_iend(Str subtree_iend) {
    subtree_iend_ = subtree_iend;
}

inline Bounded::Bounded(const Json& param)
    : has_lower_(!param.get("lbound").is_null()),
      has_upper_(!param.get("ubound").is_null()),
      lower_(param["lbound"].as_i(0)),
      upper_(param["ubound"].as_i(0)) {
}

inline bool Bounded::has_bounds() const {
    return has_lower_ || has_upper_;
}

inline bool Bounded::in_bounds(long val) const {
    if ((has_lower_ && val < lower_) ||
        (has_upper_ && val > upper_))
        return false;

    return true;
}

inline bool Bounded::check_bounds(const String& src, const String&old,
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

inline CopySourceRange::CopySourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend,
                                        SinkBound* sb)
    : SourceRange(server, join, m, ibegin, iend, sb), Bounded(join->jvt_config()) {
}


inline CountSourceRange::CountSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend,
                                          SinkBound* sb)
    : SourceRange(server, join, m, ibegin, iend, sb), Bounded(join->jvt_config()) {
}

inline MinSourceRange::MinSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend,
                                      SinkBound* sb)
    : SourceRange(server, join, m, ibegin, iend, sb) {
}

inline MaxSourceRange::MaxSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend,
                                      SinkBound* sb)
    : SourceRange(server, join, m, ibegin, iend, sb) {
}

inline SumSourceRange::SumSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend,
                                      SinkBound* sb)
    : SourceRange(server, join, m, ibegin, iend, sb) {
}

inline StoreIterator SinkBound::hint() {
    return last_;
}

inline bool SinkBound::single_sink() const {
    return single_sink_;
}

}
#endif
