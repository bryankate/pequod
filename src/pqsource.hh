#ifndef PEQUOD_PQSOURCE_HH
#define PEQUOD_PQSOURCE_HH
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "rb.hh"
#include "json.hh"
#include "pqjoin.hh"
#include "local_vector.hh"
#include <iostream>
namespace pq {
class Server;
class Match;
class Datum;
class Table;
class ValidJoinRange;

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
    void take_results(SourceRange& r);

    enum notify_type {
	notify_erase = -1, notify_update = 0, notify_insert = 1
    };
    virtual void notify(const Datum* src, const String& old_value, int notifier) const = 0;

    friend std::ostream& operator<<(std::ostream&, const SourceRange&);

    static uint64_t allocated_key_bytes;

  private:
    Str ibegin_;
    Str iend_;
    Str subtree_iend_;
  public:
    rblinks<SourceRange> rblinks_;
  protected:
    Join* join_;
    Table* dst_table_;  // todo: move this to the join?
    struct result {
        String key;
        ValidJoinRange* sink;
    };
    mutable local_vector<result, 4> results_;
  private:
    char buf_[32];

  protected:
    void expand_results(const Datum* src) const;
};

class SourceAccumulator {
  public:
    inline SourceAccumulator(Join *join, Table* dst_table);
    virtual ~SourceAccumulator() {}
    virtual void notify(const Datum* src) = 0;
    virtual void commit(Str dst_key) = 0;
  protected:
    Join* join_;
    Table* dst_table_;  // todo: move this to the join?
};


class CopySourceRange : public SourceRange {
  public:
    inline CopySourceRange(Server& server, Join* join, const Match& m,
                           Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
};


class CountSourceRange : public SourceRange {
  public:
    inline CountSourceRange(Server& server, Join* join, const Match& m,
                            Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
};

class CountSourceAccumulator : public SourceAccumulator {
  public:
    inline CountSourceAccumulator(Join *join, Table* dst_table);
    virtual void notify(const Datum* src);
    virtual void commit(Str dst_key);
  private:
    long n_;
};


class MinSourceRange : public SourceRange {
  public:
    inline MinSourceRange(Server& server, Join* join, const Match& m,
                          Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
};

class MinSourceAccumulator : public SourceAccumulator {
  public:
    inline MinSourceAccumulator(Join *join, Table* dst_table);
    virtual void notify(const Datum* src);
    virtual void commit(Str dst_key);
  private:
    String val_;
    bool any_;
};


class MaxSourceRange : public SourceRange {
  public:
    inline MaxSourceRange(Server& server, Join* join, const Match& m,
                          Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
};

class MaxSourceAccumulator : public SourceAccumulator {
  public:
    inline MaxSourceAccumulator(Join *join, Table* dst_table);
    virtual void notify(const Datum* src);
    virtual void commit(Str dst_key);
  private:
    String val_;
    bool any_;
};


class SumSourceRange : public SourceRange {
  public:
    inline SumSourceRange(Server& server, Join* join, const Match& m,
                          Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
};

class SumSourceAccumulator : public SourceAccumulator {
  public:
    inline SumSourceAccumulator(Join *join, Table* dst_table);
    virtual void notify(const Datum* src);
    virtual void commit(Str dst_key);
  private:
    long sum_;
    bool any_;
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
    inline BoundedCopySourceRange(Server& server, Join* join, const Match& m,
                                  Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
  private:
    Bounds bounds_;
};


class BoundedCountSourceRange : public SourceRange {
  public:
    inline BoundedCountSourceRange(Server& server, Join* join, const Match& m,
                                   Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
  private:
    Bounds bounds_;
};

class BoundedCountSourceAccumulator : public SourceAccumulator {
  public:
    inline BoundedCountSourceAccumulator(Join *join, Table* dst_table);
    virtual void notify(const Datum* src);
    virtual void commit(Str dst_key);
  private:
    long n_;
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

inline interval<Str> SourceRange::interval() const {
    return make_interval(ibegin(), iend());
}

inline Str SourceRange::subtree_iend() const {
    return subtree_iend_;
}

inline void SourceRange::set_subtree_iend(Str subtree_iend) {
    subtree_iend_ = subtree_iend;
}


inline SourceAccumulator::SourceAccumulator(Join *join, Table* dst_table)
    : join_(join), dst_table_(dst_table) {
}


inline CopySourceRange::CopySourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}


inline CountSourceRange::CountSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline CountSourceAccumulator::CountSourceAccumulator(Join *join, Table* dst_table)
    : SourceAccumulator(join, dst_table), n_(0) {
}


inline MinSourceRange::MinSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline MinSourceAccumulator::MinSourceAccumulator(Join *join, Table* dst_table)
    : SourceAccumulator(join, dst_table), any_(false) {
}


inline MaxSourceRange::MaxSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline MaxSourceAccumulator::MaxSourceAccumulator(Join *join, Table* dst_table)
    : SourceAccumulator(join, dst_table), any_(false) {
}


inline SumSourceRange::SumSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline SumSourceAccumulator::SumSourceAccumulator(Join *join, Table* dst_table)
    : SourceAccumulator(join, dst_table), sum_(0), any_(false) {
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


inline BoundedCopySourceRange::BoundedCopySourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend), bounds_(join->jvt_config()) {
}


inline BoundedCountSourceRange::BoundedCountSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend), bounds_(join->jvt_config()) {
}

inline BoundedCountSourceAccumulator::BoundedCountSourceAccumulator(Join *join, Table* dst_table)
    : SourceAccumulator(join, dst_table), n_(0), bounds_(join->jvt_config()) {
}

} // namespace pq
#endif
