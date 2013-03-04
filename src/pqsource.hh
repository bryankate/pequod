#ifndef PEQUOD_PQSOURCE_HH
#define PEQUOD_PQSOURCE_HH
#include "str.hh"
#include "string.hh"
#include "interval.hh"
#include "rb.hh"
#include "local_vector.hh"
#include <iostream>
namespace pq {
class Server;
class Join;
class Match;
class Datum;
class Table;

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
    virtual void notify(const Datum* src, const String& old_value, int notifier) const = 0;

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

class SourceAccumulator {
  public:
    inline SourceAccumulator(Table* dst_table);
    virtual ~SourceAccumulator() {}
    virtual void notify(const Datum* src) = 0;
    virtual void commit(Str dst_key) = 0;
  protected:
    Table* dst_table_;
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
    inline CountSourceAccumulator(Table* dst_table);
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
    inline MinSourceAccumulator(Table* dst_table);
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
    inline MaxSourceAccumulator(Table* dst_table);
    virtual void notify(const Datum* src);
    virtual void commit(Str dst_key);
  private:
    String val_;
    bool any_;
};


class JVSourceRange : public SourceRange {
  public:
    inline JVSourceRange(Server& server, Join* join, const Match& m,
                         Str ibegin, Str iend);
    virtual void notify(const Datum* src, const String& old_value, int notifier) const;
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

inline SourceAccumulator::SourceAccumulator(Table* dst_table)
    : dst_table_(dst_table) {
}


inline CopySourceRange::CopySourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}


inline CountSourceRange::CountSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline CountSourceAccumulator::CountSourceAccumulator(Table* dst_table)
    : SourceAccumulator(dst_table), n_(0) {
}


inline MinSourceRange::MinSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline MinSourceAccumulator::MinSourceAccumulator(Table* dst_table)
    : SourceAccumulator(dst_table), any_(false) {
}


inline MaxSourceRange::MaxSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

inline MaxSourceAccumulator::MaxSourceAccumulator(Table* dst_table)
    : SourceAccumulator(dst_table), any_(false) {
}


inline JVSourceRange::JVSourceRange(Server& server, Join* join, const Match& m, Str ibegin, Str iend)
    : SourceRange(server, join, m, ibegin, iend) {
}

}
#endif
