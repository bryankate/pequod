#ifndef PARTITIONS_HH_
#define PARTITIONS_HH_
#include "keyrange.hh"
#include "str.hh"
#include <vector>
class Json;

namespace pq {
class partition_iterator;

class partition1 {
  public:
    enum partition_type {
	type_mask = 3,
	decimal = 0,
	binary = 1,
	text = 2,

	div = 4,
	decimal_div = div | decimal,
	binary_div = div | binary,
	text_div = div | text,

	default_flag = 8
    };

    partition1(Str prefix, int server);
    partition1(Str prefix, partition_type type, int digits,
               int server_offset, int nservers);

    inline Str prefix() const;

    inline int compare(Str x) const;
    inline bool operator<(const partition1 &x) const;

    inline bool is_default() const;
    inline int server(uint32_t value) const;
    inline int first_server() const;
    inline int last_server() const;
    inline int nservers() const;

    inline uint32_t npos() const;
    uint32_t position(Str s) const;
    int emit_pos(char *buf, uint32_t pp) const;

    Json unparse_json() const;
    static partition1 parse_json(const Json &j);

  private:
    char prefix_[12];           // prefix, including termination char
    int prefix_len_;            // length of prefix
    partition_type type_;       // type of data following `prefix`
    int digits_;                // number of digits to use for modulus
    uint32_t digits_npos_;      // calculated based on `type` + `digits`
    int server_offset_;         // first server
    int nservers_;              // # servers

    partition1 next_partition(int default_server) const;

    friend class partition_set;
    friend class partition_iterator;
};

class partition_set {
  public:
    explicit partition_set(int default_server);
    partition_set(int default_server, const Json &j);

    void add(const partition1 &p);

    partition_iterator begin() const;
    partition_iterator end() const;
    partition_iterator find(Str s) const;

    void analyze(const String &first, const String &last,
                 unsigned limit, std::vector<keyrange> &result) const;

    Json unparse_json() const;
    String unparse() const;

    static void test();

  private:
    int default_server_;
    enum { first_finger = '`', nfingers = 32 };
    uint8_t finger_[nfingers];
    std::vector<partition1> p_;

    inline const partition1 *begin_p1() const;
    inline const partition1 *end_p1() const;
    void print_fail(int line, const std::vector<keyrange> &kr) const;
    friend class partition_iterator;
};

class partition_iterator {
  public:
    inline partition_iterator();

    Str key() const;
    inline bool is_default() const;
    inline int server() const;
    inline int group_first_server() const;
    inline int group_last_server() const;

    inline void operator++();
    inline void advance_group();
    inline void operator--();
    inline void reverse_advance_group();

    inline bool operator==(const partition_iterator &x) const;
    inline bool operator!=(const partition_iterator &x) const;
    inline bool operator<(const partition_iterator &x) const;

  private:
    const partition_set *ps_;
    const partition1 *p1_;
    uint32_t pp_;
    mutable int buflen_;
    mutable char buf_[28];

    inline partition_iterator(const partition_set *ps, const partition1 *p1);
    friend class partition_set;
};


class Partitioner {
  public:
    explicit Partitioner(int default_owner, int nbacking = 0);
    virtual inline ~Partitioner();
    inline void analyze(const String &first, const String &last,
                        unsigned limit, std::vector<keyrange> &result) const;
    inline int owner(const String &key) const;
    /** Return whether the host with sequence id @seqid is a backend server.
     */
    inline bool is_backend(int seqid) const;

    static Partitioner *make(const String &name, uint32_t nhosts, uint32_t default_owner);
    static Partitioner *make(const String &name, uint32_t nbacking, uint32_t nhosts, uint32_t default_owner);

    String unparse() const;

  protected:
    int nbacking_;
    partition_set ps_;
};


inline Str partition1::prefix() const {
    return Str(prefix_, prefix_len_);
}

inline int partition1::compare(Str x) const {
    int cmp = memcmp(prefix_, x.data(), std::min(prefix_len_, x.length()));
    return cmp ? cmp : prefix_len_ > x.length();
}

inline bool partition1::operator<(const partition1 &x) const {
    return Str::compare(prefix(), x.prefix()) < 0;
}

inline bool partition1::is_default() const {
    return type_ & default_flag;
}

inline int partition1::server(uint32_t value) const {
    if (nservers_ <= 1)
	return server_offset_;
    else
	return server_offset_ + (value % nservers_);
}

inline int partition1::first_server() const {
    return server_offset_;
}

inline int partition1::last_server() const {
    return server_offset_ + nservers_;
}

inline int partition1::nservers() const {
    return nservers_;
}

inline uint32_t partition1::npos() const {
    return type_ & div ? nservers_ : digits_npos_;
}


inline const partition1 *partition_set::begin_p1() const {
    return p_.data();
}

inline const partition1 *partition_set::end_p1() const {
    return p_.data() + p_.size();
}

inline partition_iterator partition_set::begin() const {
    return partition_iterator(this, begin_p1());
}

inline partition_iterator partition_set::end() const {
    return partition_iterator(this, end_p1());
}


inline partition_iterator::partition_iterator()
    : ps_(), p1_() {
}

inline partition_iterator::partition_iterator(const partition_set *ps,
                                              const partition1 *p1)
    : ps_(ps), p1_(p1), pp_(0), buflen_(-2) {
}

inline bool partition_iterator::is_default() const {
    return p1_->is_default();
}

inline int partition_iterator::server() const {
    return p1_->server(pp_);
}

inline int partition_iterator::group_first_server() const {
    return p1_->first_server();
}

inline int partition_iterator::group_last_server() const {
    return p1_->last_server();
}

inline bool partition_iterator::operator==(const partition_iterator &x) const {
    assert(ps_ == x.ps_);
    return p1_ == x.p1_ && pp_ == x.pp_;
}

inline bool partition_iterator::operator!=(const partition_iterator &x) const {
    return !(*this == x);
}

inline bool partition_iterator::operator<(const partition_iterator &x) const {
    assert(ps_ == x.ps_);
    return p1_ < x.p1_ || (p1_ == x.p1_ && pp_ < x.pp_);
}

inline void partition_iterator::advance_group() {
    assert(p1_ != ps_->end_p1());
    ++p1_;
    pp_ = 0;
    buflen_ = -2;
}

inline void partition_iterator::operator++() {
    assert(p1_ != ps_->end_p1());
    ++pp_;
    if (pp_ == p1_->npos())
        advance_group();
    else
        buflen_ = std::min(buflen_, -1);
}

inline void partition_iterator::reverse_advance_group() {
    assert(p1_ != ps_->begin_p1());
    --p1_;
    pp_ = 0;
    buflen_ = -2;
}

inline void partition_iterator::operator--() {
    assert(pp_ || p1_ != ps_->begin_p1());
    if (pp_) {
	--pp_;
	buflen_ = std::min(buflen_, -1);
    } else {
	reverse_advance_group();
	pp_ = p1_->npos() - 1;
    }
}


inline Partitioner::Partitioner(int default_owner, int nbacking)
    : nbacking_(nbacking), ps_(default_owner) {
}

inline Partitioner::~Partitioner() {
}

inline void Partitioner::analyze(const String &first, const String &last,
                                 unsigned limit,
                                 std::vector<keyrange> &result) const {
    ps_.analyze(first, last, limit, result);
}

inline int Partitioner::owner(const String &key) const {
    return ps_.find(key).server();
}

inline bool Partitioner::is_backend(int seqid) const {
    return seqid < nbacking_;
}

}
#endif
