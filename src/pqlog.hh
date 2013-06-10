#ifndef PQLOG_HH_
#define PQLOG_HH_

#include "str.hh"
#include "json.hh"
#include "time.hh"
#include <iostream>
#include <map>
#include <set>
#include <initializer_list>

namespace pq {

class Log {
  public:
    inline Log(uint64_t epoch = 0);

    template <typename T>
    inline void record(Str key, const T& value);
    template <typename T>
    inline void record_at(Str key, uint64_t time, const T& value);
    inline void clear();
    inline const Json& as_json() const;
    inline void write_json(std::ostream& s) const;

  private:
    Json log_;
    uint64_t epoch_;
};

class LogDiff {
  public:
    inline LogDiff(std::initializer_list<String> keys);

    inline void add(Str key, int32_t value);
    inline void checkpoint(Log& log);

  private:
    std::set<String> keys_;
    std::map<String, int32_t> diff_;
};


inline Log::Log(uint64_t epoch) : epoch_(epoch) {
}

template <typename T>
inline void Log::record(Str key, const T& value) {
    record_at(key, tstamp(), value);
}

template <typename T>
inline void Log::record_at(Str key, uint64_t time, const T& value) {
    assert(time > epoch_);
    log_.get_insert(key).set(String(time - epoch_), value);
}

inline void Log::clear() {
    log_ = Json();
}

inline const Json& Log::as_json() const {
    return log_;
}

inline void Log::write_json(std::ostream& s) const {
    s << log_ << std::endl;
}

inline LogDiff::LogDiff(std::initializer_list<String> keys)
    : keys_(keys) {
}

inline void LogDiff::add(Str key, int32_t value) {
    assert(keys_.find(key) != keys_.end());
    diff_[key] += value;
}

inline void LogDiff::checkpoint(Log& log) {
    uint64_t time = tstamp();
    for (String k : keys_)
        log.record_at(k, time, diff_[k]);
    diff_.clear();
}

}

#endif
