#ifndef PQLOG_HH_
#define PQLOG_HH_

#include "str.hh"
#include "json.hh"
#include "time.hh"
#include <iostream>

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

}

#endif
