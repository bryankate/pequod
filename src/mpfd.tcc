// -*- mode: c++ -*-
#include "mpfd.hh"
#include <limits.h>
#include <tamer/adapter.hh>
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

void msgpack_fd::initialize() {
    wrpos_ = 0;
    wrsize_ = 0;
    wrblocked_ = false;
    rdbuf_ = String::make_uninitialized(rdcap);
    rdpos_ = 0;
    rdlen_ = 0;
    rdquota_ = rdbatch;
    rdreply_seq_ = 0;

    wrelem_.push_back(wrelem());
    wrelem_.back().sa.reserve(wrcap);
    wrelem_.back().pos = 0;

    writer_coroutine();
    reader_coroutine();
}

msgpack_fd::~msgpack_fd() {
    wrkill_();
    rdkill_();
    wrwake_();
    rdwake_();
}

void msgpack_fd::write(const Json& j) {
    wrelem* w = &wrelem_.back();
    if (w->sa.length() >= wrhiwat) {
        wrelem_.push_back(wrelem());
        w = &wrelem_.back();
        w->sa.reserve(wrcap);
        w->pos = 0;
    }
    int old_len = w->sa.length();
    msgpack::unparse(w->sa, j);
    wrsize_ += w->sa.length() - old_len;
    wrwake_();
    if (!wrblocked_ && wrelem_.front().sa.length() >= wrlowat)
        write_once();
}

void msgpack_fd::flush(tamer::event<bool> done) {
    if (wrsize_ == 0)
        done(true);
    else
        flushelem_.push_back(flushelem{std::move(done), wrpos_ + wrsize_});
}

void msgpack_fd::flush(tamer::event<> done) {
    flush(tamer::unbind<bool>(done));
}

inline void msgpack_fd::check_coroutines() {
    if (rdquota_ == 0 || !rfd_)
        rdwake_();
    if (!wfd_)
        wrwake_();
}

bool msgpack_fd::read_one_message() {
    assert(rdquota_ != 0);

 readmore:
    // if buffer empty, read more data
    if (rdpos_ == rdlen_) {
        // make new buffer or reuse existing buffer
        if (rdcap - rdpos_ < 4096) {
            if (rdbuf_.is_shared())
                rdbuf_ = String::make_uninitialized(rdcap);
            rdpos_ = rdlen_ = 0;
        }

        ssize_t amt = ::read(rfd_.value(),
                             const_cast<char*>(rdbuf_.data()) + rdpos_,
                             rdcap - rdpos_);

        if (amt != 0 && amt != (ssize_t) -1)
            rdlen_ += amt;
        else {
            if (amt == 0)
                rfd_.close();
            else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                rfd_.close(-errno);
            rdquota_ = 0;
            check_coroutines(); // wake up coroutine [if it's sleeping]
            return false;
        }
    }

    // process new data
    rdpos_ += rdparser_.consume(rdbuf_.begin() + rdpos_, rdlen_ - rdpos_,
                                rdbuf_);

    if (rdparser_.done()) {
        --rdquota_;
        if (rdquota_ == 0)
            rdwake_();          // wake up coroutine [if it's sleeping]
        return true;
    } else
        goto readmore;
}

tamed void msgpack_fd::reader_coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> rendez;
    }

    kill = rdkill_ = tamer::make_event(rendez);

    while (kill && rfd_) {
        if (rdquota_ == 0 && rdpos_ != rdlen_)
            twait { tamer::at_asap(make_event()); }
        else if (rdquota_ == 0)
            twait { tamer::at_fd_read(rfd_.value(), make_event()); }
        else if (rdreqwait_.empty() && rdreplywait_.empty())
            twait { rdwake_ = make_event(); }

        rdquota_ = rdbatch;
        while (rdquota_ && (!rdreqwait_.empty() || !rdreplywait_.empty())
               && read_one_message())
            dispatch(false);
        if (pace_recovered())
            pacer_();
    }

    for (auto& e : rdreqwait_)
        e.unblock();
    rdreqwait_.clear();
    for (auto& re : rdreplywait_)
        re.e.unblock();
    rdreplywait_.clear();
    kill();                     // avoid leak of active event
}

bool msgpack_fd::dispatch(bool exit_on_request) {
    Json& result = rdparser_.result();
    if (!rdparser_.success())
        result = Json();        // XXX reset connection
    rdparser_.reset();
    if (result.is_a() && result[0].is_i() && result[1].is_i()
        && result[0].as_i() < 0) {
        unsigned long seq = result[1].as_i();
        if (seq >= rdreply_seq_ && seq < rdreply_seq_ + rdreplywait_.size()) {
            replyelem& done = rdreplywait_[seq - rdreply_seq_];
            if (done.e.result_pointer())
                swap(*done.e.result_pointer(), result);
            done.e.unblock();
            while (!rdreplywait_.empty() && !rdreplywait_.front().e) {
                rdreplywait_.pop_front();
                ++rdreply_seq_;
            }
        }
        return false;
    } else if (!rdreqwait_.empty()) {
        tamer::event<Json>& done = rdreqwait_.front();
        if (done.result_pointer())
            swap(*done.result_pointer(), result);
        done.unblock();
        rdreqwait_.pop_front();
        return false;
    } else if (exit_on_request)
        return true;
    else {
        rdreqq_.push_back(std::move(result));
        return false;
    }
}

void msgpack_fd::check() const {
    // document invariants
    assert(!wrelem_.empty());
    for (auto& w : wrelem_)
        assert(w.pos <= w.sa.length());
    for (size_t i = 1; i < wrelem_.size(); ++i)
        assert(wrelem_[i].pos == 0);
    for (size_t i = 0; i + 1 < wrelem_.size(); ++i)
        assert(wrelem_[i].pos < wrelem_[i].sa.length());
    if (wrelem_.size() == 1)
        assert(wrelem_[0].pos < wrelem_[0].sa.length()
               || wrelem_[0].sa.empty());
    size_t wrsize = 0;
    for (auto& w : wrelem_)
        wrsize += w.sa.length() - w.pos;
    assert(wrsize == wrsize_);
}

void msgpack_fd::write_once() {
    // check();
    assert(!wrelem_.front().sa.empty());

    struct iovec iov[3];
    int iov_count = (wrelem_.size() > 3 ? 3 : (int) wrelem_.size());
    size_t total = 0;
    for (int i = 0; i != iov_count; ++i) {
        iov[i].iov_base = wrelem_[i].sa.data() + wrelem_[i].pos;
        iov[i].iov_len = wrelem_[i].sa.length() - wrelem_[i].pos;
        total += iov[i].iov_len;
    }

    ssize_t amt = writev(wfd_.value(), iov, iov_count);
    wrblocked_ = amt == 0 || amt == (ssize_t) -1;

    if (amt != 0 && amt != (ssize_t) -1) {
        wrpos_ += amt;
        wrsize_ -= amt;
        while (wrelem_.size() > 1
               && amt >= wrelem_.front().sa.length() - wrelem_.front().pos) {
            amt -= wrelem_.front().sa.length() - wrelem_.front().pos;
            wrelem_.pop_front();
        }
        wrelem_.front().pos += amt;
        if (wrelem_.front().pos == wrelem_.front().sa.length()) {
            assert(wrelem_.size() == 1);
            wrelem_.front().sa.clear();
            wrelem_.front().pos = 0;
        }
        while (!flushelem_.empty()
               && (ssize_t) (wrpos_ - flushelem_.front().wpos) >= 0) {
            flushelem_.front().e.trigger(true);
            flushelem_.pop_front();
        }
        if (pace_recovered())
            pacer_();
    } else {
        if (amt == 0)
            wfd_.close();
        else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            wfd_.close(-errno);
        check_coroutines();     // ensure coroutine is awake
    }
}

tamed void msgpack_fd::writer_coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> rendez;
    }

    kill = wrkill_ = tamer::make_event(rendez);

    while (kill && wfd_) {
        if (wrelem_.size() == 1 && wrelem_.front().sa.empty())
            twait { wrwake_ = make_event(); }
        else if (wrblocked_) {
            twait { tamer::at_fd_write(wfd_.value(), make_event()); }
            wrblocked_ = false;
        } else
            write_once();
    }

    for (auto& e : flushelem_)
        e.e.trigger((ssize_t) (wrpos_ - e.wpos) >= 0);
    flushelem_.clear();
    for (auto& e : rdreplywait_)
        if ((ssize_t) (wrpos_ - e.wpos) < 0)
            e.e.trigger(Json());
    kill();
}
