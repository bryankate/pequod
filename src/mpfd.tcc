// -*- mode: c++ -*-
#include "mpfd.hh"
#include <limits.h>
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

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
    msgpack::compact_unparser().unparse(w->sa, j);
    wrsize_ += w->sa.length() - old_len;
    wrwake_();
    if (!wrblocked_ && wrelem_.front().sa.length() >= wrlowat)
        write_once();
}

bool msgpack_fd::read_one_message(Json* result_pointer) {
    assert(rdquota_ != 0);

 readmore:
    // if buffer empty, read more data
    if (rdpos_ == rdlen_) {
        // make new buffer or reuse existing buffer
        if (rdcap - rdpos_ < 4096) {
            if (rdbuf_.data_shared())
                rdbuf_ = String::make_uninitialized(rdcap);
            rdpos_ = rdlen_ = 0;
        }

        ssize_t amt = ::read(fd_.value(),
                             const_cast<char*>(rdbuf_.data()) + rdpos_,
                             rdcap - rdpos_);

        if (amt != 0 && amt != (ssize_t) -1)
            rdlen_ += amt;
        else {
            if (amt == 0)
                fd_.close();
            else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
                fd_.close(-errno);
            rdquota_ = 0;
            rdwake_();          // wake up coroutine [if it's sleeping]
            return false;
        }
    }

    // process new data
    if (rdparser_.complete()) {
        if (result_pointer)
            rdparser_.reset(*result_pointer);
        else
            rdparser_.reset();
    }

    rdpos_ += rdparser_.consume(rdbuf_.begin() + rdpos_, rdlen_ - rdpos_,
                                rdbuf_);

    if (rdparser_.complete()) {
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

    while (kill && fd_) {
        if (rdquota_ == 0 && rdpos_ != rdlen_)
            twait { tamer::at_asap(make_event()); }
        else if (rdquota_ == 0)
            twait { tamer::at_fd_read(fd_.value(), make_event()); }
        else if (rdreqwait_.empty() && rdreplywait_.empty())
            twait { rdwake_ = make_event(); }

        rdquota_ = rdbatch;
        while (rdquota_ && (!rdreqwait_.empty() || !rdreplywait_.empty())
               && read_one_message(0))
            dispatch(0);
        if (pace_recovered())
            pacer_();
    }

    for (auto& e : rdreqwait_)
        e.unblock();
    rdreqwait_.clear();
    for (auto& e : rdreplywait_)
        e.unblock();
    rdreplywait_.clear();
    kill();                     // avoid leak of active event
}

bool msgpack_fd::dispatch(Json* result_pointer) {
    Json& result = rdparser_.result();
    if (!rdparser_.done())
        result = Json();        // XXX reset connection
    if (result.is_a() && result[0].is_i() && result[1].is_i()
        && result[0].as_i() < 0) {
        unsigned long seq = result[1].as_i();
        if (seq >= rdreply_seq_ && seq < rdreply_seq_ + rdreplywait_.size()) {
            tamer::event<Json>& done = rdreplywait_[seq - rdreply_seq_];
            if (done.result_pointer())
                swap(*done.result_pointer(), result);
            done.unblock();
            while (!rdreplywait_.empty() && !rdreplywait_.front()) {
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
    } else if (result_pointer) {
        swap(*result_pointer, result);
        return true;
    } else {
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

    ssize_t amt = writev(fd_.value(), iov, iov_count);
    wrblocked_ = amt == 0 || amt == (ssize_t) -1;

    if (amt != 0 && amt != (ssize_t) -1) {
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
        if (pace_recovered())
            pacer_();
    } else if (amt == 0)
        fd_.close();
    else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
        fd_.close(-errno);
}

tamed void msgpack_fd::writer_coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> rendez;
    }

    kill = wrkill_ = tamer::make_event(rendez);

    while (kill && fd_) {
        if (wrelem_.size() == 1 && wrelem_.front().sa.empty())
            twait { wrwake_ = make_event(); }
        else if (wrblocked_) {
            twait { tamer::at_fd_write(fd_.value(), make_event()); }
            wrblocked_ = false;
        } else
            write_once();
    }

    kill();
}
