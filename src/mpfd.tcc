// -*- mode: c++ -*-
#include "mpfd.hh"
#include <limits.h>
#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

msgpack_fd::~msgpack_fd() {
    wrkill_();
    rdkill_();
}

void msgpack_fd::write(const Json& j) {
    if (wrelem_.back().sa.length() >= wrhiwat) {
        wrelem_.push_back(wrelem());
        wrelem_.back().sa.reserve(wrcap);
        wrelem_.back().pos = 0;
    }
    msgpack::compact_unparser().unparse(wrelem_.back().sa, j);
    wrwake_();
    if (!wrblocked_ && wrelem_.front().sa.length() >= wrlowat)
        write_once();
}

void msgpack_fd::read(tamer::event<Json> receiver) {
    while (!rdblocked_ && rdwait_.size() && read_once(rdwait_.front()))
        rdwait_.pop_front();
    if (rdblocked_ || !read_once(receiver)) {
        rdwait_.push_back(receiver);
        rdwake_();
    }
}

bool msgpack_fd::read_once(tamer::event<Json> receiver) {
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
        rdblocked_ = amt == 0 || amt == (ssize_t) -1;

        if (amt != 0 && amt != (ssize_t) -1)
            rdlen_ += amt;
        else if (amt == 0)
            fd_.close();
        else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
            fd_.close(-errno);

        if (rdpos_ == rdlen_)
            return false;
    }

    // process new data
    if (rdparser_.complete()) {
        if (receiver.__get_slot0())
            rdparser_.reset(*receiver.__get_slot0());
        else
            rdparser_.reset();
    }

    rdpos_ += rdparser_.consume(rdbuf_.begin() + rdpos_, rdlen_ - rdpos_,
                                rdbuf_);

    if (rdparser_.done()) {
        receiver(std::move(rdparser_.result()));
        return true;
    } else if (rdparser_.complete()) {
        receiver(Json());       // XXX close connection?
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
        if (rdwait_.empty())
            twait { rdwake_ = make_event(); }
        else if (rdblocked_) {
            twait { tamer::at_fd_read(fd_.value(), make_event()); }
            rdblocked_ = false;
        } else if (read_once(rdwait_.front()))
            rdwait_.pop_front();
    }

    kill();
}

void msgpack_fd::check() {
    // document invariants
    assert(!wrelem_.empty());
    for (size_t i = 0; i != wrelem_.size(); ++i)
        assert(wrelem_[i].pos <= wrelem_[i].sa.length());
    for (size_t i = 1; i < wrelem_.size(); ++i)
        assert(wrelem_[i].pos == 0);
    for (size_t i = 0; i + 1 < wrelem_.size(); ++i)
        assert(wrelem_[i].pos < wrelem_[i].sa.length());
    if (wrelem_.size() == 1)
        assert(wrelem_[0].pos < wrelem_[0].sa.length()
               || wrelem_[0].sa.empty());
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
