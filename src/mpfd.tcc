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

void msgpack_fd::write(const Json& j, tamer::event<> done) {
    //std::cerr << "want to write " << j.unparse() << "\n";
    wrelem_.push_back(wrelem{msgpack::compact_unparser().unparse(j), done});
    wriov_.push_back(iovec{(void*) wrelem_.back().s.data(),
                wrelem_.back().s.length()});
    wrwake_();
}

tamed void msgpack_fd::reader_coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> rendez;
        ssize_t amt;
        int r;
        // size_t nr = 0;
    }

    kill = rdkill_ = tamer::make_event(rendez);

    while (kill && fd_) {
        if (rdwait_.empty()) {
            twait { rdwake_ = make_event(); }
            continue;
        }

        if (rdpos_ != rdlen_)
            rdpos_ += rdparser_.consume(rdbuf_ + rdpos_, rdlen_ - rdpos_);
        if (rdparser_.complete()) {
            //if (++nr % 1024 == 0)
            // std::cerr << rdparser_.result() << "\n";
            if (rdparser_.done())
                rdwait_.front()(std::move(rdparser_.result()));
            else
                rdwait_.front()(Json());
            rdwait_.pop_front();
            rdparser_.reset();
            continue;
        }

        assert(rdpos_ == rdlen_);
        twait { fd_.read_once(rdbuf_, rdcap_, rdlen_, make_event(r)); }
        if (r < 0)
            fd_.error_close(r);
        rdpos_ = 0;
    }

    kill();
}

tamed void msgpack_fd::writer_coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> rendez;
        ssize_t amt;
        size_t wrendpos;
    }

    kill = wrkill_ = tamer::make_event(rendez);

    while (kill && fd_) {
        if (wrpos_ == wriov_.size()) {
            wrelem_.clear();
            wriov_.clear();
            wrpos_ = 0;
        } else if (wrpos_ >= 4096) {
            for (size_t i = wrpos_; i != wrelem_.size(); ++i) {
                wrelem_[i - wrpos_] = wrelem_[i];
                wriov_[i - wrpos_] = wriov_[i];
            }
            wrelem_.resize(wrelem_.size() - wrpos_);
            wriov_.resize(wriov_.size() - wrpos_);
            wrpos_ = 0;
        }

        if (wrpos_ == wriov_.size())
            twait { wrwake_ = make_event(); }
        else {
            wrendpos = wriov_.size();
            if (wrendpos - wrpos_ > IOV_MAX)
                wrendpos = wrpos_ + IOV_MAX;
            amt = writev(fd_.value(), wriov_.data() + wrpos_, wrendpos - wrpos_);
            if (amt != 0 && amt != (ssize_t) -1) {
                while (wrpos_ != wrendpos
                       && size_t(amt) >= wriov_[wrpos_].iov_len) {
                    amt -= wriov_[wrpos_].iov_len;
                    wrelem_[wrpos_].s = String();
                    wrelem_[wrpos_].done();
                    ++wrpos_;
                }
                if (wrpos_ != wrendpos && amt != 0) {
                    wriov_[wrpos_].iov_base = (char*) wriov_[wrpos_].iov_base + amt;
                    wriov_[wrpos_].iov_len -= amt;
                }
            } else if (amt == 0)
                fd_.close();
            else if (errno == EAGAIN || errno == EWOULDBLOCK)
                twait { tamer::at_fd_write(fd_.value(), make_event()); }
            else if (errno != EINTR)
                fd_.error_close(-errno);
        }
    }

    kill();
}

