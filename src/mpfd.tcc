// -*- mode: c++ -*-
#include "mpfd.hh"

msgpack_fd::~msgpack_fd() {
    wrkill_();
}

tamed void msgpack_fd::read(tamer::event<Json> done) {
    // XXX locking (several clients calling read: need to enforce order)
    tvars { int r; }

    rdparser_.reset();
    while (fd_ && done) {
        if (rdpos_ != rdlen_)
            rdpos_ += rdparser_.consume(rdbuf_ + rdpos_, rdlen_ - rdpos_);
        if (rdparser_.complete()) {
            if (rdparser_.done())
                done(std::move(rdparser_.result()));
            else
                done(Json());
            return;
        }

        assert(rdpos_ == rdlen_);
        r = -10000;
        twait { fd_.read_once(rdbuf_, rdcap_, rdlen_, make_event(r)); }
        rdpos_ = 0;
        std::cerr << "read from fd once, pos " << rdpos_ << ", r=" << r << "\n";
    }

    done(Json());
}

void msgpack_fd::write(const Json& j, tamer::event<> done) {
    wrelem_.push_back(wrelem{j.unparse(), done});
    wriov_.push_back(iovec{(void*) wrelem_.back().s.data(),
                wrelem_.back().s.length()});
    wrwake_();
}

tamed void msgpack_fd::writer_coroutine() {
    tvars {
        tamer::event<> kill;
        tamer::rendezvous<> r;
        ssize_t amt;
    }

    kill = wrkill_ = tamer::make_event(r);

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
            amt = writev(fd_.value(), wriov_.data() + wrpos_, wriov_.size() - wrpos_);
            if (amt != 0 && amt != (ssize_t) -1) {
                while (wrpos_ != wriov_.size()
                       && size_t(amt) >= wriov_[wrpos_].iov_len) {
                    amt -= wriov_[wrpos_].iov_len;
                    wrelem_[wrpos_].s = String();
                    wrelem_[wrpos_].done();
                    ++wrpos_;
                }
                if (wrpos_ != wriov_.size() && amt != 0) {
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

