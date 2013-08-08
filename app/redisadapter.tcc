// -*- mode: c++ -*-
#include "redisadapter.hh"
#include "check.hh"
#include "sock_helper.hh"
#include "straccum.hh"
#include <limits.h>

namespace pq {

#ifndef IOV_MAX
#define IOV_MAX 1024
#endif

enum { debug = 0 };

String RedisCommand::make_get(const String& k) {
    StringAccum sa;
    sa << "*2\r\n$3\r\nGET\r\n";
    sa << "$" << k.length() << "\r\n" << k << "\r\n";
    return std::move(sa.take_string());
}

String RedisCommand::make_getrange(const String& k, int begin, int end) {
    StringAccum sa;
    sa << "*4\r\n$8\r\nGETRANGE\r\n";
    sa << "$" << k.length() << "\r\n" << k << "\r\n";
    static char buf_[128];
    int n = sprintf(buf_, "%d", begin);
    sa << "$" << n << "\r\n" << buf_ << "\r\n";
    n = sprintf(buf_, "%d", end);
    sa << "$" << n << "\r\n" << buf_ << "\r\n";
    return std::move(sa.take_string());
}

String RedisCommand::make_set(const String& k, const String& v) {
    StringAccum sa;
    sa << "*3\r\n$3\r\nSET\r\n";
    sa << "$" << k.length() << "\r\n" << k << "\r\n";
    sa << "$" << v.length() << "\r\n" << v << "\r\n";
    return std::move(sa.take_string());
}

String RedisCommand::make_append(const String& k, const String& v) {
    StringAccum sa;
    sa << "*3\r\n$6\r\nAPPEND\r\n";
    sa << "$" << k.length() << "\r\n" << k << "\r\n";
    sa << "$" << v.length() << "\r\n" << v << "\r\n";
    return std::move(sa.take_string());
}

String RedisCommand::make_incr(const String& k) {
    StringAccum sa;
    sa << "*2\r\n$4\r\nINCR\r\n";
    sa << "$" << k.length() << "\r\n" << k << "\r\n";
    return std::move(sa.take_string());
}

RedisSyncClient::RedisSyncClient() {
    fd_ = sock_helper::connect("localhost", 6379);
}

void RedisSyncClient::get(const String& k, String& v) {
    v = String::make_empty();
    String cmd = RedisCommand::make_get(k);
    writen(cmd.data(), cmd.length());
    if (debug)
        std::cout << cmd << std::endl;
    read_reply(v);
}

void RedisSyncClient::getrange(const String& k, int begin, int end, String& v) {
    v = String::make_empty();
    String cmd = RedisCommand::make_getrange(k, begin, end);
    writen(cmd.data(), cmd.length());
    if (debug)
        std::cout << cmd << std::endl;
    read_reply(v);
}

void RedisSyncClient::set(const String& k, const String& v) {
    String cmd = RedisCommand::make_set(k, v);
    writen(cmd.data(), cmd.length());
    if (debug)
        std::cout << cmd << std::endl;
    String r;
    read_reply(r);
    CHECK_EQ(r, String("OK"));
}

void RedisSyncClient::append(const String& k, const String& v, int& newlen) {
    String cmd = RedisCommand::make_append(k, v);
    writen(cmd.data(), cmd.length());
    if (debug)
        std::cout << cmd << std::endl;
    String r;
    read_reply(r);
    newlen = r.to_i();
}

void RedisSyncClient::read_reply(String& v) {
    RedisReplyParser p;
    char buf[4096];
    while (!p.complete()) {
        int n = read(fd_, buf, sizeof(buf));
        for (int i = 0; i < n; ++i)
            p.consume(&buf[i], 1);
    }
    v = p.value();
}

void RedisSyncClient::incr(const String& k, int& newv) {
    String cmd = RedisCommand::make_incr(k);
    writen(cmd.data(), cmd.length());
    if (debug)
        std::cout << cmd << std::endl;
    String v;
    read_reply(v);
    newv = v.to_i();
}


void RedisSyncClient::readn(void* buf, size_t count) {
    CHECK_EQ(read(fd_, buf, count), ssize_t(count));
}

void RedisSyncClient::writen(const void* buf, size_t count) {
    CHECK_EQ(write(fd_, buf, count), ssize_t(count));
}

void RedisSyncClient::skipcrlf() {
    char c;
    readn(&c, 1);
    mandatory_assert(c == '\r');
    readn(&c, 1);
    mandatory_assert(c == '\n');
}

void RedisSyncClient::read_till_cr(String& v) {
    char c;
    while (1) {
        readn(&c, sizeof(c));
        if (c == '\r')
            break;
        v.append(&c, 1);
    }
    readn(&c, sizeof(c));
    mandatory_assert(c == '\n');
}

RedisReplyParser::RedisReplyParser() : state_(Type) {
}

void RedisReplyParser::reset() {
    v_ = String::make_empty();
    state_ = Type;
}

bool RedisReplyParser::complete() {
    return state_ == Done;
}

String& RedisReplyParser::value() {
    return v_;
}

bool RedisReplyParser::has_value() {
    return type_ == '$' && bstr_len_ >= 0;
}

int RedisReplyParser::consume(const char* buf, size_t length) {
    mandatory_assert(state_ != Done);
    const char* p = buf;
    const char* const e = buf + length;
    while (state_ != Done && p < e) {
        switch (state_) {
        case Type:
            type_ = *p;
            state_ = ReadVStr;
            ++p;
            break;
        case ReadVStr:
            while (p < e && *p != '\r')
                v_.append(p++, 1);
            if (p < e && *p == '\r') {
                ++p;
                state_ = VStrLF;
            }
            break;
        case VStrLF:
            CHECK_EQ(*p, '\n');
            ++p;
            if (type_ == '$') {
                bstr_len_ = v_.to_i();
                if (bstr_len_ >= 0) {
                    v_ = String::make_uninitialized(bstr_len_);
                    bstr_pos_ = 0;
                    state_ = BVStr;
                } else {
                    v_ = String::make_empty();
                    state_ = Done;
                }
            } else
                state_ = Done;
            break;
        case BVStr: {
            int n = std::min(size_t(bstr_len_ - bstr_pos_), size_t(e - p));
            memcpy(v_.mutable_data() + bstr_pos_, p, n);
            bstr_pos_ += n;
            p += n;
            if (bstr_pos_ == bstr_len_)
                state_ = BVStrCR;
            }break;
        case BVStrCR:
            CHECK_EQ(*p, '\r');
            state_ = BVStrLF;
            ++p;
            break;
        case BVStrLF:
            CHECK_EQ(*p, '\n');
            state_ = Done;
            ++p;
            break;
        default:
            mandatory_assert(0);
        }
    }
    return p - buf;
}


redis_fd::~redis_fd() {
    wrkill_();
    rdkill_();
    wrwake_();
    rdwake_();
}

void redis_fd::write(const Str req) {
    wrelem* w = &wrelem_.back();
    if (w->sa.length() >= wrhiwat) {
        wrelem_.push_back(wrelem());
        w = &wrelem_.back();
        w->sa.reserve(wrcap);
        w->pos = 0;
    }
    int old_len = w->sa.length();
    w->sa << req;
    wrsize_ += w->sa.length() - old_len;
    wrwake_();
    if (!wrblocked_ && wrelem_.front().sa.length() >= wrlowat)
        write_once();
}

void redis_fd::read(tamer::event<String> receiver) {
    while ((rdpos_ != rdlen_ || !rdblocked_)
           && rdwait_.size() && read_once(rdwait_.front().result_pointer())) {
        rdwait_.front().unblock();
        rdwait_.pop_front();
    }
    if ((rdpos_ != rdlen_ || !rdblocked_)
        && read_once(receiver.result_pointer()))
        receiver.unblock();
    else {
        rdwait_.push_back(receiver);
        rdwake_();
    }
}

bool redis_fd::read_once(String* receiver) {
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
    rdpos_ += rdparser_.consume(rdbuf_.begin() + rdpos_, rdlen_ - rdpos_);
    if (rdparser_.complete()) {
        mandatory_assert(receiver);
        std::swap(*receiver, rdparser_.value());
        rdparser_.reset();
        return true;
    } else
        goto readmore;
}

tamed void redis_fd::reader_coroutine() {
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
        } else if (read_once(rdwait_.front().result_pointer())) {
            rdwait_.front().unblock();
            rdwait_.pop_front();
            if (pace_recovered())
                pacer_();
        }
    }

    for (auto& e : rdwait_)
        e.unblock();
    rdwait_.clear();
    kill();
}

void redis_fd::check() const {
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

void redis_fd::write_once() {
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

tamed void redis_fd::writer_coroutine() {
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

void RedisfdHashClient::get(Str k, tamer::event<String> e) {
    if (debug)
        std::cout << "get " << k << "\n";
    fd_.call(RedisCommand::make_get(k), e);
}

void RedisfdHashClient::getrange(Str k, int begin, int end, tamer::event<String> e) {
    if (debug)
        std::cout << "getrange " << k << " [" << begin << ", " << end << ")\n";
    fd_.call(RedisCommand::make_getrange(k, begin, end), e);
}

tamed void RedisfdHashClient::set(Str k, Str v, tamer::event<> e) {
    tvars { String r; }
    if (debug)
        std::cout << "set " << k << ", " << v << "\n";
    twait { fd_.call(RedisCommand::make_set(k, v), make_event(r)); }
    CHECK_EQ(r, "OK");
    e();
}

tamed void RedisfdHashClient::append(Str k, Str v, tamer::event<> e) {
    tvars { String r; }
    if (debug)
        std::cout << "append " << k << ", " << v << "\n";
    twait { fd_.call(RedisCommand::make_append(k, v), make_event(r)); }
    e();
}

tamed void RedisfdHashClient::increment(Str k, tamer::event<> e) {
    tvars { String r; }
    if (debug)
        std::cout << "increment " << k << "\n";
    twait { fd_.call(RedisCommand::make_incr(k), make_event(r)); }
    e();
}

void RedisfdHashClient::pace(tamer::event<> e) {
    fd_.pace(e);
}

}

tamed void test_redis_async() {
    tvars {
        pq::RedisfdHashClient* client;
        tamer::fd fd;
        String v;
    }
    twait { tamer::tcp_connect(in_addr{htonl(INADDR_LOOPBACK)}, 6379, make_event(fd)); }
    client = new pq::RedisfdHashClient(fd);
    twait { client->set("k1", "v1", make_event()); }
    twait { client->get("k1", make_event(v)); }
}
