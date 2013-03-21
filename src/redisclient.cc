#include "redisclient.hh"
#include "check.hh"
#include "sock_helper.hh"
#include "straccum.hh"

namespace pq {

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
    String sb(begin);
    sa << "$" << sb.length() << "\r\n" << sb << "\r\n";
    String se(end);
    sa << "$" << se.length() << "\r\n" << se << "\r\n";
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

};
