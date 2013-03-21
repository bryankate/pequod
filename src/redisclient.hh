#ifndef REDIS_CLIENT_HH
#define REDIS_CLIENT_HH
#include "str.hh"
#include "string.hh"

namespace pq {

class RedisReplyParser {
  public:
    RedisReplyParser();
    int consume(const char* buf, size_t length);
    bool complete();
    void reset();
    bool has_value();
    String& value();
  private:
    enum { Type, ReadVStr, VStrLF, BVStr, BVStrCR, BVStrLF, Done};
    char type_;
    int state_;
    int bstr_len_;
    int bstr_pos_;
    String v_;
};

class RedisCommand {
  public:
    static String make_get(const String& k);
    static String make_getrange(const String& k, int begin, int end);
    static String make_set(const String& k, const String& v);
    static String make_append(const String& k, const String& v);
};

class RedisSyncClient {
  public:
    RedisSyncClient();
    void get(const String& k, String& v);
    void getrange(const String& k, int begin, int end, String& v);
    void set(const String& k, const String& v);
    void append(const String& k, const String& v, int& newlen);
  private:
    void read_reply(String& v);
    void read_till_cr(String& v);
    void readn(void* buf, size_t count);
    void skipcrlf();
    void writen(const void* buf, size_t count);
    int fd_;
};

};

#endif
