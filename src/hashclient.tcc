#include "hashclient.hh"
#include "check.hh"

namespace pq {

void RedisfdHashClient::get(Str k, tamer::event<String> e) {
    fd_.call(RedisCommand::make_get(k), e);
}

void RedisfdHashClient::getrange(Str k, int begin, int end, tamer::event<String> e) {
    fd_.call(RedisCommand::make_getrange(k, begin, end), e);
}

tamed void RedisfdHashClient::set(Str k, Str v, tamer::event<> e) {
    tvars { String r; }
    twait { fd_.call(RedisCommand::make_set(k, v), make_event(r)); }
    CHECK_EQ(r, "OK");
    e();
}

tamed void RedisfdHashClient::append(Str k, Str v, tamer::event<> e) {
    tvars { String r; }
    twait { fd_.call(RedisCommand::make_append(k, v), make_event(r)); }
    e();
}

tamed void RedisfdHashClient::increment(Str k, tamer::event<> e) {
    tvars { String r; }
    twait { fd_.call(RedisCommand::make_incr(k), make_event(r)); }
    e();
}

void RedisfdHashClient::pace(tamer::event<> e) {
    fd_.pace(e);
}

}
