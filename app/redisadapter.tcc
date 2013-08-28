// -*- mode: c++ -*-
#include "redisadapter.hh"

#if HAVE_HIREDIS_HIREDIS_H
namespace pq {

RedisClient::RedisClient()
    : ctx_(nullptr), host_("127.0.0.1"), port_(6379), nout_(0) {
}

RedisClient::RedisClient(String host, uint32_t port)
    : ctx_(nullptr), host_(host), port_(port), nout_(0) {
}

RedisClient::~RedisClient() {
    clear();
}

void RedisClient::connect() {
    if (ctx_)
        mandatory_assert(false && "Redis Error: Already connected?");

    ctx_ = redisAsyncConnect(host_.c_str(), port_);

    if (ctx_->err) {
        std::cerr << "Redis Error: " << ctx_->errstr << std::endl;
        mandatory_assert(false);
    }

    nout_ = 0;
    redis_tamer_attach(ctx_);
}

void RedisClient::clear() {
    // todo: we should probably wait for the disconnect to complete
    if (ctx_)
        redisAsyncDisconnect(ctx_);
    ctx_ = nullptr;
}

tamed void RedisClient::get(Str k, tamer::event<String> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_string, &e, "GET %b",
                                        k.data(), k.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::get(Str k, int32_t begin, tamer::event<String> e) {
    getrange(k, begin, -1, e);
}

tamed void RedisClient::getrange(Str k, int32_t begin, int32_t end, tamer::event<String> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_string, &e, "GETRANGE %b %d %d",
                                        k.data(), (size_t)k.length(), begin, end);
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::set(Str k, Str v, tamer::event<> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_void, &e, "SET %b %b",
                                        k.data(), (size_t)k.length(),
                                        v.data(), (size_t)v.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::append(Str k, Str v, tamer::event<> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_void, &e, "APPEND %b %b",
                                        k.data(), (size_t)k.length(),
                                        v.data(), (size_t)v.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::increment(Str k, tamer::event<> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_void, &e, "INCR %b",
                                        k.data(), (size_t)k.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::length(Str k, tamer::event<int32_t> e) {
    {
        int32_t err =  redisAsyncCommand(ctx_, redis_cb_int32, &e, "STRLEN %b",
                                         k.data(), (size_t)k.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::sadd(Str k, Str v, tamer::event<> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_void, &e, "SADD %b %b",
                                        k.data(), (size_t)k.length(),
                                        v.data(), (size_t)v.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::smembers(Str k, tamer::event<result_set> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_set, &e, "SMEMBERS %b",
                                        k.data(), (size_t)k.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::zadd(Str k, Str v, int32_t score, tamer::event<> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_void, &e, "ZADD %b %d %b",
                                        k.data(), (size_t)k.length(),
                                        score,
                                        v.data(), (size_t)v.length());
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::zrangebyscore(Str k, int32_t begin, int32_t end,
                                      tamer::event<result_set> e) {
    {
        int32_t err = redisAsyncCommand(ctx_, redis_cb_set, &e, "ZRANGEBYSCORE %b %d (%d",
                                        k.data(), (size_t)k.length(), begin, end);
        mandatory_assert(err == REDIS_OK);
        ++nout_;
    }
    twait { e.at_trigger(make_event()); }
    check_pace();
}

tamed void RedisClient::pace(tamer::event<> e) {
    if (nout_ >= nout_hi) {
        if (!pacer_) {
            twait { pacer_ = make_event(); }
            e();
        }
        else
            pacer_.at_trigger(e);
    }
    else
        e();
}

void RedisClient::check_pace() {
    --nout_;
    if (pacer_ && nout_ <= nout_lo)
        pacer_();
}

void RedisClient::done_get(Str) {
}

void redis_check_reply(redisAsyncContext* c, void* reply) {
    if (!reply) {
        std::cerr << "Redis Error: " << c->errstr << std::endl;
        mandatory_assert(false && "Redis command error.");
    }
}

void redis_cb_void(redisAsyncContext* c, void* reply, void* privdata) {
    redis_check_reply(c, reply);
    tamer::event<>* e = (tamer::event<>*)privdata;
    e->trigger();
}

void redis_cb_string(redisAsyncContext* c, void* reply, void* privdata) {
    redis_check_reply(c, reply);
    redisReply* r = (redisReply*)reply;
    tamer::event<String>* e = (tamer::event<String>*)privdata;
    e->trigger(Str(r->str, r->len));
}

void redis_cb_int32(redisAsyncContext* c, void* reply, void* privdata) {
    redis_check_reply(c, reply);
    tamer::event<int32_t>* e = (tamer::event<int32_t>*)privdata;
    e->trigger(((redisReply*)reply)->integer);
}

void redis_cb_set(redisAsyncContext* c, void* reply, void* privdata) {
    redis_check_reply(c, reply);
    redisReply* r = (redisReply*)reply;
    tamer::event<RedisClient::result_set>* e = (tamer::event<RedisClient::result_set>*)privdata;

    RedisClient::result_set& result = e->result();
    for (uint32_t i = 0; i < r->elements; ++i)
        result.push_back(r->element[i]->str);

    e->unblocker().trigger();
}


RedisMultiClient::RedisMultiClient(const Hosts* hosts, const Partitioner* part)
    : hosts_(hosts), part_(part) {
}

RedisMultiClient::~RedisMultiClient() {
    clear();
}

void RedisMultiClient::connect() {
    for (auto& h : hosts_->all()) {
        RedisClient* c = new RedisClient(h.name(), h.port());
        c->connect();
        clients_.push_back(c);
    }
}

void RedisMultiClient::clear() {
    for (auto& c : clients_) {
        c->clear();
        delete c;
    }
    clients_.clear();
}

tamed void RedisMultiClient::pace(tamer::event<> e) {
    tvars {
        tamer::gather_rendezvous gr;
    }

    for (auto& c : clients_)
        c->pace(gr.make_event());

    twait(gr);
    e();
}


RedisAdapterState::RedisAdapterState(redisAsyncContext *c)
    : context(c), reading(false), writing(false) {
}

tamed void redis_tamer_add_read(void* privdata) {
    tvars {
        RedisAdapterState* e = (RedisAdapterState*)privdata;
        int32_t err;
    }

    if (!e->reading) {
        e->reading = true;
        while(e->reading) {
            twait {
                e->read_event = make_event(err);
                tamer::at_fd_read(e->context->c.fd, e->read_event);
            }
            if (!err)
                redisAsyncHandleRead(e->context);
        }
    }
}

void redis_tamer_del_read(void* privdata) {
    RedisAdapterState* e = (RedisAdapterState*)privdata;
    if (e->reading) {
        e->reading = false;
        e->read_event.trigger(-1);
    }
}

tamed void redis_tamer_add_write(void* privdata) {
    tvars {
        RedisAdapterState* e = (RedisAdapterState*)privdata;
        int32_t err;
    }

    if (!e->writing) {
        e->writing = true;
        while(e->writing) {
            twait {
                e->write_event = make_event(err);
                tamer::at_fd_write(e->context->c.fd, e->write_event);
            }
            if (!err)
                redisAsyncHandleWrite(e->context);
        }
    }
}

void redis_tamer_del_write(void* privdata) {
    RedisAdapterState* e = (RedisAdapterState*)privdata;
    if (e->writing) {
        e->writing = false;
        e->write_event.trigger(-1);
    }
}

void redis_tamer_cleanup(void* privdata) {
    RedisAdapterState* e = (RedisAdapterState*)privdata;
    redis_tamer_del_read(privdata);
    redis_tamer_del_write(privdata);
    delete e;
}

int32_t redis_tamer_attach(redisAsyncContext* ac) {
    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return REDIS_ERR;

    RedisAdapterState* e = new RedisAdapterState(ac);

    /* Register functions to start/stop listening for events */
    ac->ev.addRead = redis_tamer_add_read;
    ac->ev.delRead = redis_tamer_del_read;
    ac->ev.addWrite = redis_tamer_add_write;
    ac->ev.delWrite = redis_tamer_del_write;
    ac->ev.cleanup = redis_tamer_cleanup;
    ac->ev.data = e;

    return REDIS_OK;
}

}
#endif
