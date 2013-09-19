// -*- mode: c++ -*-
#include "memcacheadapter.hh"
#include "sock_helper.hh"

#if HAVE_MEMCACHED_PROTOCOL_BINARY_H
#include <memcached/protocol_binary.h>
#endif

namespace pq {

MemcacheClient::MemcacheClient()
    : host_("127.0.0.1"), port_(11211), seq_(0), reading_(false), wbuffsz_(0) {
}

MemcacheClient::MemcacheClient(String host, uint32_t port)
    : host_(host), port_(port), seq_(0), reading_(false), wbuffsz_(0) {
}

MemcacheClient::~MemcacheClient() {
    clear();
}

tamed void MemcacheClient::connect(tamer::event<> done) {
    tvars {
        struct sockaddr_in sin;
    }

    sock_helper::make_sockaddr(host_.c_str(), port_, sin);
    twait { tamer::tcp_connect(sin.sin_addr, port_, make_event(sock_)); }
    done();
}

void MemcacheClient::clear() {
    reading_ = false;
    sock_.close();
    wbuffsz_ = 0;
    rdwait_.clear();
}

#if HAVE_MEMCACHED_PROTOCOL_BINARY_H
void check_error(const MemcacheResponse& res) {
    if (res.status != PROTOCOL_BINARY_RESPONSE_SUCCESS) {
        std::cerr << "Memcached error: " << res.strval << std::endl;
        mandatory_assert(false);
    }
}

tamed void MemcacheClient::get(Str key, tamer::event<String> e) {
    tvars {
        uint8_t data[sizeof(protocol_binary_request_get) + 128];
        uint32_t cmdlen;
        MemcacheResponse res;
    }

    res.seq = seq_++;
    cmdlen = prep_command(data, PROTOCOL_BINARY_CMD_GET, key, "", res.seq);
    twait { send_command(data, cmdlen, make_event(res)); }

    if (res.status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) {
        res.status = PROTOCOL_BINARY_RESPONSE_SUCCESS;
        res.strval = "";
    }

    check_error(res);
    e(res.strval);
}

tamed void MemcacheClient::set(Str key, Str value, tamer::event<> e) {
    tvars {
          uint8_t data[sizeof(protocol_binary_request_set) + 128];
          uint32_t cmdlen;
          MemcacheResponse res;
      }

      res.seq = seq_++;
      cmdlen = prep_command(data, PROTOCOL_BINARY_CMD_SET, key, value, res.seq);
      twait { send_command(data, cmdlen, make_event(res)); }

      check_error(res);
      e();
}

tamed void MemcacheClient::append(Str key, Str value, tamer::event<> e) {
    tvars {
        uint8_t data[sizeof(protocol_binary_request_append) + 128];
        uint32_t cmdlen;
        MemcacheResponse res;
    }

    res.seq = seq_++;
    cmdlen = prep_command(data, PROTOCOL_BINARY_CMD_APPEND, key, value, res.seq);
    twait { send_command(data, cmdlen, make_event(res)); }

    if (res.status == PROTOCOL_BINARY_RESPONSE_NOT_STORED) {
        set(key, value, e);
        return;
    }

    check_error(res);
    e();
}

tamed void MemcacheClient::increment(Str key, tamer::event<> e) {
    tvars {
        uint8_t data[sizeof(protocol_binary_request_incr) + 128];
        uint32_t cmdlen;
        MemcacheResponse res;
    }

    res.seq = seq_++;
    cmdlen = prep_command(data, PROTOCOL_BINARY_CMD_INCREMENT, key, "", res.seq);
    twait { send_command(data, cmdlen, make_event(res)); }

    if (res.status == PROTOCOL_BINARY_RESPONSE_KEY_ENOENT) {
        set(key, "1", e);
        return;
    }

    check_error(res);
    e();
}

tamed void MemcacheClient::stats(tamer::event<Json> e) {
    tvars {
        uint8_t data[sizeof(protocol_binary_request_stats) + 128];
        uint32_t cmdlen;
        MemcacheResponse res;
    }

    res.seq = seq_++;
    cmdlen = prep_command(data, PROTOCOL_BINARY_CMD_STAT, "", "", res.seq);
    twait { send_command(data, cmdlen, make_event(res)); }

    check_error(res);
    e(res.jsonval);
}

uint32_t MemcacheClient::prep_command(uint8_t* data, uint8_t op,
                                      Str key, Str value, uint32_t seq) {

    uint32_t hdrlen = sizeof(protocol_binary_request_header);
    protocol_binary_request_header* hdr = (protocol_binary_request_header*)data;
    memset(hdr, 0, hdrlen);

    hdr->request.magic = PROTOCOL_BINARY_REQ;
    hdr->request.opcode = op;
    write_in_net_order((uint8_t*)&hdr->request.keylen, (uint16_t)key.length());
    write_in_net_order((uint8_t*)&hdr->request.opaque, seq);
    uint8_t extraslen = 0;

    switch(op) {
        case PROTOCOL_BINARY_CMD_GET: {
            extraslen = 0;
            memcpy(data + hdrlen + extraslen, key.data(), key.length());
            break;
        }

        case PROTOCOL_BINARY_CMD_SET: {
            extraslen = 8;
            protocol_binary_request_set* setreq = (protocol_binary_request_set*)data;

            write_in_net_order((uint8_t*)&setreq->message.body.flags, (uint32_t)0);
            write_in_net_order((uint8_t*)&setreq->message.body.expiration, (uint32_t)0);
            memcpy(data + hdrlen + extraslen, key.data(), key.length());
            memcpy(data + hdrlen + extraslen + key.length(), value.data(), value.length());
            break;
        }

        case PROTOCOL_BINARY_CMD_INCREMENT: {
            extraslen = 20;
            protocol_binary_request_incr* incrreq = (protocol_binary_request_incr*)data;

            // we want the increment to fail if not present, so that we
            // can explicitly set the initial value to avoid the server fucking
            // up the format. signal this to the server by setting the
            // expiration to a special value
            write_in_net_order((uint8_t*)&incrreq->message.body.expiration, (uint32_t)-1);
            write_in_net_order((uint8_t*)&incrreq->message.body.initial, (uint64_t)1);
            write_in_net_order((uint8_t*)&incrreq->message.body.delta, (uint64_t)1);
            memcpy(data + hdrlen + extraslen, key.data(), key.length());
            break;
        }

        case PROTOCOL_BINARY_CMD_APPEND: {
            extraslen = 0;
            memcpy(data + hdrlen + extraslen, key.data(), key.length());
            memcpy(data + hdrlen + extraslen + key.length(), value.data(), value.length());
            break;
        }

        case PROTOCOL_BINARY_CMD_STAT: {
            extraslen = 0;
            break;
        }

        default:
            mandatory_assert(false && "Invalid request code.");
            break;
    }

    uint32_t bodylen = extraslen + key.length() + value.length();
    write_in_net_order((uint8_t*)&hdr->request.extlen, extraslen);
    write_in_net_order((uint8_t*)&hdr->request.bodylen, bodylen);

    return hdrlen + bodylen;
}

tamed void MemcacheClient::send_command(const uint8_t* data, uint32_t len,
                                        tamer::event<MemcacheResponse> e) {
    tvars {
        size_t nwritten = 0;
        int32_t ret;
    }

    wbuffsz_ += len;
    twait { sock_.write(data, len, &nwritten, make_event(ret)); }
    if (ret) {
        std::cerr << "error code is " << ret << std::endl;
    }
    mandatory_assert(!ret && "Problem writing to socket.");
    mandatory_assert(nwritten == len && "Did not write the correct number of bytes?");
    wbuffsz_ -= len;

    twait {
        rdwait_.push_back(make_event(e.result()));
        if (!reading_)
            read_loop();
    }

    e.unblocker().trigger();
}

tamed void MemcacheClient::read_loop() {
    tvars {
        int32_t err;
        size_t nread;
        protocol_binary_response_header hdr;
        uint32_t hdrlen = sizeof(protocol_binary_response_header);
        uint8_t* data = nullptr;
        uint32_t datasz = 0;
        uint32_t bodylen;
        tamer::event<MemcacheResponse> curr;
    }

    reading_ = true;

    while(reading_ && !rdwait_.empty()) {
        curr = rdwait_.front();
        rdwait_.pop_front();

        read_header:
        twait { sock_.read(&hdr, hdrlen, &nread, make_event(err)); }
        mandatory_assert(!err && "Problems reading memcached response.");
        mandatory_assert(nread == hdrlen);
        mandatory_assert(hdr.response.magic == PROTOCOL_BINARY_RES);
        mandatory_assert(read_in_net_order<uint32_t>((uint8_t*)&hdr.response.opaque) == curr.result().seq);

        bodylen = read_in_net_order<uint32_t>((uint8_t*)&hdr.response.bodylen);
        curr.result().status = read_in_net_order<uint16_t>((uint8_t*)&hdr.response.status);

        if (bodylen > datasz) {
            if (data)
                delete[] data;
            data = new uint8_t[bodylen];
            datasz = bodylen;
        }

        if (curr.result().status) {
            // get error message
            twait { sock_.read(data, bodylen, &nread, make_event(err)); }
            mandatory_assert(!err && "Problems reading memcached response.");
            mandatory_assert(nread == bodylen);

            curr.result().strval = String(data, bodylen);
            curr.unblocker().trigger();
            twait { check_pace(make_event()); }
            continue;
        }

        switch(hdr.response.opcode) {
            case PROTOCOL_BINARY_CMD_GET: {
                twait { sock_.read(data, bodylen, &nread, make_event(err)); }
                mandatory_assert(!err && "Problems reading memcached response.");
                mandatory_assert(nread == bodylen);

                uint16_t keylen = read_in_net_order<uint16_t>((uint8_t*)&hdr.response.keylen);
                curr.result().strval = String(data + hdr.response.extlen + keylen,
                                              bodylen - hdr.response.extlen - keylen);
                break;
            }

            case PROTOCOL_BINARY_CMD_SET: {
                mandatory_assert(!bodylen);
                mandatory_assert(hdr.response.cas);
                break;
            }

            case PROTOCOL_BINARY_CMD_INCREMENT: {
                twait { sock_.read(data, bodylen, &nread, make_event(err)); }
                mandatory_assert(!err && "Problems reading memcached response.");
                mandatory_assert(nread == bodylen);

                curr.result().intval = read_in_net_order<uint64_t>(data);
                break;
            }

            case PROTOCOL_BINARY_CMD_APPEND: {
                mandatory_assert(!bodylen);
                mandatory_assert(hdr.response.cas);
                break;
            }

            case PROTOCOL_BINARY_CMD_STAT: {
                mandatory_assert(!hdr.response.cas);
                mandatory_assert(!hdr.response.extlen);

                // stats are terminated by an empty packet
                if (!bodylen)
                    break;

                twait { sock_.read(data, bodylen, &nread, make_event(err)); }
                mandatory_assert(!err && "Problems reading memcached response.");
                mandatory_assert(nread == bodylen);

                uint16_t keylen = read_in_net_order<uint16_t>((uint8_t*)&hdr.response.keylen);
                curr.result().jsonval.set(Str(data, keylen), Str(data + keylen, bodylen - keylen));
                goto read_header;
            }

            default:
                mandatory_assert(false && "Unrecognized opcode in response!");
                break;
        }

        curr.unblocker().trigger();
        twait { check_pace(make_event()); }
    }

    reading_ = false;
    if (data)
        delete[] data;
}

#else

tamed void MemcacheClient::get(Str key, tamer::event<String> e) {
    mandatory_assert(false && "Memcached headers not installed!");
    e("");
}

tamed void MemcacheClient::set(Str key, Str value, tamer::event<> e) {
    mandatory_assert(false && "Memcached headers not installed!");
    e();
}

tamed void MemcacheClient::append(Str key, Str value, tamer::event<> e) {
    mandatory_assert(false && "Memcached headers not installed!");
    e();
}

tamed void MemcacheClient::increment(Str key, tamer::event<> e) {
    mandatory_assert(false && "Memcached headers not installed!");
    e();
}

#endif

tamed void MemcacheClient::get(Str key, int32_t offset, tamer::event<String> e) {
    tvars {
        String val;
    }

    if (!use_yandong_modified_server) {
        twait { get(key, make_event(val)); }

        if (offset < val.length())
            e(val.substring(offset));
    }
    else {
        twait {
            String offkey(key);
            offkey += "@";
            offkey += String(offset);

            get(offkey, make_event(val));
        }

        e(val);
    }
}

tamed void MemcacheClient::length(Str key, tamer::event<int32_t> e) {
    tvars {
        String val;
    }

    twait { get(key, make_event(val)); }
    e(val.length());
}

void MemcacheClient::done_get(Str) {
}

tamed void MemcacheClient::pace(tamer::event<> e) {
    if (wbuffsz_ >= wbuffsz_hi || rdwait_.size() >= nout_hi) {
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

tamed void MemcacheClient::check_pace(tamer::event<> e) {
    if (pacer_ && wbuffsz_ <= wbuffsz_lo && rdwait_.size() <= nout_lo) {
        pacer_();
        tamer::at_asap(e);
    }
    else
        e();
}


MemcacheMultiClient::MemcacheMultiClient(const Hosts* hosts, const Partitioner* part)
    : hosts_(hosts), part_(part) {
}

MemcacheMultiClient::~MemcacheMultiClient() {
    clear();
}

tamed void MemcacheMultiClient::connect(tamer::event<> e) {
    tvars {
        int32_t h;
        const Host* host;
        MemcacheClient* c;
    }

    for (h = 0; h < hosts_->size(); ++h) {
        host = hosts_->get_by_seqid(h);
        c = new MemcacheClient(host->name(), host->port());
        twait { c->connect(make_event()); }
        clients_.push_back(c);
    }
    e();
}

void MemcacheMultiClient::clear() {
    for (auto& c : clients_) {
        c->clear();
        delete c;
    }
    clients_.clear();
}

tamed void MemcacheMultiClient::stats(tamer::event<Json> e) {
    tvars {
        Json j = Json::make_array_reserve(this->clients_.size());
    }

    twait {
        for (uint32_t i = 0; i < clients_.size(); ++i)
            clients_[i]->stats(make_event(j[i].value()));
    }

    e(j);
}

tamed void MemcacheMultiClient::pace(tamer::event<> e) {
    tvars {
        tamer::gather_rendezvous gr;
    }

    for (auto& c : clients_)
        c->pace(gr.make_event());

    twait(gr);
    e();
}

}
