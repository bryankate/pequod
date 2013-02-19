#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"

namespace pq {

ServerRange::ServerRange(const String& first, const String& last,
			 Join* join, const Match& m)
    : interval<String>(first, last), subtree_iend_(iend()),
      join_(join), resultkey_(String::make_uninitialized((*join)[0].key_length())) {
    join->back().first(resultkey_.mutable_udata(), m);
}

void ServerRange::insert(const Datum& d, Server& server) const {
    if (join_->back().match(d.key())) {
	join_->expand(resultkey_.mutable_udata(), d.key());
	server.insert(resultkey_, d.value_);
    }
}

void ServerRange::erase(const Datum& d, Server& server) const {
    if (join_->back().match(d.key())) {
	join_->expand(resultkey_.mutable_udata(), d.key());
	server.erase(resultkey_);
    }
}

void Server::insert(const String& key, const String& value) {
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    if (p.second)
	store_.insert_commit(*new Datum(key, value), cd);
    else
	p.first->value_ = value;
}

void Server::erase(const String& key) {
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end())
	store_.erase_and_dispose(it, DatumDispose());
}

void Server::process_join(const JoinState* js, Str first, Str last) {
    Match mf, ml;
    js->match(first, mf);
    js->match(last, ml);

    const Join& join = js->join();
    int pattern = js->joinpos() + 1;
    uint8_t kf[128], kl[128];
    int kflen = join[pattern].first(kf, mf);
    int kllen = join[pattern].last(kl, ml);

    auto it = store_.lower_bound(Str(kf, kflen), DatumCompare());
    auto ilast = store_.lower_bound(Str(kl, kllen), DatumCompare());
    store_type::iterator iinsert;
    bool iinsert_valid = false;
    for (; it != ilast; ++it) {
	Match m;
	if (it->key().length() != join[pattern].key_length()
	    || !join[pattern].match(it->key(), m))
	    continue;
	if (pattern + 1 == join.size()) {
	    kflen = join[0].first(kf, js, m);
	    Datum* d = new Datum(Str(kf, kflen), it->value_);
	    if (iinsert_valid)
		iinsert = store_.insert(iinsert, *d);
	    else
		iinsert = store_.insert(*d).first;
	} else {
	    JoinState* njs = js->make_state(m);
	    process_join(njs, first, last);
	    delete njs;
	}
    }
}

} // namespace

int main(int argc, char** argv) {
    pq::Server server;

    const char *keys[] = {
	"f|00001|00002",
	"f|00001|10000",
	"p|00002|0000000000",
	"p|00002|0000000001",
	"p|00002|0000000022",
	"p|10000|0000000010",
	"p|10000|0000000018"
    }, *values[] = {
	"1",
	"1",
	"Should not appear",
	"Hello,",
	"Which is awesome",
	"My name is",
	"Jennifer Jones"
    };
    server.replace_range(keys, keys + sizeof(keys) / sizeof(keys[0]), values);

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";

    pq::Join j;
    j.assign_parse("t|<user_id:5>|<time:10>|<poster_id:5> "
		   "f|<user_id>|<poster_id> "
		   "p|<poster_id>|<time>");
    j.ref();

    pq::JoinState* js = j.make_state();
    server.process_join(js, "t|00001|0000000001", "t|00001}");

    std::cerr << "After processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
}
