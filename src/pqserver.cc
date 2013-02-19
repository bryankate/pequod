#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"

namespace pq {

ServerRange::ServerRange(const String& first, const String& last, range_type type,
			 Join* join, const Match& m)
    : interval<String>(first, last), type_(type), subtree_iend_(iend()),
      join_(join), resultkey_(String::make_uninitialized((*join)[0].key_length())) {
    std::cerr << "Creating range, match is " << m << "\n";
    (*join)[0].expand(resultkey_.mutable_udata(), m);
}

void ServerRange::notify(const Datum* d, int notifier, Server& server) const {
    // XXX PERFORMANCE the match() is often not necessary
    if (type_ == copy && join_->back().match(d->key())) {
	join_->expand(resultkey_.mutable_udata(), d->key());
	std::cerr << "Notifying " << resultkey_ << "\n";
	if (notifier >= 0)
	    server.insert(resultkey_, d->value_);
	else
	    server.erase(resultkey_);
    }
}

std::ostream& operator<<(std::ostream& stream, const ServerRange& r) {
    stream << "{" << (const interval<String>&) r;
    if (r.type_ == ServerRange::copy)
	stream << ": copy -> " << r.resultkey_;
    else
	stream << ": ??";
    return stream << "}";
}

void ServerRangeSet::push_back(ServerRange* r) {
    if (!(r->type() & types_))
	return;

    int i = nr_;
    while (sw_ > (1 << i))
	++i;
    assert(i == 0 || !(r->ibegin() < r_[i - 1]->ibegin()));
    assert(i < rangecap);

    r_[i] = r;
    if (last_ && first_ < r_[i]->ibegin())
	sw_ |= 1 << i;
    else if (last_ && r_[i]->iend() < last_) {
	++nr_;
	sw_ |= 1 << i;
    } else
	++nr_;
}

void ServerRangeSet::hard_visit(const Datum* datum) {
    while (sw_ > (1 << nr_) && !(datum->key() < r_[nr_]->ibegin())) {
	if (!(r_[nr_]->iend() < last_))
	    sw_ &= ~(1 << nr_);
	++nr_;
    }
    if (sw_ & ((1 << nr_) - 1))
	for (int i = 0; i != nr_; ++i)
	    if ((sw_ & (1 << i)) && !(datum->key() < r_[nr_]->iend())) {
		r_[i] = 0;
		sw_ &= ~(1 << i);
	    }
}

std::ostream& operator<<(std::ostream& stream, const ServerRangeSet& srs) {
    stream << "[";
    const char* sep = "";
    for (int i = 0; i != srs.nr_; ++i)
	if (srs.r_[i]) {
	    stream << sep << *srs.r_[i];
	    sep = ", ";
	}
    return stream << "]";
}

void Server::insert(const String& key, const String& value) {
    ServerRangeSet srs(this, ServerRange::copy);
    ranges_.visit_contains(key, ServerRangeCollector(srs));

    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
	d->value_ = value;
    }
    srs.notify(d, p.second ? ServerRange::notify_insert : ServerRange::notify_update);
}

void Server::erase(const String& key) {
    ServerRangeSet srs(this, ServerRange::copy);
    ranges_.visit_contains(key, ServerRangeCollector(srs));

    auto it = store_.find(key, DatumCompare());
    if (it != store_.end()) {
	Datum* d = it.operator->();
	store_.erase(it);
	srs.notify(d, ServerRange::notify_erase);
	delete d;
    }
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

    server.insert("p|10000|0000000021", "(Jennifer Jones has a secret)");

    pq::Match m;
    j[0].match("t|00001|", m);
    server.add_copy("p|10000|", "p|10000}", &j, m);
    server.add_copy("p|00002|", "p|00002}", &j, m);

    server.insert("p|10000|0000000022", "This should appear in t|00001");
    server.insert("p|00002|0000000023", "As should this");

    std::cerr << "After processing add_copy:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
}
