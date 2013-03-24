#include <unistd.h>
#include <set>
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "error.hh"
#include <sys/resource.h>

namespace pq {

const Datum Datum::empty_datum{Str()};
const Datum Datum::max_datum(Str("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"));
Table Table::empty_table{Str()};

Table::Table(Str name)
    : ninsert_(0), nmodify_(0), nerase_(0), nvalidate_(0),
      nvalidate_optimized_(0), nvalidate_increasing_(0), nvalidate_skipped_(0),
      njoins_(0), flush_at_(0), all_pull_(true), namelen_(name.length()) {
    assert(namelen_ <= (int) sizeof(name_));
    memcpy(name_, name.data(), namelen_);
}

Table::~Table() {
    while (SourceRange* r = source_ranges_.unlink_leftmost_without_rebalance()) {
        r->clear_without_deref();
        delete r;
    }
    while (JoinRange* r = join_ranges_.unlink_leftmost_without_rebalance())
        delete r;
    // delete store last since join_ranges_ have refs to Datums
    while (Datum* d = store_.unlink_leftmost_without_rebalance())
        delete d;
}

void Table::add_source(SourceRange* r) {
    for (auto it = source_ranges_.begin_contains(r->interval());
	 it != source_ranges_.end(); ++it)
	if (it->join() == r->join() && it->joinpos() == r->joinpos()) {
	    // XXX may copy too much. This will not actually cause visible
	    // bugs I think?, but will grow the store
	    it->take_results(*r);
            delete r;
	    return;
	}
    source_ranges_.insert(*r);
}

void Table::remove_source(Str first, Str last, SinkRange* sink, Str context) {
    for (auto it = source_ranges_.begin_overlaps(first, last);
	 it != source_ranges_.end(); ) {
        SourceRange* source = it.operator->();
        ++it;
	if (source->join() == sink->join())
            source->remove_sink(sink, context);
    }
}

void Table::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    FileErrorHandler xerrh(stderr);
    errh = errh ? errh : &xerrh;

    // check for redundant join
    for (auto it = join_ranges_.begin_overlaps(first, last);
         it != join_ranges_.end(); ++it)
        if (it->join()->same_structure(*join)) {
            errh->error("join on [%p{Str}, %p{Str}) has same structure as overlapping join\n(new join ignored)", &first, &last);
            return;
        }

    join_ranges_.insert(*new JoinRange(first, last, join));
    if (join->maintained() || join->staleness())
        all_pull_ = false;
    ++njoins_;
}

void Server::add_join(Str first, Str last, Join* join, ErrorHandler* errh) {
    join->attach(*this);
    Str tname = table_name(first, last);
    assert(tname);
    make_table(tname).add_join(first, last, join, errh);
}

void Table::insert(Str key, String value) {
    assert(namelen_);
    store_type::insert_commit_data cd;
    auto p = store_.insert_check(key, DatumCompare(), cd);
    Datum* d;
    if (p.second) {
	d = new Datum(key, value);
        value = String();
	store_.insert_commit(*d, cd);
    } else {
	d = p.first.operator->();
        d->value().swap(value);
    }
    notify(d, value, p.second ? SourceRange::notify_insert : SourceRange::notify_update);
    ++ninsert_;
    all_pull_ = false;
}

auto Table::validate(Str first, Str last, uint64_t now) -> iterator {
    if (njoins_ != 0) {
        if (njoins_ == 1) {
            auto it = store_.lower_bound(first, DatumCompare());
            if (it != store_.end() && it->key() < last && it->owner()
                && it->owner()->valid() && !it->owner()->has_expired(now)
                && it->owner()->ibegin() <= first && last <= it->owner()->iend()
                && !it->owner()->need_update())
                return it;
        }
        for (auto it = join_ranges_.begin_overlaps(first, last);
             it != join_ranges_.end(); ++it)
            it->validate(first, last, *server_, now);
    }
    return store_.lower_bound(first, DatumCompare());
}

void Table::hard_flush_for_pull(uint64_t now) {
    while (Datum* d = store_.unlink_leftmost_without_rebalance()) {
        invalidate_dependents(d->key());
        d->invalidate();
    }
    flush_at_ = now;
}

void Table::erase(Str key) {
    auto it = store_.find(key, DatumCompare());
    if (it != store_.end())
        erase(it);
    ++nerase_;
}

size_t Server::validate_count(Str first, Str last) {
    Table& t = make_table(table_name(first));
    auto it = validate(first, last);
    auto itend = t.end();
    size_t n = 0;
    for (; it != itend && it->key() < last; ++it)
        ++n;
    return n;
}

Json Server::stats() const {
    size_t store_size = 0, source_ranges_size = 0, join_ranges_size = 0,
        sink_ranges_size = 0;
    struct rusage ru;
    getrusage(RUSAGE_SELF, &ru);

    Json tables = Json::make_array();
    for (auto& t : tables_by_name_) {
        size_t source_size = t.source_ranges_.size();
        size_t join_size = t.join_ranges_.size();
        size_t sink_size = 0;
        for (auto& jr : t.join_ranges_)
            sink_size += jr.valid_ranges_size();

        Json pt = Json().set("name", t.name());
        if (t.ninsert_)
            pt.set("ninsert", t.ninsert_);
        if (t.nmodify_)
            pt.set("nmodify", t.nmodify_);
        if (t.nerase_)
            pt.set("nerase", t.nerase_);
        if (t.store_.size())
            pt.set("store_size", t.store_.size());
        if (source_size)
            pt.set("source_ranges_size", source_size);
        if (sink_size)
            pt.set("sink_ranges_size", sink_size);
        if (t.nvalidate_)
            pt.set("nvalidate", t.nvalidate_);
        if (t.nvalidate_optimized_)
            pt.set("nvalidate_optimized", t.nvalidate_optimized_);
        if (t.nvalidate_increasing_)
            pt.set("nvalidate_increasing", t.nvalidate_increasing_);
        if (t.nvalidate_skipped_)
            pt.set("nvalidate_skipped", t.nvalidate_skipped_);
        tables.push_back(pt);

        store_size += t.store_.size();
        source_ranges_size += source_size;
        join_ranges_size += join_size;
        sink_ranges_size += sink_size;
    }

    Json answer;
    answer.set("store_size", store_size)
	.set("source_ranges_size", source_ranges_size)
	.set("join_ranges_size", join_ranges_size)
	.set("valid_ranges_size", sink_ranges_size)
        .set("server_user_time", to_real(ru.ru_utime))
        .set("server_system_time", to_real(ru.ru_stime))
        .set("server_max_rss_mb", ru.ru_maxrss / 1024);
    if (SourceRange::allocated_key_bytes)
        answer.set("source_allocated_key_bytes", SourceRange::allocated_key_bytes);
    if (ServerRangeBase::allocated_key_bytes)
        answer.set("sink_allocated_key_bytes", ServerRangeBase::allocated_key_bytes);
    if (SinkRange::invalidate_hit_keys)
        answer.set("invalidate_hits", SinkRange::invalidate_hit_keys);
    if (SinkRange::invalidate_miss_keys)
        answer.set("invalidate_misses", SinkRange::invalidate_miss_keys);
    return answer.set("tables", tables);
}

void Table::print_sources(std::ostream& stream) const {
    stream << source_ranges_;
}

void Server::print(std::ostream& stream) {
    stream << "sources:" << std::endl;
    bool any = false;
    for (auto& t : tables_by_name_)
        if (!t.source_ranges_.empty()) {
            stream << t.source_ranges_;
            any = true;
        }
    if (!any)
        stream << "<empty>\n";
}

} // namespace
