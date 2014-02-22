#include <boost/random/random_number_generator.hpp>
#include <sys/resource.h>
#include <unistd.h>
#include <set>
#include <vector>
#if DO_PERF
#include <sys/prctl.h>
#include <sys/wait.h>
#endif
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "time.hh"
#include "check.hh"
#include "partitioner.hh"

namespace  {

typedef void (*test_func)();

void test_simple() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
	{"f|00001|00002", "1"},
	{"f|00001|10000", "1"},
	{"p|00002|0000000000", "Should not appear"},
	{"p|00002|0000000001", "Hello,"},
	{"p|00002|0000000022", "Which is awesome"},
	{"p|10000|0000000010", "My name is"},
	{"p|10000|0000000018", "Jennifer Jones"},
        {"p|10001|0000000011", "Not whatever the next thing claims"},
        {"p|10001|0000000019", ", Idiot,"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    pq::Join j;
    j.assign_parse("t|<subscriber:5>|<time:10>|<poster:5> = "
		   "using f|<subscriber>|<poster> "
		   "copy p|<poster>|<time>");
    j.ref();
    server.add_join("t|", "t}", &j);

    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001|0000000001", "t|00001}"), size_t(4));

    server.insert("p|10000|0000000022", "This should appear in t|00001");
    server.insert("p|00002|0000000023", "As should this");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));

    server.insert("f|00001|10001", "1");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));

    server.erase("f|00001|10001");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));
    server.insert("p|10001|0000000050", "Should be removed");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(9));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.insert("p|10001|0000000051", "Should not be inserted");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.insert("p|00002|0000000052", "Should be inserted");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(7));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(7));
}

void test_overlap() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
	{"s|00001|00002", "1"},
	{"s|00001|10000", "1"},
        {"s|00003|00002", "1"},
        {"s|00003|00004", "1"},
	{"p|00002|0000000000", "Should not appear"},
	{"p|00002|0000000001", "Hello,"},
	{"p|00002|0000000022", "Which is awesome"},
	{"p|10000|0000000010", "My name is"},
	{"p|10000|0000000018", "Jennifer Jones"},
        {"p|10001|0000000011", "Not whatever the next thing claims"},
        {"p|10001|0000000019", ", Idiot,"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    pq::Join j;
    j.assign_parse("t|<subscriber:5>|<time:10>|<poster:5> = "
		   "using s|<subscriber>|<poster> "
		   "copy p|<poster>|<time>");
    j.ref();
    server.add_join("t|", "t}", &j);

    server.validate("t|00001|0000000001", "t|00003}");
    CHECK_EQ(server.count("t|00001|0000000001", "t|00003}"), size_t(7));
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(4));
    CHECK_EQ(server.count("t|00003", "t|00003}"), size_t(3));

    server.insert("p|10000|0000000022", "This should appear in t|00001");
    server.insert("p|00002|0000000023", "As should this");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    CHECK_EQ(server.count("t|00003", "t|00003}"), size_t(4));

    server.insert("p|00004|0000000000", "This should appear in t|00003");
    CHECK_EQ(server.count("t|00003", "t|00003}"), size_t(5));

    server.insert("s|00001|10001", "1");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));

    server.erase("s|00001|10001");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));
    server.insert("p|10001|0000000050", "Should be removed");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(9));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.insert("p|10001|0000000051", "Should not be inserted");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.insert("p|00002|0000000052", "Should be inserted");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(7));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(7));
}

void test_celebrity() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"celeb|10001", "1"},
	{"s|00001|00002", "1"},
	{"s|00001|10000", "1"},
	{"p|00002|0000000000", "Should not appear"},
	{"p|00002|0000000001", "Hello,"},
	{"p|00002|0000000022", "Which is awesome"},
	{"p|10000|0000000010", "My name is"},
	{"p|10000|0000000018", "Jennifer Jones"},
        {"cp|10001|0000000011", "Not whatever the next thing claims"},
        {"cp|10001|0000000019", ", Idiot,"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    pq::Join join[4];
    join[0].assign_parse("t|<subscriber:5>|<time:10>|<poster:5> = "
                         "using s|<subscriber>|<poster> "
                         "copy p|<poster>|<time>");
    server.add_join("t|", "t}", &join[0]);
    join[1].assign_parse("t|<u>|<t>|<p> = using filter cs|<u>|<p> pull copy ct|<t>|<p> where u:5, p:5, t:10");
    server.add_join("t|", "t}", &join[1]);
    join[2].assign_parse("ct|<t>|<p> = copy cp|<p>|<t> where p:5, t:10");
    server.add_join("ct|", "ct}", &join[2]);
    join[3].assign_parse("cs|<u>|<p> = using filter celeb|<p> copy s|<u>|<p> where u:5, p:5");
    server.add_join("cs|", "cs}", &join[3]);

    server.validate("t|00001|0000000001", "t|00001}");
    //for (auto it = server.begin(); it != server.end(); ++it)
    //    std::cerr << "  " << *it << "\n";
    CHECK_EQ(server.count("t|00001|0000000001", "t|00001}"), size_t(4));

    server.insert("p|10000|0000000022", "This should appear in t|00001");
    server.insert("p|00002|0000000023", "As should this");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));

    server.insert("s|00001|10001", "1");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));

    server.erase("s|00001|10001");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));
    server.insert("cp|10001|0000000050", "Should be removed");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(8));
    server.validate("t|00001|0000000001", "t|00001}");
    //for (auto it = server.begin(); it != server.end(); ++it)
    //    std::cerr << "  " << *it << "\n";
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.insert("cp|10001|0000000051", "Should not be inserted");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(6));
    server.insert("p|00002|0000000052", "Should be inserted");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(7));
    server.validate("t|00001|0000000001", "t|00001}");
    CHECK_EQ(server.count("t|00001", "t|00001}"), size_t(7));
}

void test_expansion() {
    using pq::RangeMatch;
    pq::Join j;
    j.assign_parse("t|<subscriber:5>|<time:10>|<poster:5> = "
		   "using f|<subscriber>|<poster> "
		   "copy p|<poster>|<time>");
    j.ref();

    pq::Match m;
    j.source(0).match("f|11111|22222", m);
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|11111|9999999999",
                                       "t|11111}",
                                       m)), "t|11111|9999999999|22222");
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|00000|9999999999",
                                       "t|99999}",
                                       m)), "t|11111|");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|11111|9999999999",
                                      "t|11111}",
                                      m)), "t|11111}");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|00000|9999999999",
                                      "t|99999}",
                                      m)), "t|11111}");

    j.clear();
    j.assign_parse("t|<subscriber:5><time:10><poster:5> = "
		   "using s|<subscriber>|<poster> "
		   "copy p|<poster>|<time>");

    j.source(0).match("f|1111122222", m);
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|111119999999999",
                                       "t|11112",
                                       m)), "t|11111999999999922222");
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|000009999999999",
                                       "t|99999",
                                       m)), "t|11111");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|111119999999999",
                                      "t|11112",
                                      m)), "t|11112");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|000009999999999",
                                      "t|99999",
                                      m)), "t|11112");

    m.clear();
    m.set_slot(j.slot("subscriber"), "11111", 5);
    m.set_slot(j.slot("time"), "2222222222", 10);
    m.set_slot(j.slot("poster"), "fffff", 5);
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|111119999999999",
                                       "t|11112",
                                       m)), "t|111119999999999fffff");
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|000009999999999",
                                       "t|99999",
                                       m)), "t|111112222222222fffff");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|111119999999999",
                                      "t|11112",
                                      m)), "t|111112222222222ffffg");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|000009999999999",
                                      "t|99999",
                                      m)), "t|111112222222222ffffg");
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|000009999999999",
                                       "t|11111",
                                       m)), "t|111112222222222fffff");

    m.clear();
    m.set_slot(j.slot("time"), "2222222222", 10);
    m.set_slot(j.slot("poster"), "fffff", 5);
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|111119999999999",
                                       "t|11112",
                                       m)), "t|111119999999999fffff");
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|000009999999999",
                                       "t|99999",
                                       m)), "t|000009999999999fffff");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|111119999999999",
                                      "t|11112",
                                      m)), "t|111112222222222ffffg");
    CHECK_EQ(j.expand_last(j.sink(),
                           RangeMatch("t|000009999999999",
                                      "t|99999",
                                      m)), "t|999992222222222ffffg");
    CHECK_EQ(j.expand_first(j.sink(),
                            RangeMatch("t|000009999999999",
                                       "t|11111",
                                       m)), "t|000009999999999fffff");

    m.clear();
    CHECK_EQ(j.expand_first(j.source(0),
                            RangeMatch("t|11111",
                                       "t|11112",
                                       m)), "s|11111|");
    CHECK_EQ(j.expand_last(j.source(0),
                           RangeMatch("t|11111",
                                      "t|11112",
                                      m)), "s|11111}");

    RangeMatch rm("s|11111", "s|11112");
    j.source(0).match_range(rm);
    CHECK_EQ(rm.match.slot(j.slot("subscriber")), "11111");
}

void test_count() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"b|00002|0000000010", "b3"},
        {"b|00002|0000000020", "b4"},
        {"b|00003|0000000002", "b5"},
        {"d|00003|00001", "1"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> = "
                    "using a|<a_id>|<b_id> "
                    "copy b|<b_id>|<time>");
    j1.ref();
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("e|<d_id:5>|<time:10>|<b_id:5> = "
                    "using d|<d_id>|<a_id:5> "
                    "copy c|<a_id>|<time>|<b_id>");
    j2.ref();
    server.add_join("e|", "e}", &j2);

    server.validate("e|", "e}");
    CHECK_EQ(server.count("e|", "e}"), size_t(4), "Count of recursive expansion failed.");
    server.validate("b|", "b}");
    CHECK_EQ(server.count("b|", "b}"), size_t(5));
    server.validate("b|00002|", "b|00002}");
    CHECK_EQ(server.count("b|00002|", "b|00002}"), size_t(4));
    server.validate("b|00002|0000000002", "b|00002|0000000015");
    CHECK_EQ(server.count("b|00002|0000000002", "b|00002|0000000015"), size_t(2), "\nWrong subrange count.");
    server.validate("c|", "c}");
    CHECK_EQ(server.count("c|", "c}"), size_t(4));
    server.validate("j|", "j}");
    CHECK_EQ(server.count("j|", "j}"), size_t(0));
}

void test_recursive() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"d|00003|00001", "1"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time>|<b_id> = "
                    "copy b|<b_id:5>|<time:10d> "
                    "using a|<a_id:5>|<b_id> ");
    j1.ref();
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("e|<d_id:5>|<time:10>|<b_id:5> = "
                    "using d|<d_id>|<a_id:5> "
                    "copy c|<a_id>|<time>|<b_id>");
    j2.ref();
    server.add_join("e|", "e}", &j2);

    server.validate("e|00003|0000000001", "e|00003}");
    CHECK_EQ(server.count("e|00003|0000000001", "e|00003}"), size_t(2));
    CHECK_EQ(server.count("c|00001|0000000001", "c|00001}"), size_t(2));

    server.insert("b|00002|0000001000", "This should appear in c|00001 and e|00003");
    server.insert("b|00002|0000002000", "As should this");

    CHECK_EQ(server.count("e|00003|0000000001", "e|00003}"), size_t(4));
    CHECK_EQ(server.count("c|00001|0000000001", "c|00001}"), size_t(4));
}

void test_annotation() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"d|00003|00004", "1"},
        {"e|00004|0000000001", "e1"},
        {"e|00004|0000000002", "e2"},
        {"e|00004|0000000010", "e3"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    pq::Join j1;
    j1.assign_parse("c|<a_id>|<time>|<b_id> = "
                    "using a|<a_id>|<b_id> "
                    "copy b|<b_id>|<time> pull "
                    "where a_id:5d, time:10d, b_id:5d");
    j1.ref();
    // will validate join each time and not install autopush triggers
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("f|<d_id:5>|<time:10>|<e_id:5> = "
                    "using d|<d_id>|<e_id> "
                    "copy e|<e_id>|<time>");
    j2.ref();
    // will not re-validate join within T seconds of last validation
    // for the requested range. the store will hold stale results
    j2.set_staleness(0.1);
    server.add_join("f|", "f}", &j2);

    // should NOT have a validrange for c|00001 or a copy for b|00002
    server.validate("c|00001|0000000001", "c|00001}");
    CHECK_EQ(server.count("c|00001|0000000001", "c|00001}"), size_t(2));

    // should NOT trigger the insertion of a new c|00001 key
    server.insert("b|00002|0000000005", "b3");
    CHECK_EQ(server.count("c|00001|0000000001", "c|00001}"), size_t(2));

    // SHOULD have a validrange for f|00003 (that expires) but NO copy of e|00004
    server.validate("f|00003|0000000001", "f|00003|0000000005");
    CHECK_EQ(server.count("f|00003|0000000001", "f|00003|0000000005"), size_t(2));

    // should NOT trigger the insertion of a new f|00003 key
    server.insert("e|00004|0000000003", "e4");
    CHECK_EQ(server.count("f|00003|0000000001", "f|00003|0000000005"), size_t(2));

    // should NOT re-validate the lower part of the range.
    // SHOULD have 2 validranges for f|00003 (that expire) but NO copies of e|00004
    server.validate("f|00003|0000000001", "f|00003|0000000015");
    CHECK_EQ(server.count("f|00003|0000000001", "f|00003|0000000015"), size_t(3));

    usleep(200000);

    // SHOULD re-validate the whole range
    // SHOULD have a validrange for f|00003 (that expires) but NO copy of e|00004
    // MIGHT have 2 expired validranges for f|00003 we implement cleanup of expired ranges
    server.validate("f|00003|0000000001", "f|00003|0000000015");
    CHECK_EQ(server.count("f|00003|0000000001", "f|00003|0000000015"), size_t(4));
}

void test_join1() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("c|<a_id:5>|<b_id:5>|<index:5> = "
                               "using a|<a_id>|<b_id> "
                               "copy b|<index>|<b_id>"));
    CHECK_EQ(j1.nsource(), 2);
    CHECK_EQ(j1.completion_source(), 1);

    j1.ref();
    server.add_join("c|", "c}", &j1);

    String begin("c|00000|");
    String end("c|10000}");
    server.insert("a|00000|B0000", "a: index-only");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(0));

    server.insert("b|I0000|B0000", "b: real value");
    CHECK_EQ(server.count(begin, end), size_t(1));
}

void test_op_count() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("\
k|<uid> = count v|<aid>|<voter> using a|<uid>|<aid> \
  where aid:5d, uid:5d, voter:5d"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    server.insert("a|00000|00000", "article 0");
    server.insert("a|00000|00001", "article 1");
    server.insert("a|00001|00003", "article 2");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(0));

    server.insert("v|00000|00000", "vote 0");
    CHECK_EQ(server.count(begin, end), size_t(1));
    auto k0 = server.find("k|00000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value(), "1");

    server.insert("v|00001|00000", "vote 0");
    server.insert("v|00003|00000", "vote 0");
    CHECK_EQ(server.count(begin, end), size_t(2));
    auto k1 = server.find("k|00001");
    mandatory_assert(k1);
    CHECK_EQ(k0->value(), "2");
    CHECK_EQ(k1->value(), "1");
}

// One Server::validate produces multiple grouped keys
void test_op_count_validate1() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> = "
                               "using a|<uid>|<aid:5> "
                               "count v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    server.insert("a|00000|00000", "article 0");
    server.insert("a|00000|00001", "article 1");
    server.insert("a|00001|00002", "article 2");
    server.insert("v|00000|00000", "vote 0");
    server.insert("v|00001|00000", "vote 0");
    server.insert("v|00002|00000", "vote 0");

    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(2));
    auto k0 = server.find("k|00000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value(), "2");
    auto k1 = server.find("k|00001");
    mandatory_assert(k1);
    CHECK_EQ(k1->value(), "1");
}

void test_karma() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> = "
                               "using a|<uid>|<aid:5> "
                               "count v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    int nuser = 10000;
    int nvotes_per_aid = 1000;
    String aid;
    char buf[20];
    for (int i = 0; i < nuser; ++i) {
        sprintf(buf, "a|%05d|%05d", i, i);
        server.insert(String(buf), "article");
        for (int j = 0; j < nvotes_per_aid; ++j) {
            sprintf(buf, "v|%05d|%05d", i, j + 1);
            server.insert(String(buf), "vote");
        }
    }
    struct rusage ru[2];
    getrusage(RUSAGE_SELF, &ru[0]);
    server.validate(begin, end);
    getrusage(RUSAGE_SELF, &ru[1]);

    CHECK_EQ(server.count(begin, end), size_t(nuser));
    for (int i = 0; i < nuser; ++i) {
        sprintf(buf, "k|%05d", i);
        auto k0 = server.find(String(buf));
        mandatory_assert(k0);
        CHECK_EQ(k0->value(), String(nvotes_per_aid));
    }
    Json stats = Json().set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
}

void test_ma() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<author:5> = "
                               "using a|<author:5><seqid:5> "
                               "count v|<author><seqid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    String mab("ma|");
    String mae("ma}");

    pq::Join j2;
    CHECK_TRUE(j2.assign_parse("ma|<author:5><seqid:5>|k = "
                               "using a|<author><seqid> "
                               "copy k|<author>"));
    CHECK_EQ(j2.nsource(), 2);

    j2.ref();
    server.add_join("ma|", "ma}", &j2);

    int nuser = 10;
    int nvotes_per_aid = 4;
    String aid;
    char buf[20];

    for (int i = 0; i < nuser; ++i) {
        sprintf(buf, "a|%05d%05d", i, i);
        server.insert(String(buf), "article");
        for (int j = 0; j < nvotes_per_aid; ++j) {
            sprintf(buf, "v|%05d%05d|%05d", i, i, j + 1);
            server.insert(String(buf), "1");
        }
    }

    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(nuser));
    server.validate(mab, mae);
    // FAILS
    CHECK_EQ(server.count(mab, mae), size_t(nuser));

    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value() << "\n";
}

void test_karma_online() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> = "
                               "using a|<uid>|<aid:5> "
                               "count v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    int nuser = 10000;
    int nvotes_per_aid = 200;
    String aid;
    char buf[20];
    for (int i = 0; i < nuser; ++i) {
        sprintf(buf, "a|%05d|%05d", i, i);
        server.insert(String(buf), "article");
        for (int j = 0; j < 1; ++j) {
            sprintf(buf, "v|%05d|%05d", i, j + 1);
            server.insert(String(buf), "vote");
        }
    }
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(nuser));
    for (int i = 0; i < nuser; ++i) {
        sprintf(buf, "k|%05d", i);
        auto k0 = server.find(String(buf));
        mandatory_assert(k0);
        CHECK_EQ(k0->value(), String(1));
    }
#if DO_PERF
    // perf profiling
    {
        String me(getpid());
        pid_t pid = fork();
        if (!pid) {
            prctl(PR_SET_PDEATHSIG, SIGINT);
            execlp("perf", "perf", "record", "-g", "-p", me.c_str(), NULL);
            exit(0);
        }
    }
#endif
    // online votes
    struct rusage ru[2];
    getrusage(RUSAGE_SELF, &ru[0]);
    for (int i = 0; i < nuser; ++i) {
        for (int j = 1; j < nvotes_per_aid; ++j) {
            sprintf(buf, "v|%05d|%05d", i, j + 1);
            server.insert(String(buf), "vote");
        }
    }
    getrusage(RUSAGE_SELF, &ru[1]);
    CHECK_EQ(server.count(begin, end), size_t(nuser));
    for (int i = 0; i < nuser; ++i) {
        sprintf(buf, "k|%05d", i);
        auto k0 = server.find(String(buf));
        mandatory_assert(k0);
        CHECK_EQ(k0->value(), String(nvotes_per_aid));
    }
    Json stats = Json().set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
}

void test_op_min() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> = "
                               "using a|<uid>|<aid:5> "
                               "min v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    server.insert("a|00000|00000", "article 0");
    server.insert("a|00000|00001", "article 1");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(0));

    server.insert("v|00000|00009", "v9");
    server.insert("v|00000|00008", "v8");
    CHECK_EQ(server.count(begin, end), size_t(1));
    auto k0 = server.find("k|00000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value(), "v8");

    server.insert("v|00000|00005", "v5");
    CHECK_EQ(k0->value(), "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.insert("v|00000|00006", "v6");
    CHECK_EQ(k0->value(), "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));
}

void test_op_max() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> = "
                               "using a|<uid>|<aid:5> "
                               "max v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    server.insert("a|00000|00000", "article 0");
    server.insert("a|00000|00001", "article 1");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(0));

    server.insert("v|00000|00001", "v1");
    server.insert("v|00000|00003", "v2");
    CHECK_EQ(server.count(begin, end), size_t(1));
    auto k0 = server.find("k|00000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value(), "v2");

    server.insert("v|00000|00002", "v5");
    CHECK_EQ(k0->value(), "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.insert("v|00000|00004", "v4");
    CHECK_EQ(k0->value(), "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.insert("v|00001|00005", "v6");
    CHECK_EQ(k0->value(), "v6");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.erase("v|00001|00004");
    CHECK_EQ(k0->value(), "v6");

    server.erase("v|00001|00005");
    CHECK_EQ(k0->value(), pq::invalidate_marker());
    server.validate(begin, end);
    k0 = server.find("k|00000");
    CHECK_EQ(!k0, 0);
    CHECK_EQ(k0->value(), "v5");
}

void test_op_sum() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("sum|<sid:5> = "
                               "using a|<sid>|<aid:5> "
                               "sum b|<aid>|<bid:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.ref();
    server.add_join("sum|", "sum}", &j1);

    String begin("sum|");
    String end("sum}");
    server.insert("a|00000|00000", "");
    server.insert("a|00000|00001", "");
    server.insert("a|00001|00003", "");
    server.insert("b|00000|00000", "10");
    CHECK_EQ(server.count(begin, end), size_t(0));

    server.validate(begin, end);                        // sum|00000 = 10
    CHECK_EQ(server.count(begin, end), size_t(1));
    auto sum0 = server.find("sum|00000");
    CHECK_TRUE(sum0);
    CHECK_EQ(sum0->value(), "10");

    server.insert("b|00000|00000", "5");                // sum|00000 = 5
    server.insert("b|00001|00000", "3");                // sum|00000 = 8
    server.insert("b|00000|00001", "77");               // sum|00000 = 85
    server.insert("b|00003|00000", "8");                // sum|00001 = 8
    server.erase("b|00000|00000");                      // sum|00000 = 80
    CHECK_EQ(server.count(begin, end), size_t(2));
    auto sum1 = server.find("sum|00001");
    CHECK_TRUE(sum1);
    auto sum3 = server.find("sum|00003");
    CHECK_TRUE(!sum3);
    CHECK_EQ(sum0->value(), "80");
    CHECK_EQ(sum1->value(), "8");
}

void test_partitioner_analyze() {
    pq::Partitioner* part = pq::Partitioner::make("twitternew-text", 6, -1);
    std::vector<pq::keyrange> parts;

    part->analyze("t|", "t}", 0, parts);
    CHECK_EQ(parts.size(), (uint32_t)100000);

    parts.clear();
    part->analyze("t|00000000|", "t|00000000}", 0, parts);
    CHECK_EQ(parts.size(), (uint32_t)1);
    CHECK_EQ(parts.begin()->key, "t|00000000|");

    parts.clear();
    part->analyze("t|00000000|00000003", "t|00000000}", 0, parts);
    CHECK_EQ(parts.size(), (uint32_t)1);
    CHECK_EQ(parts.begin()->key, "t|00000000|00000003");
}

void test_cross() {
    pq::Server server;
    pq::Join j1, j2;

    CHECK_TRUE(j1.assign_parse(
        "t|<user>|<time>|<poster> = "
        "copy p|<poster>|<time> "
        "using s|<user>|<poster> "
        "where user:5t, time:10, poster:5t"));
    j1.ref();
    server.add_join("t|", "t}", &j1);

    CHECK_TRUE(j2.assign_parse(
        "u|<user>|<time>|<poster> = "
        "copy p|<poster>|<time> "
        "using s|<user>|<poster> "
        "where user:5, time:10, poster:5"));
    j2.ref();
    server.add_join("u|", "u}", &j2);

    std::pair<const char*, const char*> values[] = {
        {"s|00001|00002", "1"},
        {"s|00001|10000", "1"},
        {"s|00002|10000", "1"},
        {"p|00002|0000000000", "Should not appear"},
        {"p|00002|0000000001", "Hello,"},
        {"p|00002|0000000022", "Which is awesome"},
        {"p|10000|0000000010", "My name is"},
        {"p|10000|0000000018", "Jennifer Jones"},
        {"p|10001|0000000011", "Not whatever the next thing claims"},
        {"p|10001|0000000019", ", Idiot,"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second);

    // crosses subtables
    server.validate("t|00001|0000000000", "t|00002}");
    CHECK_EQ(server.count("t|00001|0000000000", "t|00002}"), size_t(7));

    // crosses top-level tables
    // todo: not supported right now...
    //server.validate("t|00001|0000000000", "u|00002}");
    //CHECK_EQ(server.count("t|00001|0000000000", "u|00002}"), size_t(14));
}

#if 0
void test_op_bounds() {
    pq::Server server;

    // tests count bounds
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("o|<oid:5> "
                               "a|<oid>|<aid:5> "
                               "b|<aid>|<bid:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_bounded_count_match);
    j1.set_jvt_config(Json().set("lbound", 5).set("ubound", 10));
    j1.ref();

    server.add_join("o|", "o}", &j1);

    String begin("o|");
    String end("o}");
    server.insert("a|00000|00000", "");
    server.insert("b|00000|00000", "6");
    server.insert("b|00000|00001", "8");
    server.insert("b|00000|00002", "12");
    server.insert("b|00000|00003", "-1");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(1));

    auto dst = server.find("o|00000");
    mandatory_assert(dst);
    CHECK_EQ(dst->value(), "2");

    server.insert("b|00000|00004", "7");
    server.insert("b|00000|00005", "3");
    CHECK_EQ(dst->value(), "3");

    server.insert("b|00000|00004", "8");
    CHECK_EQ(dst->value(), "3");

    server.insert("b|00000|00004", "0");
    CHECK_EQ(dst->value(), "2");

    server.erase("b|00000|00003");
    CHECK_EQ(dst->value(), "2");

    server.erase("b|00000|00001");
    CHECK_EQ(dst->value(), "1");

    // test copy bounds
    pq::Join j2;
    CHECK_TRUE(j2.assign_parse("e|<eid:5>|<cid:5> "
                               "d|<eid>|<did:5> "
                               "c|<did>|<cid>"));
    CHECK_EQ(j2.nsource(), 2);

    j2.set_jvt(pq::jvt_bounded_copy_last);
    j2.set_jvt_config(Json().set("lbound", 5).set("ubound", 10));
    j2.ref();

    server.add_join("e|", "e}", &j2);

    begin = "e|";
    end = "e}";
    server.insert("d|00000|00000", "");
    server.insert("c|00000|00000", "6");
    server.insert("c|00000|00001", "12");
    server.insert("c|00000|00002", "9");
    server.insert("c|00000|00003", "-2");
    server.insert("c|00000|00004", "88");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(2));

    server.insert("c|00000|00003", "7");
    CHECK_EQ(server.count(begin, end), size_t(3));

    server.insert("c|00000|00000", "8");
    CHECK_EQ(server.count(begin, end), size_t(3));

    server.erase("c|00000|00004");
    CHECK_EQ(server.count(begin, end), size_t(3));

    server.erase("c|00000|00003");
    CHECK_EQ(server.count(begin, end), size_t(2));
}
#endif

void test_swap() {
    String s1("abcde");
    String s2("ghijk");
    const int64_t n = 80000000;
    struct rusage ru[2];
    getrusage(RUSAGE_SELF, &ru[0]);
    for (int64_t i = 0; i < n; ++i)
        s1.swap(s2);
    getrusage(RUSAGE_SELF, &ru[1]);
    Json stats = Json().set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";

    getrusage(RUSAGE_SELF, &ru[0]);
    for (int64_t i = 0; i < n; ++i)
        std::swap(s1, s2);
    getrusage(RUSAGE_SELF, &ru[1]);
    stats = Json().set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
}

void test_iupdate() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("\
k|<author> = count v|<chapter>|<voter>\
  using b|<author>|<book>, c|<book>|<chapter>\
  where author:5, chapter:5, book:5, voter:5"));
    j1.ref();
    server.add_join("k|", "k}", &j1);

    server.insert("b|u0000|bxxx1", "");
    server.insert("c|bxxx1|c0001", "");
    server.insert("v|c0001|u0001", "");
    server.validate("k|", "k}");
    CHECK_EQ(server.count("k|", "k}"), size_t(1));
    auto k0 = server.find("k|u0000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value(), "1");

    server.insert("c|bxxx1|c0002", "");
    server.insert("v|c0002|u0002", "");
    server.validate("k|", "k}");
    CHECK_EQ(server.count("k|", "k}"), size_t(1));
    CHECK_EQ(k0->value(), "2");
}

void test_iupdate_t() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("\
k|<author> = count v|<chapter>|<voter>\
  using b|<author>|<book>, c|<book>|<chapter>\
  where author:5, chapter:5t, book:5, voter:5"));
    j1.ref();
    server.add_join("k|", "k}", &j1);

    server.insert("b|u0000|bxxx1", "");
    server.insert("c|bxxx1|c0001", "");
    server.insert("v|c0001|u0001", "");
    server.validate("k|", "k}");
    CHECK_EQ(server.count("k|", "k}"), size_t(1));
    auto k0 = server.find("k|u0000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value(), "1");

    server.insert("c|bxxx1|c0002", "");
    server.insert("v|c0002|u0002", "");
    server.validate("k|", "k}");
    CHECK_EQ(server.count("k|", "k}"), size_t(1));
    CHECK_EQ(k0->value(), "2");
}

void test_iupdate2() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("\
k|<author> = count v|<chapter>|<voter>\
  using b|<author>|<book>, c|<book>|<chapter>\
  where author:1, chapter:2, book:1, voter:1"));
    j1.ref();
    server.add_join("k|", "k}", &j1);

    const char* const keys[] = {
        "b|a|i", "b|b|i", "b|a|j",
        "c|i|i1", "c|i|i2", "c|i|i3", "c|j|j1", "c|j|j2",
        "v|i1|x", "v|i1|y", "v|j2|x"
    };
    for (auto it : keys)
        server.insert(it, "");

    server.validate("k|a");
    CHECK_EQ(server["k|a"].value(), "3");

    server.erase("v|i1|y");
    CHECK_EQ(server["k|a"].value(), "2");
    server.insert("v|i1|y", "");
    CHECK_EQ(server["k|a"].value(), "3");
    server.insert("v|i1|y", "");
    CHECK_EQ(server["k|a"].value(), "3");

    server.validate("k|b");
    CHECK_EQ(server["k|b"].value(), "2");

    server.erase("b|a|i");
    server.validate("k|a");
    CHECK_EQ(server["k|a"].value(), "1");

    server.insert("b|a|i", "");
    server.validate("k|a");
    CHECK_EQ(server["k|a"].value(), "3");

    server.erase("b|a|i");
    server.insert("v|j2|m", "");
    server.validate("k|a");
    CHECK_EQ(server["k|a"].value(), "2");
    server.insert("v|j2|n", "");
    CHECK_EQ(server["k|a"].value(), "3");
}

void test_iupdate3() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("\
k|<author> = count v|<chapter>|<voter>\
  using b|<author>|<book>, c|<book>|<chapter>\
  where author:1, chapter:2, book:1, voter:1"));
    j1.ref();
    server.add_join("k|", "k}", &j1);

    const char* const keys[] = {
        "b|a|i", "b|b|i", "b|a|j",
        "c|i|i1", "c|i|i2", "c|i|i3", "c|j|j1", "c|j|j2",
        "v|i1|x", "v|i1|y", "v|j2|x"
    };
    for (auto it : keys)
        server.insert(it, "");

    server.validate("k|", "k}");
    CHECK_EQ(server["k|a"].value(), "3");

    server.erase("v|i1|y");
    CHECK_EQ(server["k|a"].value(), "2");
    server.insert("v|i1|y", "");
    CHECK_EQ(server["k|a"].value(), "3");
    server.insert("v|i1|y", "");
    CHECK_EQ(server["k|a"].value(), "3");

    server.validate("k|", "k}");
    CHECK_EQ(server["k|b"].value(), "2");

    server.erase("b|a|i");
    server.validate("k|", "k}");
    CHECK_EQ(server["k|a"].value(), "1");
    CHECK_EQ(server["k|b"].value(), "2");

    server.insert("v|i1|z", "");
    CHECK_EQ(server["k|b"].value(), "3");
}

void test_iupdate4() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("\
k|<author> = count v|<chapter>|<voter>\
  using b|<author>|<book>, c|<book>|<chapter>\
  where author:1, chapter:2, book:1, voter:1"));
    j1.ref();
    server.add_join("k|", "k}", &j1);

    pq::Join j2;
    CHECK_TRUE(j2.assign_parse("\
kk|<author> = copy k|<author> where author:1"));
    server.add_join("kk|", "kk}", &j2);

    pq::Join j3;
    CHECK_TRUE(j3.assign_parse("\
bb|<bid> = copy b|<bid> where bid:3"));
    server.add_join("bb|", "bb}", &j3);

    const char* const keys[] = {
        "b|a|i", "b|b|i", "b|a|j",
        "c|i|i1", "c|i|i2", "c|i|i3", "c|j|j1", "c|j|j2",
        "v|i1|x", "v|i1|y", "v|j2|x"
    };
    for (auto it : keys)
        server.insert(it, "");

    server.validate("k|", "k}");
    CHECK_EQ(server["k|a"].value(), "3");

    server.erase("v|i1|y");
    CHECK_EQ(server["k|a"].value(), "2");
    server.insert("v|i1|y", "");
    CHECK_EQ(server["k|a"].value(), "3");
    server.insert("v|i1|y", "");
    CHECK_EQ(server["k|a"].value(), "3");

    server.validate("k|", "k}");
    CHECK_EQ(server["k|b"].value(), "2");

#if 0
    for (auto it = server.table("bb").begin(); it != server.table("bb").end(); ++it)
        std::cerr << *it << "\n";
#endif
    server.validate("bb|", "bb}");
#if 0
    for (auto it = server.table("bb").begin(); it != server.table("bb").end(); ++it)
        std::cerr << *it << "\n";
#endif

    server.validate("kk|a");
#if 0
    for (auto it = server.table("kk").begin(); it != server.table("kk").end(); ++it)
        std::cerr << *it << "\n";
#endif
    CHECK_EQ(server["kk|a"].value(), "3");
    server.validate("kk|b");
    CHECK_EQ(server["kk|b"].value(), "2");

    server.erase("b|a|i");
    server.validate("k|", "k}");
    CHECK_EQ(server["k|a"].value(), "1");
    CHECK_EQ(server["k|b"].value(), "2");

    CHECK_EQ(server["kk|a"].value(), "");
    CHECK_EQ(server["kk|b"].value(), "2");
    server.validate("kk|a");
    CHECK_EQ(server["kk|a"].value(), "1");
    CHECK_EQ(server["kk|b"].value(), "2");

    server.insert("v|i1|z", "");
    CHECK_EQ(server["k|b"].value(), "3");
    CHECK_EQ(server["kk|b"].value(), "3");
}

} // namespace

void test_string() {
    // click_strcmp
    CHECK_TRUE(String::natural_compare("a", "b") < 0);
    CHECK_TRUE(String::natural_compare("a9", "a10") < 0);
    CHECK_TRUE(String::natural_compare("a001", "a2") < 0);   // 1 < 2
    CHECK_TRUE(String::natural_compare("a001", "a1") > 0);   // longer string of initial zeros
    CHECK_TRUE(String::natural_compare("a", "B") < 0);
    CHECK_TRUE(String::natural_compare("Baa", "baa") < 0);
    CHECK_TRUE(String::natural_compare("Baa", "caa") < 0);
    CHECK_TRUE(String::natural_compare("baa", "Caa") < 0);
    CHECK_TRUE(String::natural_compare("baa", "Baa") > 0);
    CHECK_TRUE(String::natural_compare("baA", "baA") == 0);
    CHECK_TRUE(String::natural_compare("a9x", "a10") < 0);
    CHECK_TRUE(String::natural_compare("a9x", "a9xy") < 0);
    CHECK_TRUE(String::natural_compare("0", "0.1") < 0);
    CHECK_TRUE(String::natural_compare("0", "0.") < 0);
    CHECK_TRUE(String::natural_compare("0", "-0.1") > 0);
    CHECK_TRUE(String::natural_compare("-0", "-0.1") > 0);
    CHECK_TRUE(String::natural_compare("-9", "-0.1") < 0);
    CHECK_TRUE(String::natural_compare("9", "0.1") > 0);
    CHECK_TRUE(String::natural_compare("-0.2", "-0.1") < 0);
    CHECK_TRUE(String::natural_compare("-2.2", "-2") < 0);
    CHECK_TRUE(String::natural_compare("2.2", "2") > 0);
    CHECK_TRUE(String::natural_compare("0.2", "0.1") > 0);
    CHECK_TRUE(String::natural_compare("0.2", "0.39") < 0);
    CHECK_TRUE(String::natural_compare(".2", "0.2") < 0);
    CHECK_TRUE(String::natural_compare("a-2", "a-23") < 0);
    CHECK_TRUE(String::natural_compare("a-2", "a-3") < 0);
    CHECK_TRUE(String::natural_compare("a1.2", "a1a") > 0);
    CHECK_TRUE(String::natural_compare("1.2.3.4", "10.2.3.4") < 0);
    CHECK_TRUE(String::natural_compare("1.12", "1.2") < 0);
    CHECK_TRUE(String::natural_compare("1.012", "1.2") < 0);
    CHECK_TRUE(String::natural_compare("1.012.", "1.2.") < 0);
    CHECK_TRUE(String::natural_compare("1.12.3.4", "1.2.3.4") > 0);
    CHECK_TRUE(String::natural_compare("1.2.10.4", "1.2.9.4") > 0);
    CHECK_TRUE(String::natural_compare("1.2.10.4:100", "1.2.10.4:2") > 0);
}

extern void test_mpfd();
extern void test_mpfd2();
extern void test_redis();
extern void test_memcache();
extern void test_postgres();

void unit_tests(const std::set<String> &testcases) {
    std::vector<std::pair<String, test_func> > tests_;
    std::vector<std::pair<String, test_func> > exptests_;
    std::vector<std::pair<String, test_func> > othertests_;
#define ADD_TEST(test) tests_.push_back(std::pair<String, test_func>(#test, test))
#define ADD_EXP_TEST(test) exptests_.push_back(std::pair<String, test_func>(#test, test))
#define ADD_OTHER_TEST(test) othertests_.push_back(std::pair<String, test_func>(#test, test))
    ADD_TEST(test_simple);
    ADD_TEST(test_overlap);
    ADD_TEST(test_expansion);
    ADD_TEST(test_recursive);
    ADD_TEST(test_count);
    ADD_TEST(test_annotation);
    ADD_TEST(test_join1);
    ADD_TEST(test_op_count);
    ADD_TEST(test_op_count_validate1);
    ADD_TEST(test_op_min);
    ADD_TEST(test_op_max);
    ADD_TEST(test_op_sum);
    //ADD_TEST(test_op_bounds);
    ADD_TEST(test_partitioner_analyze);
    ADD_TEST(test_cross);
    ADD_TEST(test_iupdate);
    ADD_TEST(test_iupdate2);
    ADD_TEST(test_iupdate3);
    ADD_TEST(test_iupdate4);
    ADD_TEST(test_iupdate_t);
    ADD_TEST(test_celebrity);
    ADD_TEST(test_string);
    ADD_EXP_TEST(test_karma);
    ADD_EXP_TEST(test_ma);
    ADD_EXP_TEST(test_swap);
    ADD_EXP_TEST(test_karma_online);
    ADD_OTHER_TEST(test_mpfd);
    ADD_OTHER_TEST(test_mpfd2);
    ADD_OTHER_TEST(test_redis);
    ADD_OTHER_TEST(test_memcache);
    ADD_OTHER_TEST(test_postgres);
    size_t ntests = 0;
    for (auto& t : tests_)
        if (testcases.empty() || testcases.find(t.first) != testcases.end()) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
            ++ntests;
        }
    for (auto& t : exptests_)
        if (testcases.find(t.first) != testcases.end()) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
            ++ntests;
        }
    if (ntests)
        std::cerr << "PASS" << std::endl;
    for (auto& t : othertests_)
        if (testcases.find(t.first) != testcases.end()) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
        }
}
