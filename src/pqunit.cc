#include <boost/random/random_number_generator.hpp>
#include <sys/resource.h>
#include <unistd.h>
#include <set>
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "time.hh"
#include "check.hh"

namespace  {

typedef void (*test_func)();
std::vector<std::pair<String, test_func> > tests_;

void simple() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
	{"f|00001|00002", "1"},
	{"f|00001|10000", "1"},
	{"p|00002|0000000000", "Should not appear"},
	{"p|00002|0000000001", "Hello,"},
	{"p|00002|0000000022", "Which is awesome"},
	{"p|10000|0000000010", "My name is"},
	{"p|10000|0000000018", "Jennifer Jones"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second, true);

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    pq::Join j;
    j.assign_parse("t|<user_id:5>|<time:10>|<poster_id:5> "
		   "f|<user_id>|<poster_id> "
		   "p|<poster_id>|<time>");
    j.ref();
    server.add_join("t|", "t}", &j);

    server.validate("t|00001|0000000001", "t|00001}");

    std::cerr << "After processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    server.insert("p|10000|0000000022", "This should appear in t|00001", true);
    server.insert("p|00002|0000000023", "As should this", true);

    std::cerr << "After processing add_copy:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
	std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    server.print(std::cerr);
    std::cerr << std::endl;
}

void count() {
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
        server.insert(it->first, it->second, true);
    std::cerr << std::endl;

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> "
                    "a|<a_id>|<b_id> "
                    "b|<b_id>|<time>");
    j1.ref();
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("e|<d_id:5>|<time:10>|<b_id:5> "
                    "d|<d_id>|<a_id:5> "
                    "c|<a_id>|<time>|<b_id>");
    j2.ref();
    server.add_join("e|", "e}", &j2);

    CHECK_EQ(server.validate_count("e|", "e}"), 4, "Count of recursive expansion failed.");
    CHECK_EQ(server.validate_count("b|", "b}"), 5);
    CHECK_EQ(server.validate_count("b|00002|", "b|00002}"), 4);
    CHECK_EQ(server.validate_count("b|00002|0000000002", "b|00002|0000000015"), 2, "Wrong subrange count.");
    CHECK_EQ(server.validate_count("c|", "c}"), 4);
    CHECK_EQ(server.validate_count("j|", "j}"), 0);
}

void recursive() {
    pq::Server server;

    std::pair<const char*, const char*> values[] = {
        {"a|00001|00002", "1"},
        {"b|00002|0000000001", "b1"},
        {"b|00002|0000000002", "b2"},
        {"d|00003|00001", "1"}
    };
    for (auto it = values; it != values + sizeof(values)/sizeof(values[0]); ++it)
        server.insert(it->first, it->second, true);

    std::cerr << "Before processing join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> "
                    "a|<a_id>|<b_id> "
                    "b|<b_id>|<time>");
    j1.ref();
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("e|<d_id:5>|<time:10>|<b_id:5> "
                    "d|<d_id>|<a_id:5> "
                    "c|<a_id>|<time>|<b_id>");
    j2.ref();
    server.add_join("e|", "e}", &j2);

    server.validate("e|00003|0000000001", "e|00003}");

    std::cerr << "After processing recursive join:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    server.insert("b|00002|0000001000", "This should appear in c|00001 and e|00003", true);
    server.insert("b|00002|0000002000", "As should this", true);

    std::cerr << "After processing inserts:\n";
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;
}

void annotation() {
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
        server.insert(it->first, it->second, true);

    pq::Join j1;
    j1.assign_parse("c|<a_id:5>|<time:10>|<b_id:5> "
                    "a|<a_id>|<b_id> "
                    "b|<b_id>|<time>");
    j1.ref();
    // will validate join each time and not install autopush triggers
    j1.set_maintained(false);
    server.add_join("c|", "c}", &j1);

    pq::Join j2;
    j2.assign_parse("f|<d_id:5>|<time:10>|<e_id:5> "
                    "d|<d_id>|<e_id> "
                    "e|<e_id>|<time>");
    j2.ref();
    // will not re-validate join within T seconds of last validation
    // for the requested range. the store will hold stale results
    j2.set_staleness(0.5);
    server.add_join("f|", "f}", &j2);

    server.validate("c|00001|0000000001", "c|00001}");

    // should NOT have a validrange for c|00001 or a copy for b|00002
    std::cerr << "After validating [c|00001|0000000001, c|00001})" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    // should NOT trigger the insertion of a new c|00001 key
    server.insert("b|00002|0000000005", "b3", true);

    std::cerr << "After inserting new b|00002 key" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    server.validate("f|00003|0000000001", "f|00003|0000000005");

    // SHOULD have a validrange for f|00003 (that expires) but NO copy of e|00004
    std::cerr << "After validating [f|00003|0000000001, f|00003|0000000005)" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    server.insert("e|00004|0000000003", "e4", true);

    // should NOT trigger the insertion of a new f|00003 key
    std::cerr << "After inserting new e|00004 key" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    std::cerr << std::endl;

    // should NOT re-validate the lower part of the range.
    server.validate("f|00003|0000000001", "f|00003|0000000010");

    // SHOULD have 2 validranges for f|00003 (that expire) but NO copies of e|00004
    std::cerr << "After validating [f|00003|0000000001, f|00003|0000000010)" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl;

    sleep(1);

    // SHOULD re-validate the whole range
    server.validate("f|00003|0000000001", "f|00003|0000000010");

    // SHOULD have a validrange for f|00003 (that expires) but NO copy of e|00004
    // MIGHT have 2 expired validranges for f|00003 we implement cleanup of expired ranges
    std::cerr << "After sleep and validating [f|00003|0000000001, f|00003|0000000010)" << std::endl;
    for (auto it = server.begin(); it != server.end(); ++it)
        std::cerr << "  " << it->key() << ": " << it->value_ << "\n";
    server.print(std::cerr);
    std::cerr << std::endl << std::endl;
}

void srs() {
    pq::Server server;
    pq::ServerRangeSet srs(&server, "a001", "a}",
                       pq::ServerRange::joinsink | pq::ServerRange::validjoin);

    pq::Join j;
    pq::ServerRange *r0 = pq::ServerRange::make("a", "a}", pq::ServerRange::joinsink, &j);
    pq::ServerRange *r1 = pq::ServerRange::make("a003", "a005", pq::ServerRange::validjoin, &j);
    pq::ServerRange *r2 = pq::ServerRange::make("a007", "a010", pq::ServerRange::validjoin, &j);

    srs.push_back(r0);
    srs.push_back(r1);
    srs.push_back(r2);

    CHECK_EQ(srs.total_size(), 3);
}

void test_join1() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("c|<a_id:5>|<b_id:5>|<index:5> "
                               "a|<a_id>|<b_id> "
                               "b|<index>|<b_id>"));

    j1.ref();
    server.add_join("c|", "c}", &j1);

    String begin("c|00000|");
    String end("c|10000}");
    server.insert("a|00000|B0000", "a: index-only", true);
    server.validate(begin, end);
    CHECK_EQ(size_t(0), server.count(begin, end));

    server.insert("b|I0000|B0000", "b: real value", true);
    CHECK_EQ(size_t(1), server.count(begin, end));
}

} // namespace

void unit_tests(const std::set<String> &testcases) {
#define ADD_TEST(test) tests_.push_back(std::pair<String, test_func>(#test, test))
    ADD_TEST(simple);
    ADD_TEST(recursive);
    ADD_TEST(count);
    ADD_TEST(annotation);
    ADD_TEST(srs);
    ADD_TEST(test_join1);
    for (auto& t : tests_)
        if (testcases.empty() || testcases.find(t.first) != testcases.end()) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
        }
    std::cerr << "PASS" << std::endl;
}
