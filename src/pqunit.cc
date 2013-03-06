#define DO_PERF 0
#include <boost/random/random_number_generator.hpp>
#include <sys/resource.h>
#include <unistd.h>
#include <set>
#if DO_PERF
#include <sys/prctl.h>
#include <sys/wait.h>
#endif
#include "pqserver.hh"
#include "pqjoin.hh"
#include "json.hh"
#include "time.hh"
#include "check.hh"

namespace  {

typedef void (*test_func)();
std::vector<std::pair<String, test_func> > tests_;
std::vector<std::pair<String, test_func> > exptests_;

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
        server.insert(it->first, it->second);

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

    server.insert("p|10000|0000000022", "This should appear in t|00001");
    server.insert("p|00002|0000000023", "As should this");

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
        server.insert(it->first, it->second);
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

    CHECK_EQ(server.validate_count("e|", "e}"), size_t(4), "Count of recursive expansion failed.");
    CHECK_EQ(server.validate_count("b|", "b}"), size_t(5));
    CHECK_EQ(server.validate_count("b|00002|", "b|00002}"), size_t(4));
    CHECK_EQ(server.validate_count("b|00002|0000000002", "b|00002|0000000015"), size_t(2), "\nWrong subrange count.");
    CHECK_EQ(server.validate_count("c|", "c}"), size_t(4));
    CHECK_EQ(server.validate_count("j|", "j}"), size_t(0));
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
        server.insert(it->first, it->second);

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

    server.insert("b|00002|0000001000", "This should appear in c|00001 and e|00003");
    server.insert("b|00002|0000002000", "As should this");

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
        server.insert(it->first, it->second);

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
    server.insert("b|00002|0000000005", "b3");

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

    server.insert("e|00004|0000000003", "e4");

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
    pq::ServerRangeSet srs("a001", "a}",
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

void test_count() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> "
                               "a|<uid>|<aid:5> "
                               "v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_count_match);
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
    CHECK_EQ(k0->value_, "1");

    server.insert("v|00001|00000", "vote 0");
    server.insert("v|00003|00000", "vote 0");
    CHECK_EQ(server.count(begin, end), size_t(2));
    auto k1 = server.find("k|00001");
    mandatory_assert(k1);
    CHECK_EQ(k0->value_, "2");
    CHECK_EQ(k1->value_, "1");
}

// One Server::validate produces multiple grouped keys
void test_count_validate1() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> "
                               "a|<uid>|<aid:5> "
                               "v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_count_match);
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
    CHECK_EQ(k0->value_, "2");
    auto k1 = server.find("k|00001");
    mandatory_assert(k1);
    CHECK_EQ(k1->value_, "1");
}

void test_karma() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> "
                               "a|<uid>|<aid:5> "
                               "v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_count_match);
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
        CHECK_EQ(k0->value_, String(nvotes_per_aid));
    }
    Json stats = Json().set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
}

void test_karma_online() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> "
                               "a|<uid>|<aid:5> "
                               "v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_count_match);
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
        CHECK_EQ(k0->value_, String(1));
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
        CHECK_EQ(k0->value_, String(nvotes_per_aid));
    }
    Json stats = Json().set("time", to_real(ru[1].ru_utime - ru[0].ru_utime));
    std::cout << stats.unparse(Json::indent_depth(4)) << "\n";
}

void test_min() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> "
                               "a|<uid>|<aid:5> "
                               "v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_min_last);
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
    CHECK_EQ(k0->value_, "v8");

    server.insert("v|00000|00005", "v5");
    CHECK_EQ(k0->value_, "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.insert("v|00000|00006", "v6");
    CHECK_EQ(k0->value_, "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));
}

void test_max() {
    pq::Server server;
    pq::Join j1;
    CHECK_TRUE(j1.assign_parse("k|<uid:5> "
                               "a|<uid>|<aid:5> "
                               "v|<aid>|<voter:5>"));
    CHECK_EQ(j1.nsource(), 2);

    j1.set_jvt(pq::jvt_max_last);
    j1.ref();
    server.add_join("k|", "k}", &j1);

    String begin("k|");
    String end("k}");
    server.insert("a|00000|00000", "article 0");
    server.insert("a|00000|00001", "article 1");
    server.validate(begin, end);
    CHECK_EQ(server.count(begin, end), size_t(0));

    server.insert("v|00000|00001", "v1");
    server.insert("v|00000|00002", "v2");
    CHECK_EQ(server.count(begin, end), size_t(1));
    auto k0 = server.find("k|00000");
    mandatory_assert(k0);
    CHECK_EQ(k0->value_, "v2");

    server.insert("v|00000|00003", "v5");
    CHECK_EQ(k0->value_, "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.insert("v|00000|00004", "v4");
    CHECK_EQ(k0->value_, "v5");
    CHECK_EQ(server.count(begin, end), size_t(1));

    server.insert("v|00001|00005", "v6");
    CHECK_EQ(k0->value_, "v6");
    CHECK_EQ(server.count(begin, end), size_t(1));
}

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

} // namespace

void unit_tests(const std::set<String> &testcases) {
#define ADD_TEST(test) tests_.push_back(std::pair<String, test_func>(#test, test))
#define ADD_EXP_TEST(test) exptests_.push_back(std::pair<String, test_func>(#test, test))
    ADD_TEST(simple);
    ADD_TEST(recursive);
    ADD_TEST(count);
    ADD_TEST(annotation);
    ADD_TEST(srs);
    ADD_TEST(test_join1);
    ADD_TEST(test_count);
    ADD_TEST(test_count_validate1);
    ADD_TEST(test_min);
    ADD_TEST(test_max);
    ADD_EXP_TEST(test_karma);
    ADD_EXP_TEST(test_swap);
    ADD_EXP_TEST(test_karma_online);
    for (auto& t : tests_)
        if (testcases.empty() || testcases.find(t.first) != testcases.end()) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
        }
    for (auto& t : exptests_)
        if (testcases.find(t.first) != testcases.end()) {
            std::cerr << "Testing " << t.first << std::endl;
            t.second();
        }
    std::cerr << "PASS" << std::endl;
}
