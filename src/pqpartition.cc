#include "partitioner.hh"

namespace pq {

namespace {
class DefaultPartitioner : public Partitioner {
  public:
    DefaultPartitioner(uint32_t nservers);
};

DefaultPartitioner::DefaultPartitioner(uint32_t nservers)
    : Partitioner(0, 0) {
    ps_.add(partition1("", partition1::text, 1, 0, nservers));
}


class UnitTestPartitioner : public Partitioner {
  public:
    UnitTestPartitioner(uint32_t nservers, int32_t default_owner);
  private:
    static const char prefixes_[];
};
const char UnitTestPartitioner::prefixes_[] =
    " !\"#$%&'()*+,-./0123456789:;<=>?@"
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`"
    "abcdefghijklmnopqrstuvwxyz{|}";

UnitTestPartitioner::UnitTestPartitioner(uint32_t nservers, int default_owner)
    : Partitioner(default_owner, 0) {
    // make partitions, except leave '~' as default_owner.
    // Keys with the same first character are owned by the same server.
    for (size_t i = 0; prefixes_[i]; ++i)
        ps_.add(partition1(Str(&prefixes_[i], 1), partition1::text, 0, i % nservers, 1));
}


class TwitterPartitioner : public Partitioner {
  public:
    TwitterPartitioner(uint32_t nservers, uint32_t nbacking,
                       uint32_t default_owner, bool newtwitter, bool binary);
};

TwitterPartitioner::TwitterPartitioner(uint32_t nservers, uint32_t nbacking,
                                       uint32_t default_owner,
                                       bool newtwitter, bool binary)
    : Partitioner(default_owner, nbacking) {

    ps_.add(partition1("c|", partition1::text, 0, 0, 1));

    if (newtwitter) {
        if (binary) {
            ps_.add(partition1("cp|", partition1::binary, 24, 0, (nbacking) ? nbacking : nservers));
            ps_.add(partition1("p|", partition1::binary, 24, 0, (nbacking) ? nbacking : nservers));
            ps_.add(partition1("s|", partition1::binary, 24, 0, (nbacking) ? nbacking : nservers));
            ps_.add(partition1("t|", partition1::binary, 24, nbacking, nservers - nbacking));
        }
        else {
            ps_.add(partition1("cp|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
            ps_.add(partition1("p|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
            ps_.add(partition1("s|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
            ps_.add(partition1("t|", partition1::decimal, 5, nbacking, nservers - nbacking));
        }
    }
    else {
        ps_.add(partition1("f|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("p|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("s|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
        ps_.add(partition1("t|", partition1::decimal, 5, nbacking, nservers - nbacking));
    }
}


class HackerNewsPartitioner : public Partitioner {
  public:
    HackerNewsPartitioner(uint32_t nservers, uint32_t nbacking, uint32_t default_owner);
};

HackerNewsPartitioner::HackerNewsPartitioner(uint32_t nservers, uint32_t nbacking,
                                             uint32_t default_owner)
    : Partitioner(default_owner, nbacking) {

    ps_.add(partition1("a|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
    ps_.add(partition1("c|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
    ps_.add(partition1("v|", partition1::decimal, 5, 0, (nbacking) ? nbacking : nservers));
    ps_.add(partition1("k|", partition1::decimal, 5, nbacking, nservers - nbacking));
    ps_.add(partition1("ma|", partition1::decimal, 5, nbacking, nservers - nbacking));
}

}


Partitioner *Partitioner::make(const String &name, uint32_t nservers,
                               uint32_t default_owner) {
    return Partitioner::make(name, 0, nservers, default_owner);
}

Partitioner *Partitioner::make(const String &name, uint32_t nbacking,
                               uint32_t nservers, uint32_t default_owner) {
    if (name == "default" || nservers == 1)
        return new DefaultPartitioner(nservers);
    else if (name == "unit")
        return new UnitTestPartitioner(nservers, default_owner);
    else if (name == "twitter")
        return new TwitterPartitioner(nservers, nbacking, default_owner, false, false);
    else if (name == "twitternew")
        return new TwitterPartitioner(nservers, nbacking, default_owner, true, true);
    else if (name == "twitternew-text")
        return new TwitterPartitioner(nservers, nbacking, default_owner, true, false);
    else if (name == "hackernews")
        return new HackerNewsPartitioner(nservers, nbacking, default_owner);
    else
        assert(0 && "Unknown partition name");
    return 0;
}

}
