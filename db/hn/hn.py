# sudo apt-get install python-psycopg2
# su postgres
# psql
# postgres=# CREATE DATABASE hn
# postgres=# GRANT ALL PRIVILEGES ON hn TO neha

import sys
import os
from random import randint
from subprocess import Popen, PIPE
import psycopg2
import datetime
from optparse import OptionParser

# Schema for files
ARTICLE = "{0[aid]}\t{0[author]}\t{0[text]}\n"
COMMENT = "{0[cid]}\t{0[aid]}\t{0[commentor]}\t{0[text]}\n"
VOTE = "{0[aid]}\t{0[voter]}\n"

# Benchmark read query
QUERY = """SELECT articles.aid,articles.author,articles.link,""" \
"""comments.cid,comments.comment,""" \
"""karma_mv.karma,count(votes.aid) as vote_count """ \
"""FROM articles,comments,votes,karma_mv """ \
"""WHERE articles.aid = %d """ \
"""AND comments.aid = articles.aid """ \
"""AND votes.aid = articles.aid """ \
"""AND karma_mv.author=comments.commenter """ \
"""GROUP BY articles.aid,comments.cid,karma_mv.karma\n"""


parser = OptionParser()
parser.add_option("-p", "--port", action="store", type="int", dest="port", default=5477)
parser.add_option("-X", "--large", action="store_true", dest="largedb", default=False)
parser.add_option("-m", "--materialize", action="store_true", dest="materialize", default=False)
(options, args) = parser.parse_args()


class HNPopulator:
    def __init__(self, db, user, port, narticles, nusers, nvotes, ncomments):
        self.narticles = narticles
        self.nusers = nusers
        self.nvotes = nvotes
        self.ncomments = ncomments
        self.nid = 0
        self.db = db
        self.user = user
        self.port = port

    def generate(self, fn):
        articles = []
        votes = []
        comments = []        
        aid = 0
        for aid in range(0, self.narticles):
            d = {}
            d['author'] = randint(0, self.nusers)
            d['aid'] = aid
            d['text'] = "lalalala"
            articles.append(d)

            v =  {'voter':d['author'], 'aid':aid}
            votes.append(v)

        seen = { }
        for x in range(0, self.narticles):
            for i in range(0, randint(0, self.nvotes)):
                aid = randint(0, self.narticles-1)
                voter = randint(0, self.nusers-1)
                if seen.has_key(voter):
                    k = seen[voter]
                    try:
                        x = k[aid]
                        continue 
                    except:
                        pass
                else:
                    seen[voter] = {}
                seen[voter][aid] = 1
                votes.append( {'voter':voter, 'aid':aid} )

            for i in range(0, randint(0, self.ncomments)):
                aid = randint(0, self.narticles-1)
                comments.append( {'commentor':randint(0, self.nusers-1), 'aid':aid, 'cid':self.nid, 'text':'csljdf'} )
                self.nid += 1

        self.write_file(fn+".articles", ARTICLE, articles)
        self.write_file(fn+".votes", VOTE, votes)
        self.write_file(fn+".comments", COMMENT, comments)

    def write_file(self, fn, format_string, stuff):
        with open(fn, 'w') as f:
            for obj in stuff:
                f.write(format_string.format(obj))
            
    def load(self, fn):
        self.clear()
        self.psql("schema.sql")
        conn = psycopg2.connect("user=%s dbname=%s port=%d" % (self.user, self.db, self.port))
        curs = conn.cursor()
        for table in ['articles', 'votes', 'comments']:
            curs.copy_from(open(fn+"."+table, 'r'), table)
        conn.commit()
        conn.close()
        self.psql("schema_index.sql")
        if options.materialize:
            self.psql("views.sql")

    def clear(self):
        tmpdb = self.db
        self.db = "blah"
        self.psql("drop.sql")
        self.db = tmpdb

    def psql(self, fn):
        stdout, stderr = Popen(['psql -p %d -d %s < %s' % (self.port, self.db, fn)], shell=True, stdout=PIPE, stderr=PIPE).communicate()
        if 'ERROR' in stderr:
            print stdout, stderr

    def make_benchmark(self, bfn, n):
        with open(bfn, 'w') as f:
            for i in range(0, n):
                aid = randint(1, self.narticles)
                f.write(QUERY % aid)

    def bench(self):
        stdout, stderr = Popen(['/usr/lib/postgresql/9.1/bin/pgbench %s -p %d -n -f bench.sql -T 20 -c 5' % (self.db, self.port)], shell=True, stdout=PIPE, stderr=PIPE).communicate()
        print stdout, stderr

    def dump(self, fn):
        stdout, stderr = Popen(["/usr/lib/postgresql/9.1/bin/pg_dump -p %d %s > %s" % (self.port, self.db, fn)], shell=True,  stdout=PIPE, stderr=PIPE).communicate()
        print stdout, stderr


if __name__ == "__main__":
    if options.largedb:
        hn = HNPopulator("hn", os.environ['USER'], options.port, 100000, 10000, 50, 20)
        print "Generating 100000 articles, 10000 users, 50 votes, 20 comments"
    else:
        hn = HNPopulator("hn", os.environ['USER'], options.port, 100, 10, 5, 2)
        print "Generating 100 articles, 10 users, 5 votes, 2 comments"
    suffix = ".%s%s" % ("large" if options.largedb else "small", ".mv" if options.materialize else "")
    dataset = "hn.data" + suffix
    hn.generate(dataset)
    print "Loading", dataset
    hn.load(dataset)
    hn.dump("pg.dump"+suffix)
    
