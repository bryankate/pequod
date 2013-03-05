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

# Schema for files
ARTICLE = "{0[aid]}\t{0[author]}\t{0[text]}\n"
COMMENT = "{0[cid]}\t{0[aid]}\t{0[commentor]}\t{0[text]}\n"
VOTE = "{0[aid]}\t{0[voter]}\n"


class HNPopulator:
    def __init__(self):
        self.narticles = 10
        self.nusers = 10
        self.nvotes = 2
        self.ncomments = 2
        self.nid = 0

    def generate(self, fn):
        articles = []
        votes = []
        comments = []
        aid = 0
        for aid in range(0, self.narticles):
            d = {}
            d['author'] = randint(1, self.nusers)
            d['aid'] = aid
            d['text'] = "lalalala"
            articles.append(d)
            votes.append( {'voter':d['author'], 'aid':aid} )

            for i in range(0, randint(1, self.nvotes)):
                votes.append( {'voter':randint(1, self.nusers), 'aid':aid} )

            for i in range(0, randint(1, self.ncomments)):
                comments.append( {'commentor':randint(1, self.nusers), 'aid':aid, 'cid':self.nid, 'text':'csljdf'} )
                self.nid += 1

        self.write_file(fn+".articles", ARTICLE, articles)
        self.write_file(fn+".votes", VOTE, votes)
        self.write_file(fn+".comments", COMMENT, comments)

    def write_file(self, fn, format_string, stuff):
        with open(fn, 'w') as f:
            for obj in stuff:
                f.write(format_string.format(obj))
            
    def load(self, fn, db, user):
        stdout, stderr = Popen(['psql -d %s < schema.sql' % db], shell=True, stdout=PIPE, stderr=PIPE).communicate()
        if 'ERROR' in stderr:
            print stdout, stderr
            exit(1)
        conn = psycopg2.connect("user=%s dbname=%s" % (user, db))
        curs = conn.cursor()
        for table in ['articles', 'votes', 'comments']:
            curs.copy_from(open(fn+"."+table, 'r'), table)
        conn.commit()
        conn.close()


if __name__ == "__main__":
    hn = HNPopulator()
    if len(sys.argv) == 3:
        hn.load(sys.argv[1], sys.argv[2], os.environ['USER'])
    elif len(sys.argv) == 1:
        hn.generate("hn.data")
        hn.load("hn.data", "hn", os.environ['USER'])
    else:
        print "Usage: %s <file> <db>" %  sys.argv[0]

    
