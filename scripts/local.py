#!/usr/bin/env python

import sys
import os
from os import system
import subprocess
from subprocess import Popen
import shutil
import time
from time import sleep
from optparse import OptionParser
import json, fnmatch, re
import socket
import lib.aggregate
from lib.aggregate import aggregate_dir
import lib.gnuplotter
from lib.gnuplotter import make_gnuplot


parser = OptionParser()
parser.add_option("-e", "--expfile", action="store", type="string", dest="expfile", 
                  default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "exp", "testexperiments.py"))
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-p", "--startport", action="store", type="int", dest="startport", default=9000)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)
parser.add_option("-A", "--startcpu", action="store", type="int", dest="startcpu", default=0)
parser.add_option("-k", "--skipcpu", action="store", type="int", dest="skipcpu", default=1)
parser.add_option("-P", "--perfserver", action="store", type="int", dest="perfserver", default=-1)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
parser.add_option("-g", "--clientgroups", action="store", type="int", dest="ngroups", default=1)
parser.add_option("-D", "--dbstartport", action="store", type="int", dest="dbstartport", default=10000)
parser.add_option("-d", "--dumpdb", action="store_true", dest="dumpdb", default=False)
parser.add_option("-l", "--loaddb", action="store_true", dest="loaddb", default=False)
parser.add_option("-r", "--ramfs", action="store", type="string", dest="ramfs", default="/mnt/tmp")
(options, args) = parser.parse_args()

expfile = options.expfile
nbacking = options.nbacking
ncaching = options.ncaching
startport = options.startport
affinity = options.affinity
startcpu = options.startcpu
skipcpu = options.skipcpu
perfserver = options.perfserver
ngroups = options.ngroups
dbstartport = options.dbstartport
dumpdb = options.dumpdb
loaddb = options.loaddb
ramfs = options.ramfs

nprocesses = nbacking + ncaching
ndbs = nbacking if nbacking > 0 else ncaching
dbhost = "127.0.0.1"
hosts = []
pin = ""

topdir = None
uniquedir = None
hostpath = None
begintime = time.time()
localhost = socket.gethostname()

def prepare_experiment(xname, ename):
    global topdir, uniquedir, hostpath
    if topdir is None:
        topdir = "results"

    if uniquedir is None:
        uniquedir = topdir + "/exp_" + time.strftime("%Y_%m_%d-%H_%M_%S")
        os.makedirs(uniquedir)

        if os.path.lexists("last"):
            os.unlink("last")
        os.symlink(uniquedir, "last")

        hostpath = os.path.join(uniquedir, "hosts.txt")
        hfile = open(hostpath, "w")
        for h in range(nprocesses):
            hfile.write(localhost + "\t" + str(startport + h) + "\n");
        hfile.close()

    resdir = os.path.join(uniquedir, xname, ename if ename else "")
    os.makedirs(resdir)
    return (os.path.join(uniquedir, xname), resdir)

def check_database_env(expdef):
    global dbenvpath
    
    # possibly redirect to ramfs
    if expdef.get('def_db_in_memory'):
        cmd = "mount | grep '" + ramfs + "' | grep -o '[0-9]\\+[kmg]'"
        (a, _) = Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
        if a:
            if len(a) > 8:
                print "[[37;1minfo[0m] Memory FS is %s. (~%sGb)" % (a[:-1],a[:-8])
            else:
                print "[[33;1mwarn[0m] Memory FS is small, %s is less than 1Gb memory." % (a[:-1])
            (a,_) = Popen("df " + ramfs + " | grep -o '[0-9]\+%'", stdout=subprocess.PIPE, shell=True).communicate()
            if a[0] is not "0":
                print "[[33;1mwarn[0m] " + ramfs + " is %s full." % (a[:-1])
        else:
            print "[[31mFAIL[0m] '" + ramfs + "' not found or missing size."
            print "[[31mFAIL[0m] search command:\"%s\"." % (cmd)
            exit()
        dbenvpath = os.path.join(ramfs, dbenvpath)

def start_postgres(expdef, id):
    global dbpath
    
    fartfile = os.path.join(resdir, "fart_db_" + str(id) + ".txt")
    dbpath = os.path.join(dbenvpath, "postgres_" + str(id))
    os.makedirs(dbpath)

    cmd = "initdb " + dbpath + " -E utf8 -A trust" + \
          " >> " + fartfile + " 2>> " + fartfile
    print cmd
    Popen(cmd, shell=True).wait()
          
    cmd = "postgres -h " + dbhost + " -p " + str(dbstartport + id) + \
          " -D " + dbpath + " -c synchronous_commit=off -c fsync=off " + \
          " -c full_page_writes=off  -c bgwriter_lru_maxpages=0 " + \
          " >> " + fartfile + " 2>> " + fartfile
    print cmd
    proc = Popen(cmd, shell=True)
    sleep(2)
    
    cmd = "createdb -h " + dbhost + " -p " + str(dbstartport + id) + " pequod" + \
          " >> " + fartfile + " 2>> " + fartfile
    print cmd
    Popen(cmd, shell=True).wait()

    if 'def_db_sql_script' in expdef:
        cmd = "psql -p %d pequod < %s >> %s 2>> %s" % \
              (dbstartport + id, expdef['def_db_sql_script'], fartfile, fartfile)
        
        print cmd
        Popen(cmd, shell=True).wait()
        
    return proc

# load experiment definitions as global 'exps'
exph = open(expfile, "r")
exec(exph, globals())
exph.close()

for x in exps:
    expdir = None
    
    for e in x['defs']:
        expname = e['name'] if 'name' in e else None
        
        # skip this experiment if it doesn't match an argument
        if args and not [argmatcher for argmatcher in args if fnmatch.fnmatch(x['name'], argmatcher)]:
            continue
        elif not args and e.get("disabled"):
            continue

        print "Running experiment" + ((" '" + expname + "'") if expname else "") + \
              " in test '" + x['name'] + "'."
        
        if 'def_build' in e:
            fartfile = os.path.join(resdir, "fart_build.txt")
            fd = open(fartfile, "w")

            print e['def_build']
            Popen(e['def_build'], stdout=fd, stderr=fd, shell=True).wait()
            fd.close()
             
        (expdir, resdir) = prepare_experiment(x["name"], expname)
        usedb = True if 'def_db_type' in e else False
        dbcompare = e.get('def_db_compare')
        dbmonitor = e.get('def_db_writearound')
        serverprocs = [] 
        dbprocs = []
        
        if usedb:
            dbenvpath = os.path.join(resdir, "store")
            check_database_env(e)
            os.makedirs(dbenvpath)
        
        if dbcompare:
            # if we are comparing to a db, don't start any pequod servers.
            # the number of caching servers (-c) will be used as the number 
            # of simultaneous connections to the database.
            if ncaching < 1 or ngroups > 1:
                print "ERROR: -c must be > 0 for DB comparison experiments"
                exit(-1)
                
            dbprocs.append(start_postgres(e, 0))
        else:
            if usedb:
                dbhostpath = os.path.join(resdir, "dbhosts.txt")
                dbfile = open(dbhostpath, "w")
                
            for s in range(nprocesses):
                if s < nbacking:
                    servercmd = e['backendcmd']
                else:
                    servercmd = e['cachecmd']
                
                if usedb and s < ndbs:
                    if e['def_db_type'] == 'berkeleydb':
                        servercmd = servercmd + \
                            " --berkeleydb --dbname=pequod_" + str(s) + \
                            " --dbenvpath=" + dbenvpath
                    elif e['def_db_type'] == 'postgres':
                        servercmd = servercmd + \
                            " --postgres --dbname=pequod" + \
                            " --dbhost=" + dbhost + " --dbport=" + str(dbstartport + s)
                        if dbmonitor:
                            servercmd = servercmd + " --monitordb"
    
                        dbfile.write(dbhost + "\t" + str(dbstartport + s) + "\n");
                        dbprocs.append(start_postgres(e, s))
    
                part = options.part if options.part else e['def_part']
                serverargs = " -H=" + hostpath + " -B=" + str(nbacking) + " -P=" + part
                outfile = os.path.join(resdir, "output_srv_")
                fartfile = os.path.join(resdir, "fart_srv_")
      
                if affinity:
                    pin = "numactl -C " + str(startcpu + (s * skipcpu)) + " "
                    
                if s == perfserver:
                    perf = "perf record -g -o " + os.path.join(resdir, "perf-") + str(s) + ".dat "
                else:
                    perf = ""
    
                full_cmd = pin + perf + servercmd + serverargs + \
                    " -kl=" + str(startport + s) + \
                    " > " + outfile + str(s) + ".txt" + \
                    " 2> " + fartfile + str(s) + ".txt"
    
                print full_cmd
                serverprocs.append(Popen(full_cmd, shell=True))
        
            if usedb:
                dbfile.close()
                
        sleep(3)


        if 'initcmd' in e:
            print "Initializing cache servers."
            initcmd = e['initcmd']
            fartfile = os.path.join(resdir, "fart_init.txt")
            
            if dbcompare:
                initcmd = initcmd + " -c=%d --dbpool-max=%d" % (dbstartport, ncaching)
            else:
                initcmd = initcmd + " -H=" + hostpath + " -B=" + str(nbacking)
            
            if affinity:
                pin = "numactl -C " + str(startcpu + (nprocesses * skipcpu)) + " "
            
            full_cmd = pin + initcmd + " 2> " + fartfile

            print full_cmd
            Popen(full_cmd, shell=True).wait()

        if loaddb and dbmonitor and e['def_db_type'] == 'postgres':
            print "Populating backend from database archive."
            dbarchive = os.path.join("dumps", x["name"], e["name"], "nshard_" + str(ndbs))
            procs = []
            for s in range(ndbs):
                archfile = os.path.join(dbarchive, "pequod_" + str(s))
                cmd = "pg_restore -p " + str(dbport + s) + " -d pequod -a " + archfile
                print cmd
                procs.append(Popen(cmd, shell=True))
                
            for p in procs:
                p.wait()
        elif 'populatecmd' in e:
            print "Populating backend."
            popcmd = e['populatecmd']
            fartfile = os.path.join(resdir, "fart_pop.txt")
            
            if dbcompare:
                popcmd = popcmd + " -c=%d --dbpool-max=%d" % (dbstartport, ncaching)
            else:
                popcmd = popcmd + " -H=" + hostpath + " -B=" + str(nbacking)
            
            if dbmonitor:
                popcmd = popcmd + " --writearound --dbhostfile=" + dbhostpath
            
            if affinity:
                pin = "numactl -C " + str(startcpu + (nprocesses * skipcpu)) + " "
            
            full_cmd = pin + popcmd + " 2> " + fartfile

            print full_cmd
            Popen(full_cmd, shell=True).wait()

        if dumpdb and dbmonitor and e['def_db_type'] == 'postgres':
            print "Dumping backend database after population."
            procs = []
            dbarchive = os.path.join("dumps", x["name"], e["name"], "nshard_" + str(ndbs))
            for s in range(ndbs):
                archfile = os.path.join(dbarchive, "pequod_" + str(s))
                system("rm -rf " + archfile + "; mkdir -p " + dbarchive)
                cmd = "pg_dump -p " + str(dbport + s) + " -f " + archfile + " -a -F d pequod" 
                print cmd
                procs.append(Popen(cmd, shell=True))
                
            for p in procs:
                p.wait()


        print "Starting app clients."
        clientprocs = []
        clientcmd = e['clientcmd']
        
        for c in range(ngroups):
            outfile = os.path.join(resdir, "output_app_")
            fartfile = os.path.join(resdir, "fart_app_")
            
            if dbcompare:
                clientcmd = clientcmd + " -c=%d --dbpool-max=%d" % (dbstartport, ncaching)
            else:
                clientcmd = clientcmd + " -H=" + hostpath + " -B=" + str(nbacking)
            
            if dbmonitor:
                clientcmd = clientcmd + " --writearound --dbhostfile=" + dbhostpath
                
            if affinity:
                pin = "numactl -C " + str(startcpu + ((nprocesses + c) * skipcpu)) + " "

            full_cmd = pin + clientcmd + \
                " --ngroups=" + str(ngroups) + " --groupid=" + str(c) + \
                " > " + outfile + str(c) + ".json" + \
                " 2> " + fartfile + str(c) + ".txt"

            print full_cmd
            clientprocs.append(Popen(full_cmd, shell=True));
            
        # wait for clients to finish
        for p in clientprocs:
            p.wait()
    
        for p in serverprocs + dbprocs:
            p.kill()
            p.wait()
    
        if ngroups > 1:
            aggregate_dir(resdir)
    
        if usedb and e.get('def_db_in_memory'):
           shutil.rmtree(dbenvpath)

        print "Done experiment. Results are stored at", resdir
    
    if expdir and 'plot' in x:
        make_gnuplot(x['name'], expdir, x['plot'])
        
